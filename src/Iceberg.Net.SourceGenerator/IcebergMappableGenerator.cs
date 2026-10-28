using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Iceberg.Net.SourceGenerator;

[Generator]
public sealed class IcebergMappableGenerator : IIncrementalGenerator
{
    private const string AttributeFullName = "Iceberg.Net.Linq.IcebergMappableAttribute";
    private const string AttributeShortName = "IcebergMappable";

    private static readonly SymbolDisplayFormat DisplayFormat = SymbolDisplayFormat.FullyQualifiedFormat
        .AddMiscellaneousOptions(SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var candidates = context.SyntaxProvider
            .CreateSyntaxProvider(
                predicate: static (node, _) => IsCandidate(node),
                transform: static (ctx, ct) => GetModel(ctx, ct))
            .Where(static m => m is not null);

        context.RegisterSourceOutput(candidates, static (spc, model) =>
        {
            if (model is not null)
            {
                Emit(spc, model);
            }
        });
    }

    private static bool IsCandidate(SyntaxNode node) =>
        node is TypeDeclarationSyntax typeDecl && typeDecl.AttributeLists.Count > 0;

    private static MappableTypeModel? GetModel(GeneratorSyntaxContext ctx, CancellationToken ct)
    {
        if (ctx.Node is not TypeDeclarationSyntax typeDecl) return null;

        var symbol = ctx.SemanticModel.GetDeclaredSymbol(typeDecl, ct) as INamedTypeSymbol;
        if (symbol is null) return null;

        var hasAttr = symbol.GetAttributes().Any(a =>
            a.AttributeClass?.ToDisplayString() == AttributeFullName ||
            a.AttributeClass?.Name == AttributeShortName ||
            a.AttributeClass?.Name == AttributeShortName + "Attribute");

        if (!hasAttr) return null;

        var ns = symbol.ContainingNamespace.IsGlobalNamespace ? null : symbol.ContainingNamespace.ToDisplayString();
        var typeName = symbol.Name;
        var typeKind = typeDecl switch
        {
            RecordDeclarationSyntax r when r.ClassOrStructKeyword.IsKind(SyntaxKind.StructKeyword) => "record struct",
            RecordDeclarationSyntax => "record",
            StructDeclarationSyntax => "struct",
            _ => "class"
        };

        var properties = new List<MappablePropertyModel>();
        int defaultFieldId = 1;

        foreach (var member in symbol.GetMembers().OfType<IPropertySymbol>())
        {
            if (member.IsStatic || member.IsIndexer || member.DeclaredAccessibility != Accessibility.Public)
                continue;

            if (member.GetAttributes().Any(a => a.AttributeClass?.Name is "IcebergIgnore" or "IcebergIgnoreAttribute"))
                continue;

            int fieldId = defaultFieldId++;
            string? customName = null;
            bool isKey = false;

            var fieldAttr = member.GetAttributes().FirstOrDefault(a => a.AttributeClass?.Name is "IcebergField" or "IcebergFieldAttribute");
            if (fieldAttr is not null)
            {
                if (fieldAttr.ConstructorArguments.Length > 0 && fieldAttr.ConstructorArguments[0].Value is int explicitId && explicitId > 0)
                {
                    fieldId = explicitId;
                }
                foreach (var named in fieldAttr.NamedArguments)
                {
                    if (named.Key == "Name" && named.Value.Value is string s) customName = s;
                    if (named.Key == "Id" && named.Value.Value is int id && id > 0) fieldId = id;
                }
            }

            if (member.GetAttributes().Any(a => a.AttributeClass?.Name is "IcebergKey" or "IcebergKeyAttribute"))
            {
                isKey = true;
            }

            var typeSymbol = member.Type;
            var isNullable = typeSymbol.NullableAnnotation == NullableAnnotation.Annotated ||
                             (typeSymbol is INamedTypeSymbol nts && nts.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

            var typeDisplay = typeSymbol.ToDisplayString(DisplayFormat);
            var isValueType = typeSymbol.IsValueType;
            var conversion = GetConversionKind(typeSymbol);

            properties.Add(new MappablePropertyModel(
                member.Name,
                customName ?? member.Name,
                fieldId,
                typeDisplay,
                isNullable,
                isValueType,
                isKey,
                GetIcebergTypeCreationExpression(typeSymbol),
                conversion
            ));
        }

        // Determine if primary constructor / constructor with matching parameters exists
        var ctors = symbol.Constructors.Where(c => !c.IsStatic && c.DeclaredAccessibility == Accessibility.Public).ToList();
        var bestCtor = ctors.OrderByDescending(c => c.Parameters.Length).FirstOrDefault();
        bool useConstructor = false;
        var ctorParams = new List<string>();

        if (bestCtor is not null && bestCtor.Parameters.Length > 0)
        {
            var matched = bestCtor.Parameters.All(p =>
                properties.Any(pr => string.Equals(pr.PropertyName, p.Name, StringComparison.OrdinalIgnoreCase)));
            if (matched)
            {
                useConstructor = true;
                foreach (var p in bestCtor.Parameters)
                {
                    var pr = properties.First(pr => string.Equals(pr.PropertyName, p.Name, StringComparison.OrdinalIgnoreCase));
                    ctorParams.Add(pr.PropertyName);
                }
            }
        }

        return new MappableTypeModel(ns, typeName, typeKind, properties.ToImmutableArray(), useConstructor, ctorParams.ToImmutableArray());
    }

    private static ConversionKind GetConversionKind(ITypeSymbol type)
    {
        var unwrapped = (type is INamedTypeSymbol nts && nts.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            ? nts.TypeArguments[0]
            : type;

        var name = unwrapped.ToDisplayString();
        return name switch
        {
            "decimal" or "System.Decimal" => ConversionKind.Decimal,
            "System.DateOnly" => ConversionKind.DateOnly,
            "System.TimeOnly" => ConversionKind.TimeOnly,
            "System.DateTime" => ConversionKind.DateTime,
            "System.DateTimeOffset" => ConversionKind.DateTimeOffset,
            _ => ConversionKind.Identity
        };
    }

    private static string GetIcebergTypeCreationExpression(ITypeSymbol type)
    {
        var unwrapped = (type is INamedTypeSymbol nts && nts.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            ? nts.TypeArguments[0]
            : type;

        var full = unwrapped.ToDisplayString();
        return full switch
        {
            "bool" or "System.Boolean" => "global::Iceberg.Net.Types.PrimitiveType.Boolean",
            "int" or "System.Int32" => "global::Iceberg.Net.Types.PrimitiveType.Int",
            "long" or "System.Int64" => "global::Iceberg.Net.Types.PrimitiveType.Long",
            "float" or "System.Single" => "global::Iceberg.Net.Types.PrimitiveType.Float",
            "double" or "System.Double" => "global::Iceberg.Net.Types.PrimitiveType.Double",
            "string" or "System.String" => "global::Iceberg.Net.Types.PrimitiveType.String",
            "decimal" or "System.Decimal" => "new global::Iceberg.Net.Types.DecimalType(38, 18)",
            "System.Guid" => "global::Iceberg.Net.Types.PrimitiveType.Uuid",
            "byte[]" => "global::Iceberg.Net.Types.PrimitiveType.Binary",
            "System.DateOnly" => "global::Iceberg.Net.Types.PrimitiveType.Date",
            "System.TimeOnly" => "global::Iceberg.Net.Types.PrimitiveType.Time",
            "System.DateTime" => "global::Iceberg.Net.Types.PrimitiveType.Timestamp",
            "System.DateTimeOffset" => "global::Iceberg.Net.Types.PrimitiveType.TimestampTz",
            _ => "global::Iceberg.Net.Types.PrimitiveType.String"
        };
    }

    private static void Emit(SourceProductionContext spc, MappableTypeModel model)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();

        if (model.Namespace is not null)
        {
            sb.AppendLine($"namespace {model.Namespace};");
            sb.AppendLine();
        }

        sb.AppendLine($"partial {model.TypeKind} {model.TypeName} : global::Iceberg.Net.Linq.IIcebergMappable<{model.TypeName}>");
        sb.AppendLine("{");

        // Emit IcebergSchema property
        sb.AppendLine($"    /// <summary>Compile-time Iceberg schema derived for <see cref=\"{model.TypeName}\"/>.</summary>");
        sb.AppendLine($"    public static global::Iceberg.Net.Types.Schema IcebergSchema {{ get; }} = new global::Iceberg.Net.Types.Schema(0, new global::Iceberg.Net.Types.NestedField[]");
        sb.AppendLine("    {");
        for (int i = 0; i < model.Properties.Length; i++)
        {
            var p = model.Properties[i];
            var reqOrOpt = p.IsNullable ? "Optional" : "Require";
            var comma = i < model.Properties.Length - 1 ? "," : "";
            sb.AppendLine($"        global::Iceberg.Net.Types.NestedField.{reqOrOpt}({p.FieldId}, \"{p.ColumnName}\", {p.IcebergTypeExpr}){comma}");
        }
        sb.AppendLine("    });");
        sb.AppendLine();

        // Emit CreateRowMapper
        sb.AppendLine($"    /// <summary>Creates a zero-reflection row mapper for <see cref=\"{model.TypeName}\"/>.</summary>");
        sb.AppendLine($"    public static global::Iceberg.Net.Linq.IIcebergRowMapper<{model.TypeName}> CreateRowMapper(global::Iceberg.Net.Linq.IcebergMapperOptions? options = null)");
        sb.AppendLine($"        => new __GeneratedRowMapper(options);");
        sb.AppendLine();

        // Emit __GeneratedRowMapper class
        sb.AppendLine($"    private sealed class __GeneratedRowMapper : global::Iceberg.Net.Linq.IIcebergRowMapper<{model.TypeName}>");
        sb.AppendLine("    {");
        sb.AppendLine("        private readonly global::Iceberg.Net.Linq.IcebergMapperOptions? _options;");
        sb.AppendLine("        public __GeneratedRowMapper(global::Iceberg.Net.Linq.IcebergMapperOptions? options) => _options = options;");
        sb.AppendLine();
        sb.AppendLine($"        public global::Iceberg.Net.Linq.IIcebergRowBinding<{model.TypeName}> Bind(global::Iceberg.Net.Types.Schema schema, global::Iceberg.Net.Types.NameMapping? nameMapping = null)");
        sb.AppendLine("        {");
        sb.AppendLine($"            return new __GeneratedRowBinding(schema);");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine();

        // Emit __GeneratedRowBinding class
        sb.AppendLine($"    private sealed class __GeneratedRowBinding : global::Iceberg.Net.Linq.IIcebergRowBinding<{model.TypeName}>");
        sb.AppendLine("    {");
        sb.AppendLine("        public global::Iceberg.Net.Types.Schema Schema { get; }");
        sb.AppendLine("        public global::System.Collections.Generic.IReadOnlyList<global::Iceberg.Net.Linq.IcebergMemberBinding> Members { get; }");
        foreach (var p in model.Properties)
        {
            sb.AppendLine($"        private readonly int _pos_{p.FieldId};");
            if (p.Conversion == ConversionKind.Decimal)
            {
                sb.AppendLine($"        private readonly int _scale_{p.FieldId};");
            }
        }
        sb.AppendLine();
        sb.AppendLine("        public __GeneratedRowBinding(global::Iceberg.Net.Types.Schema schema)");
        sb.AppendLine("        {");
        sb.AppendLine("            Schema = schema;");
        foreach (var p in model.Properties)
        {
            sb.AppendLine($"            _pos_{p.FieldId} = FindPos(schema, {p.FieldId});");
        }
        sb.AppendLine("            var list = new global::System.Collections.Generic.List<global::Iceberg.Net.Linq.IcebergMemberBinding>();");
        foreach (var p in model.Properties)
        {
            sb.AppendLine($"            var f_{p.FieldId} = schema.Columns.FirstOrDefault(c => c.Id == {p.FieldId}) ?? global::Iceberg.Net.Types.NestedField.Optional({p.FieldId}, \"{p.ColumnName}\", {p.IcebergTypeExpr});");
            if (p.Conversion == ConversionKind.Decimal)
            {
                sb.AppendLine($"            _scale_{p.FieldId} = (f_{p.FieldId}.Type as global::Iceberg.Net.Types.DecimalType)?.Scale ?? 18;");
            }
            sb.AppendLine($"            var prop_{p.PropertyName} = typeof({model.TypeName}).GetProperty(nameof({model.TypeName}.{p.PropertyName}))!;");

            string toIcebergExpr;
            string fromIcebergExpr;
            switch (p.Conversion)
            {
                case ConversionKind.Decimal:
                    toIcebergExpr = $"v => v is decimal m ? global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(m, _scale_{p.FieldId}) : v";
                    fromIcebergExpr = $"v => v is global::Iceberg.Net.Types.IcebergDecimal id ? id.ToDecimal() : v";
                    break;
                case ConversionKind.DateOnly:
                    toIcebergExpr = $"v => v is global::System.DateOnly d ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(d) : v";
                    fromIcebergExpr = $"v => v is int i ? global::Iceberg.Net.Types.IcebergTime.DateFromDays(i) : v";
                    break;
                case ConversionKind.TimeOnly:
                    toIcebergExpr = $"v => v is global::System.TimeOnly t ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(t) : v";
                    fromIcebergExpr = $"v => v is long l ? global::Iceberg.Net.Types.IcebergTime.TimeFromMicros(l) : v";
                    break;
                case ConversionKind.DateTime:
                    toIcebergExpr = $"v => v is global::System.DateTime dt ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dt) : v";
                    fromIcebergExpr = $"v => v is long l ? global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(l) : v";
                    break;
                case ConversionKind.DateTimeOffset:
                    toIcebergExpr = $"v => v is global::System.DateTimeOffset dto ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dto) : v";
                    fromIcebergExpr = $"v => v is long l ? new global::System.DateTimeOffset(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(l), global::System.TimeSpan.Zero) : v";
                    break;
                default:
                    toIcebergExpr = "v => v";
                    fromIcebergExpr = "v => v";
                    break;
            }
            sb.AppendLine($"            list.Add(new global::Iceberg.Net.Linq.IcebergMemberBinding(prop_{p.PropertyName}, prop_{p.PropertyName}.PropertyType, f_{p.FieldId}, {toIcebergExpr}, {fromIcebergExpr}));");
        }
        sb.AppendLine("            Members = list;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // Emit ToRow
        sb.AppendLine($"        public object?[] ToRow({model.TypeName} item)");
        sb.AppendLine("        {");
        sb.AppendLine("            var row = new object?[Schema.Columns.Count];");
        for (int i = 0; i < model.Properties.Length; i++)
        {
            var p = model.Properties[i];
            string valExpr;
            switch (p.Conversion)
            {
                case ConversionKind.Decimal:
                    valExpr = p.IsNullable
                        ? $"item.{p.PropertyName} is {{ }} m_{p.FieldId} ? (object)global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(m_{p.FieldId}, _scale_{p.FieldId}) : null"
                        : $"global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(item.{p.PropertyName}, _scale_{p.FieldId})";
                    break;
                case ConversionKind.DateOnly:
                    valExpr = p.IsNullable
                        ? $"item.{p.PropertyName} is {{ }} d_{p.FieldId} ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(d_{p.FieldId}) : null"
                        : $"global::Iceberg.Net.Types.IcebergTime.DaysFromDate(item.{p.PropertyName})";
                    break;
                case ConversionKind.TimeOnly:
                    valExpr = p.IsNullable
                        ? $"item.{p.PropertyName} is {{ }} t_{p.FieldId} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(t_{p.FieldId}) : null"
                        : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(item.{p.PropertyName})";
                    break;
                case ConversionKind.DateTime:
                    valExpr = p.IsNullable
                        ? $"item.{p.PropertyName} is {{ }} dt_{p.FieldId} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dt_{p.FieldId}) : null"
                        : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(item.{p.PropertyName})";
                    break;
                case ConversionKind.DateTimeOffset:
                    valExpr = p.IsNullable
                        ? $"item.{p.PropertyName} is {{ }} dto_{p.FieldId} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dto_{p.FieldId}) : null"
                        : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(item.{p.PropertyName})";
                    break;
                default:
                    valExpr = $"item.{p.PropertyName}";
                    break;
            }
            sb.AppendLine($"            if (_pos_{p.FieldId} >= 0) row[_pos_{p.FieldId}] = {valExpr};");
        }
        sb.AppendLine("            return row;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // Emit CreateReader
        sb.AppendLine($"        public global::System.Func<object?[], {model.TypeName}> CreateReader(global::Iceberg.Net.Types.Schema rowSchema)");
        sb.AppendLine("        {");
        for (int i = 0; i < model.Properties.Length; i++)
        {
            var p = model.Properties[i];
            sb.AppendLine($"            int p_{p.FieldId} = FindPos(rowSchema, {p.FieldId});");
        }
        sb.AppendLine("            return row =>");
        sb.AppendLine("            {");
        if (model.UseConstructor)
        {
            sb.Append($"                return new {model.TypeName}(");
            for (int i = 0; i < model.ConstructorParameters.Length; i++)
            {
                var paramName = model.ConstructorParameters[i];
                var p = model.Properties.First(pr => pr.PropertyName == paramName);
                var comma = i < model.ConstructorParameters.Length - 1 ? ", " : "";
                string readExpr = GenerateReadExpr(p);
                sb.Append($"{readExpr}{comma}");
            }
            sb.AppendLine(");");
        }
        else
        {
            sb.AppendLine($"                return new {model.TypeName}");
            sb.AppendLine("                {");
            for (int i = 0; i < model.Properties.Length; i++)
            {
                var p = model.Properties[i];
                var comma = i < model.Properties.Length - 1 ? "," : "";
                string readExpr = GenerateReadExpr(p);
                sb.AppendLine($"                    {p.PropertyName} = {readExpr}{comma}");
            }
            sb.AppendLine("                };");
        }
        sb.AppendLine("            };");
        sb.AppendLine("        }");
        sb.AppendLine();

        // Emit FindPos helper
        sb.AppendLine("        private static int FindPos(global::Iceberg.Net.Types.Schema s, int id)");
        sb.AppendLine("        {");
        sb.AppendLine("            for (int i = 0; i < s.Columns.Count; i++) if (s.Columns[i].Id == id) return i;");
        sb.AppendLine("            return -1;");
        sb.AppendLine("        }");

        sb.AppendLine("    }");
        sb.AppendLine("}");

        spc.AddSource($"{model.TypeName}.Iceberg.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private static string GenerateReadExpr(MappablePropertyModel p)
    {
        switch (p.Conversion)
        {
            case ConversionKind.Decimal:
                return p.IsNullable
                    ? $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is not null) ? (row[p_{p.FieldId}] is global::Iceberg.Net.Types.IcebergDecimal id_{p.FieldId} ? id_{p.FieldId}.ToDecimal() : (decimal)row[p_{p.FieldId}]!) : default"
                    : $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is not null) ? (row[p_{p.FieldId}] is global::Iceberg.Net.Types.IcebergDecimal id_{p.FieldId} ? id_{p.FieldId}.ToDecimal() : (decimal)row[p_{p.FieldId}]!) : 0m";
            case ConversionKind.DateOnly:
                return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is int d_{p.FieldId}) ? global::Iceberg.Net.Types.IcebergTime.DateFromDays(d_{p.FieldId}) : default";
            case ConversionKind.TimeOnly:
                return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is long t_{p.FieldId}) ? global::Iceberg.Net.Types.IcebergTime.TimeFromMicros(t_{p.FieldId}) : default";
            case ConversionKind.DateTime:
                return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is long dt_{p.FieldId}) ? global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(dt_{p.FieldId}) : default";
            case ConversionKind.DateTimeOffset:
                return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is long dto_{p.FieldId}) ? new global::System.DateTimeOffset(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(dto_{p.FieldId}), global::System.TimeSpan.Zero) : default";
            default:
                if (p.IsNullable)
                    return $"p_{p.FieldId} >= 0 ? ({p.TypeDisplay})row[p_{p.FieldId}] : default";
                else if (p.IsValueType)
                    return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is not null) ? ({p.TypeDisplay})row[p_{p.FieldId}]! : default";
                else
                    return $"(p_{p.FieldId} >= 0 && row[p_{p.FieldId}] is not null) ? ({p.TypeDisplay})row[p_{p.FieldId}]! : default!";
        }
    }

    private enum ConversionKind
    {
        Identity,
        Decimal,
        DateOnly,
        TimeOnly,
        DateTime,
        DateTimeOffset
    }

    private sealed record MappableTypeModel(
        string? Namespace,
        string TypeName,
        string TypeKind,
        ImmutableArray<MappablePropertyModel> Properties,
        bool UseConstructor,
        ImmutableArray<string> ConstructorParameters
    );

    private sealed record MappablePropertyModel(
        string PropertyName,
        string ColumnName,
        int FieldId,
        string TypeDisplay,
        bool IsNullable,
        bool IsValueType,
        bool IsKey,
        string IcebergTypeExpr,
        ConversionKind Conversion
    );
}
