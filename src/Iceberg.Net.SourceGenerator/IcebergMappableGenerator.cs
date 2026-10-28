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

    private static readonly DiagnosticDescriptor UnsupportedTypeDiagnostic = new(
        id: "ICERG001",
        title: "Unsupported property type for Iceberg source generation",
        messageFormat: "Property '{0}' on type '{1}' has unsupported type '{2}' for Iceberg source generation. Remove [IcebergMappable] to use ReflectionRowMapper or specify a supported type.",
        category: "Iceberg",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor OptionalKeyDiagnostic = new(
        id: "ICERG002",
        title: "Identifier fields cannot be optional",
        messageFormat: "Property '{0}' on type '{1}' is marked with [IcebergKey] but is optional; identifier fields must be required",
        category: "Iceberg",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true);

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
        var diagnostics = new List<Diagnostic>();

        foreach (var member in symbol.GetMembers().OfType<IPropertySymbol>())
        {
            if (member.IsStatic || member.IsIndexer || member.DeclaredAccessibility != Accessibility.Public)
                continue;

            if (member.GetAttributes().Any(a => a.AttributeClass?.Name is "IcebergIgnore" or "IcebergIgnoreAttribute"))
                continue;

            int? explicitFieldId = null;
            string? customName = null;
            string? declaredType = null;
            int? nullability = null;
            string? doc = null;
            bool isKey = false;

            var fieldAttr = member.GetAttributes().FirstOrDefault(a => a.AttributeClass?.Name is "IcebergField" or "IcebergFieldAttribute");
            if (fieldAttr is not null)
            {
                if (fieldAttr.ConstructorArguments.Length > 0 && fieldAttr.ConstructorArguments[0].Value is int explicitId && explicitId > 0)
                {
                    explicitFieldId = explicitId;
                }
                foreach (var named in fieldAttr.NamedArguments)
                {
                    if (named.Key == "Name" && named.Value.Value is string s) customName = s;
                    if (named.Key == "Id" && named.Value.Value is int id && id > 0) explicitFieldId = id;
                    if (named.Key == "Type" && named.Value.Value is string t) declaredType = t;
                    if (named.Key == "Nullability" && named.Value.Value is int n) nullability = n;
                    if (named.Key == "Doc" && named.Value.Value is string d) doc = d;
                }
            }

            if (member.GetAttributes().Any(a => a.AttributeClass?.Name is "IcebergKey" or "IcebergKeyAttribute"))
            {
                isKey = true;
            }

            var typeSymbol = member.Type;
            bool clrNullable = typeSymbol.NullableAnnotation == NullableAnnotation.Annotated ||
                              (typeSymbol is INamedTypeSymbol nts && nts.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T);

            bool isNullable;
            if (isKey)
            {
                if (clrNullable || nullability == 2)
                {
                    var loc = member.Locations.FirstOrDefault() ?? Location.None;
                    diagnostics.Add(Diagnostic.Create(OptionalKeyDiagnostic, loc, member.Name, typeName));
                }
                isNullable = false;
            }
            else if (nullability == 1) // Required
            {
                isNullable = false;
            }
            else if (nullability == 2) // Optional
            {
                isNullable = true;
            }
            else
            {
                isNullable = clrNullable;
            }

            var typeDisplay = typeSymbol.ToDisplayString(DisplayFormat);
            var isValueType = typeSymbol.IsValueType;
            bool hasPublicSetter = member.SetMethod is not null && member.SetMethod.DeclaredAccessibility == Accessibility.Public;

            TypeAndConversion? resolved = null;
            if (declaredType is not null)
            {
                resolved = ParseDeclaredType(declaredType, typeSymbol);
            }
            resolved ??= GetDefaultTypeAndConversion(typeSymbol);

            if (resolved is null)
            {
                var loc = member.Locations.FirstOrDefault() ?? Location.None;
                diagnostics.Add(Diagnostic.Create(UnsupportedTypeDiagnostic, loc, member.Name, typeName, typeDisplay));
                continue;
            }

            int fieldId = explicitFieldId ?? defaultFieldId++;

            properties.Add(new MappablePropertyModel(
                member.Name,
                customName ?? member.Name,
                fieldId,
                explicitFieldId,
                customName,
                doc,
                typeDisplay,
                isNullable,
                isValueType,
                hasPublicSetter,
                isKey,
                resolved.TypeExpr,
                resolved.Conversion,
                resolved.Scale
            ));
        }

        // Determine matching constructor
        var ctors = symbol.Constructors.Where(c => !c.IsStatic && c.DeclaredAccessibility == Accessibility.Public).ToList();
        var validCtors = ctors.Where(c => c.Parameters.Length > 0 && c.Parameters.All(param =>
            properties.Any(pr => string.Equals(pr.PropertyName, param.Name, StringComparison.OrdinalIgnoreCase))
        )).OrderByDescending(c => c.Parameters.Length).ToList();

        var bestCtor = validCtors.FirstOrDefault();
        bool useConstructor = bestCtor is not null;
        var ctorParams = new List<string>();

        if (bestCtor is not null)
        {
            foreach (var p in bestCtor.Parameters)
            {
                var pr = properties.First(pr => string.Equals(pr.PropertyName, p.Name, StringComparison.OrdinalIgnoreCase));
                ctorParams.Add(pr.PropertyName);
            }
        }

        return new MappableTypeModel(
            ns,
            typeName,
            typeKind,
            properties.ToImmutableArray(),
            useConstructor,
            ctorParams.ToImmutableArray(),
            diagnostics.ToImmutableArray()
        );
    }

    private static TypeAndConversion? ParseDeclaredType(string declared, ITypeSymbol typeSymbol)
    {
        var trimmed = declared.Trim();
        if (trimmed.StartsWith("decimal(", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith(")"))
        {
            var inner = trimmed.Substring(8, trimmed.Length - 9);
            var parts = inner.Split(',');
            if (parts.Length == 2 && int.TryParse(parts[0].Trim(), out var p) && int.TryParse(parts[1].Trim(), out var s))
            {
                return new TypeAndConversion($"new global::Iceberg.Net.Types.DecimalType({p}, {s})", ConversionKind.Decimal, s);
            }
        }
        if (trimmed.StartsWith("fixed[", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith("]"))
        {
            var lenStr = trimmed.Substring(6, trimmed.Length - 7);
            if (int.TryParse(lenStr.Trim(), out var len))
            {
                return new TypeAndConversion($"new global::Iceberg.Net.Types.FixedType({len})", ConversionKind.Fixed, 0);
            }
        }

        return trimmed.ToLowerInvariant() switch
        {
            "boolean" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Boolean", ConversionKind.Identity, 0),
            "int" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.Int, 0),
            "long" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Long", ConversionKind.Long, 0),
            "float" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Float", ConversionKind.Identity, 0),
            "double" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Double", ConversionKind.Identity, 0),
            "string" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.String", ConversionKind.Identity, 0),
            "uuid" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Uuid", ConversionKind.Identity, 0),
            "binary" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Binary", ConversionKind.Identity, 0),
            "date" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Date",
                typeSymbol.ToDisplayString().Contains("DateTime") ? ConversionKind.DateTimeDate : ConversionKind.DateOnly, 0),
            "time" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Time",
                typeSymbol.ToDisplayString().Contains("TimeSpan") ? ConversionKind.TimeSpan : ConversionKind.TimeOnly, 0),
            "timestamp" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Timestamp",
                typeSymbol.ToDisplayString().Contains("DateTimeOffset") ? ConversionKind.DateTimeOffsetTimestamp : ConversionKind.DateTime, 0),
            "timestamptz" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.TimestampTz",
                typeSymbol.ToDisplayString().Contains("DateTimeOffset") ? ConversionKind.DateTimeOffset : ConversionKind.DateTimeTz, 0),
            _ => null
        };
    }

    private static TypeAndConversion? GetDefaultTypeAndConversion(ITypeSymbol typeSymbol)
    {
        var unwrapped = (typeSymbol is INamedTypeSymbol nts && nts.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            ? nts.TypeArguments[0]
            : typeSymbol;

        var nonNullable = unwrapped.WithNullableAnnotation(NullableAnnotation.None);

        if (nonNullable.TypeKind == TypeKind.Enum)
        {
            var named = (INamedTypeSymbol)nonNullable;
            var underlying = named.EnumUnderlyingType?.SpecialType;
            if (underlying is SpecialType.System_Int64 or SpecialType.System_UInt32 or SpecialType.System_UInt64)
            {
                return new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Long", ConversionKind.EnumLong, 0);
            }
            return new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.EnumInt, 0);
        }

        var full = nonNullable.ToDisplayString();
        return full switch
        {
            "bool" or "System.Boolean" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Boolean", ConversionKind.Identity, 0),
            "int" or "System.Int32" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.Identity, 0),
            "short" or "System.Int16" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.Short, 0),
            "byte" or "System.Byte" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.Byte, 0),
            "sbyte" or "System.SByte" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.SByte, 0),
            "ushort" or "System.UInt16" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Int", ConversionKind.UShort, 0),
            "long" or "System.Int64" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Long", ConversionKind.Identity, 0),
            "uint" or "System.UInt32" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Long", ConversionKind.UInt, 0),
            "float" or "System.Single" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Float", ConversionKind.Identity, 0),
            "double" or "System.Double" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Double", ConversionKind.Identity, 0),
            "string" or "System.String" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.String", ConversionKind.Identity, 0),
            "decimal" or "System.Decimal" => new TypeAndConversion("new global::Iceberg.Net.Types.DecimalType(38, 18)", ConversionKind.Decimal, 18),
            "System.Guid" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Uuid", ConversionKind.Identity, 0),
            "byte[]" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Binary", ConversionKind.Identity, 0),
            "System.DateOnly" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Date", ConversionKind.DateOnly, 0),
            "System.TimeOnly" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Time", ConversionKind.TimeOnly, 0),
            "System.TimeSpan" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Time", ConversionKind.TimeSpan, 0),
            "System.DateTime" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.Timestamp", ConversionKind.DateTime, 0),
            "System.DateTimeOffset" => new TypeAndConversion("global::Iceberg.Net.Types.PrimitiveType.TimestampTz", ConversionKind.DateTimeOffset, 0),
            _ => null
        };
    }

    private static void Emit(SourceProductionContext spc, MappableTypeModel model)
    {
        foreach (var diag in model.Diagnostics)
        {
            spc.ReportDiagnostic(diag);
        }

        if (model.Diagnostics.Any(d => d.Severity == DiagnosticSeverity.Error))
        {
            return;
        }

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
        var keyProps = model.Properties.Where(p => p.IsKey).ToList();
        sb.AppendLine($"    /// <summary>Compile-time Iceberg schema derived for <see cref=\"{model.TypeName}\"/>.</summary>");
        sb.AppendLine($"    public static global::Iceberg.Net.Types.Schema IcebergSchema {{ get; }} = new global::Iceberg.Net.Types.Schema(0, new global::Iceberg.Net.Types.NestedField[]");
        sb.AppendLine("    {");
        for (int i = 0; i < model.Properties.Length; i++)
        {
            var p = model.Properties[i];
            var reqOrOpt = p.IsNullable ? "Optional" : "Require";
            var docParam = p.Doc is not null ? $", doc: \"{EscapeString(p.Doc)}\"" : "";
            var comma = i < model.Properties.Length - 1 ? "," : "";
            sb.AppendLine($"        global::Iceberg.Net.Types.NestedField.{reqOrOpt}({p.FieldId}, \"{p.ColumnName}\", {p.IcebergTypeExpr}{docParam}){comma}");
        }
        if (keyProps.Count > 0)
        {
            var keyIdsStr = string.Join(", ", keyProps.Select(k => k.FieldId));
            sb.AppendLine($"    }}, new int[] {{ {keyIdsStr} }});");
        }
        else
        {
            sb.AppendLine("    });");
        }
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
        sb.AppendLine($"            return new __GeneratedRowBinding(schema, nameMapping, _options);");
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
            sb.AppendLine($"        private readonly int _pos_{p.PropertyName};");
            sb.AppendLine($"        private readonly int _id_{p.PropertyName};");
            if (p.Conversion == ConversionKind.Decimal)
            {
                sb.AppendLine($"        private readonly int _scale_{p.PropertyName};");
            }
        }
        sb.AppendLine();
        sb.AppendLine($"        public __GeneratedRowBinding(global::Iceberg.Net.Types.Schema schema, global::Iceberg.Net.Types.NameMapping? nameMapping, global::Iceberg.Net.Linq.IcebergMapperOptions? options)");
        sb.AppendLine("        {");
        sb.AppendLine("            Schema = schema;");
        sb.AppendLine("            var list = new global::System.Collections.Generic.List<global::Iceberg.Net.Linq.IcebergMemberBinding>();");
        foreach (var p in model.Properties)
        {
            var explicitIdStr = p.ExplicitFieldId.HasValue ? p.ExplicitFieldId.Value.ToString() : "null";
            var customNameStr = p.CustomName is not null ? $"\"{EscapeString(p.CustomName)}\"" : "null";
            sb.AppendLine($"            var f_{p.PropertyName} = Resolve(\"{p.PropertyName}\", {explicitIdStr}, {customNameStr}, schema, nameMapping, options);");
            sb.AppendLine($"            if (f_{p.PropertyName} is not null)");
            sb.AppendLine("            {");
            sb.AppendLine($"                _pos_{p.PropertyName} = FindPos(schema, f_{p.PropertyName}.Id);");
            sb.AppendLine($"                _id_{p.PropertyName} = f_{p.PropertyName}.Id;");
            if (p.Conversion == ConversionKind.Decimal)
            {
                sb.AppendLine($"                _scale_{p.PropertyName} = (f_{p.PropertyName}.Type as global::Iceberg.Net.Types.DecimalType)?.Scale ?? {p.Scale};");
            }
            sb.AppendLine($"                var prop_{p.PropertyName} = typeof({model.TypeName}).GetProperty(nameof({model.TypeName}.{p.PropertyName}))!;");
            sb.AppendLine($"                list.Add(new global::Iceberg.Net.Linq.IcebergMemberBinding(prop_{p.PropertyName}, prop_{p.PropertyName}.PropertyType, f_{p.PropertyName}, {GetToIcebergExpr(p)}, {GetFromIcebergExpr(p)}));");
            sb.AppendLine("            }");
            sb.AppendLine("            else");
            sb.AppendLine("            {");
            sb.AppendLine($"                _pos_{p.PropertyName} = -1;");
            sb.AppendLine($"                _id_{p.PropertyName} = -1;");
            if (p.Conversion == ConversionKind.Decimal)
            {
                sb.AppendLine($"                _scale_{p.PropertyName} = {p.Scale};");
            }
            sb.AppendLine("            }");
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
            sb.AppendLine($"            if (_pos_{p.PropertyName} >= 0) row[_pos_{p.PropertyName}] = {GetWriteExpr(p)};");
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
            sb.AppendLine($"            int p_{p.PropertyName} = _id_{p.PropertyName} >= 0 ? FindPos(rowSchema, _id_{p.PropertyName}) : -1;");
        }
        sb.AppendLine("            return row =>");
        sb.AppendLine("            {");

        var ctorParamNames = new HashSet<string>(model.ConstructorParameters, StringComparer.OrdinalIgnoreCase);
        var remainingProperties = model.Properties.Where(pr => !ctorParamNames.Contains(pr.PropertyName) && pr.HasPublicSetter).ToList();

        if (model.UseConstructor)
        {
            sb.Append($"                return new {model.TypeName}(");
            for (int i = 0; i < model.ConstructorParameters.Length; i++)
            {
                var paramName = model.ConstructorParameters[i];
                var p = model.Properties.First(pr => string.Equals(pr.PropertyName, paramName, StringComparison.OrdinalIgnoreCase));
                var comma = i < model.ConstructorParameters.Length - 1 ? ", " : "";
                sb.Append($"{GenerateReadExpr(p)}{comma}");
            }
            sb.Append(")");
            if (remainingProperties.Count > 0)
            {
                sb.AppendLine();
                sb.AppendLine("                {");
                for (int i = 0; i < remainingProperties.Count; i++)
                {
                    var p = remainingProperties[i];
                    var comma = i < remainingProperties.Count - 1 ? "," : "";
                    sb.AppendLine($"                    {p.PropertyName} = {GenerateReadExpr(p)}{comma}");
                }
                sb.AppendLine("                };");
            }
            else
            {
                sb.AppendLine(";");
            }
        }
        else
        {
            sb.AppendLine($"                return new {model.TypeName}");
            sb.AppendLine("                {");
            for (int i = 0; i < remainingProperties.Count; i++)
            {
                var p = remainingProperties[i];
                var comma = i < remainingProperties.Count - 1 ? "," : "";
                sb.AppendLine($"                    {p.PropertyName} = {GenerateReadExpr(p)}{comma}");
            }
            sb.AppendLine("                };");
        }
        sb.AppendLine("            };");
        sb.AppendLine("        }");
        sb.AppendLine();

        // Emit Resolve and FindPos helpers
        sb.AppendLine("        private static global::Iceberg.Net.Types.NestedField? Resolve(");
        sb.AppendLine("            string memberName,");
        sb.AppendLine("            int? explicitId,");
        sb.AppendLine("            string? customName,");
        sb.AppendLine("            global::Iceberg.Net.Types.Schema schema,");
        sb.AppendLine("            global::Iceberg.Net.Types.NameMapping? nameMapping,");
        sb.AppendLine("            global::Iceberg.Net.Linq.IcebergMapperOptions? options)");
        sb.AppendLine("        {");
        sb.AppendLine("            options ??= global::Iceberg.Net.Linq.IcebergMapperOptions.Default;");
        sb.AppendLine("            if (explicitId is { } eid && eid > 0)");
        sb.AppendLine("            {");
        sb.AppendLine("                return schema.Columns.FirstOrDefault(f => f.Id == eid)");
        sb.AppendLine("                    ?? (options.IgnoreUnmatchedMembers ? null : throw new global::System.InvalidOperationException($\"{memberName} is bound to field ID {eid}, which is not in the schema\"));");
        sb.AppendLine("            }");
        sb.AppendLine("            var configuredId = options.FieldIds?.Invoke(memberName);");
        sb.AppendLine("            if (configuredId is { } fid)");
        sb.AppendLine("            {");
        sb.AppendLine("                return schema.Columns.FirstOrDefault(f => f.Id == fid)");
        sb.AppendLine("                    ?? (options.IgnoreUnmatchedMembers ? null : throw new global::System.InvalidOperationException($\"{memberName} is bound to field ID {fid}, which is not in the schema\"));");
        sb.AppendLine("            }");
        sb.AppendLine("            var name = customName ?? options.NameConverter?.Invoke(memberName) ?? memberName;");
        sb.AppendLine("            var exact = schema.Columns.FirstOrDefault(f => f.Name == name);");
        sb.AppendLine("            if (exact is not null) return exact;");
        sb.AppendLine("            if (nameMapping?.Fields?.FirstOrDefault(x => x.FieldId is not null && x.Names.Contains(name)) is { FieldId: { } aliasId } &&");
        sb.AppendLine("                schema.Columns.FirstOrDefault(f => f.Id == aliasId) is { } alias)");
        sb.AppendLine("                return alias;");
        sb.AppendLine("            if (options.CaseInsensitive)");
        sb.AppendLine("            {");
        sb.AppendLine("                var matches = schema.Columns.Where(f => string.Equals(f.Name, name, global::System.StringComparison.OrdinalIgnoreCase)).ToList();");
        sb.AppendLine("                if (matches.Count == 1) return matches[0];");
        sb.AppendLine("                if (matches.Count > 1) throw new global::System.InvalidOperationException($\"{memberName} matches {matches.Count} columns case-insensitively\");");
        sb.AppendLine("            }");
        sb.AppendLine("            return options.IgnoreUnmatchedMembers ? null");
        sb.AppendLine("                : throw new global::System.InvalidOperationException($\"{memberName} matches no column (looked for '{name}')\");");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        private static int FindPos(global::Iceberg.Net.Types.Schema s, int id)");
        sb.AppendLine("        {");
        sb.AppendLine("            for (int i = 0; i < s.Columns.Count; i++) if (s.Columns[i].Id == id) return i;");
        sb.AppendLine("            return -1;");
        sb.AppendLine("        }");

        sb.AppendLine("    }");
        sb.AppendLine("}");

        spc.AddSource($"{model.TypeName}.Iceberg.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private static string GetWriteExpr(MappablePropertyModel p) => p.Conversion switch
    {
        ConversionKind.Decimal => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} m_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(m_{p.PropertyName}, _scale_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(item.{p.PropertyName}, _scale_{p.PropertyName})",
        ConversionKind.DateOnly => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} d_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(d_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergTime.DaysFromDate(item.{p.PropertyName})",
        ConversionKind.DateTimeDate => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} dt_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(dt_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergTime.DaysFromDate(item.{p.PropertyName})",
        ConversionKind.TimeOnly => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} t_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(t_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(item.{p.PropertyName})",
        ConversionKind.TimeSpan => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} ts_{p.PropertyName} ? (object)(ts_{p.PropertyName}.Ticks / 10) : null"
            : $"item.{p.PropertyName}.Ticks / 10",
        ConversionKind.DateTime or ConversionKind.DateTimeTz => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} dt_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dt_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(item.{p.PropertyName})",
        ConversionKind.DateTimeOffset or ConversionKind.DateTimeOffsetTimestamp => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} dto_{p.PropertyName} ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dto_{p.PropertyName}) : null"
            : $"global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(item.{p.PropertyName})",
        ConversionKind.Short => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} s_{p.PropertyName} ? (object)(int)s_{p.PropertyName} : null"
            : $"(int)item.{p.PropertyName}",
        ConversionKind.Byte => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} b_{p.PropertyName} ? (object)(int)b_{p.PropertyName} : null"
            : $"(int)item.{p.PropertyName}",
        ConversionKind.SByte => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} sb_{p.PropertyName} ? (object)(int)sb_{p.PropertyName} : null"
            : $"(int)item.{p.PropertyName}",
        ConversionKind.UShort => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} us_{p.PropertyName} ? (object)(int)us_{p.PropertyName} : null"
            : $"(int)item.{p.PropertyName}",
        ConversionKind.UInt => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} u_{p.PropertyName} ? (object)(long)u_{p.PropertyName} : null"
            : $"(long)item.{p.PropertyName}",
        ConversionKind.EnumInt => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} e_{p.PropertyName} ? (object)(int)e_{p.PropertyName} : null"
            : $"(int)item.{p.PropertyName}",
        ConversionKind.EnumLong => p.IsNullable
            ? $"item.{p.PropertyName} is {{ }} e_{p.PropertyName} ? (object)(long)e_{p.PropertyName} : null"
            : $"(long)item.{p.PropertyName}",
        _ => $"item.{p.PropertyName}"
    };

    private static string GenerateReadExpr(MappablePropertyModel p) => p.Conversion switch
    {
        ConversionKind.Decimal => p.IsNullable
            ? $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is not null) ? (row[p_{p.PropertyName}] is global::Iceberg.Net.Types.IcebergDecimal id_{p.PropertyName} ? id_{p.PropertyName}.ToDecimal() : (decimal)row[p_{p.PropertyName}]!) : default"
            : $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is not null) ? (row[p_{p.PropertyName}] is global::Iceberg.Net.Types.IcebergDecimal id_{p.PropertyName} ? id_{p.PropertyName}.ToDecimal() : (decimal)row[p_{p.PropertyName}]!) : 0m",
        ConversionKind.DateOnly => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int d_{p.PropertyName}) ? global::Iceberg.Net.Types.IcebergTime.DateFromDays(d_{p.PropertyName}) : default",
        ConversionKind.DateTimeDate => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int d_{p.PropertyName}) ? global::System.DateTime.SpecifyKind(global::Iceberg.Net.Types.IcebergTime.Epoch.AddDays(d_{p.PropertyName}), global::System.DateTimeKind.Unspecified) : default",
        ConversionKind.TimeOnly => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long t_{p.PropertyName}) ? global::Iceberg.Net.Types.IcebergTime.TimeFromMicros(t_{p.PropertyName}) : default",
        ConversionKind.TimeSpan => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long ts_{p.PropertyName}) ? global::System.TimeSpan.FromTicks(ts_{p.PropertyName} * 10) : default",
        ConversionKind.DateTime => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long dt_{p.PropertyName}) ? global::System.DateTime.SpecifyKind(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(dt_{p.PropertyName}), global::System.DateTimeKind.Unspecified) : default",
        ConversionKind.DateTimeTz => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long dt_{p.PropertyName}) ? global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(dt_{p.PropertyName}) : default",
        ConversionKind.DateTimeOffset or ConversionKind.DateTimeOffsetTimestamp => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long dto_{p.PropertyName}) ? new global::System.DateTimeOffset(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(dto_{p.PropertyName}), global::System.TimeSpan.Zero) : default",
        ConversionKind.Short => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int s_{p.PropertyName}) ? (short)s_{p.PropertyName} : default",
        ConversionKind.Byte => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int b_{p.PropertyName}) ? (byte)b_{p.PropertyName} : default",
        ConversionKind.SByte => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int sb_{p.PropertyName}) ? (sbyte)sb_{p.PropertyName} : default",
        ConversionKind.UShort => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int us_{p.PropertyName}) ? (ushort)us_{p.PropertyName} : default",
        ConversionKind.UInt => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long u_{p.PropertyName}) ? (uint)u_{p.PropertyName} : default",
        ConversionKind.EnumInt => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is int e_{p.PropertyName}) ? ({p.TypeDisplay})e_{p.PropertyName} : default",
        ConversionKind.EnumLong => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is long e_{p.PropertyName}) ? ({p.TypeDisplay})e_{p.PropertyName} : default",
        ConversionKind.Fixed => $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is byte[] f_{p.PropertyName}) ? f_{p.PropertyName} : default",
        _ => p.IsNullable
            ? $"p_{p.PropertyName} >= 0 ? ({p.TypeDisplay})row[p_{p.PropertyName}] : default"
            : p.IsValueType
                ? $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is not null) ? ({p.TypeDisplay})row[p_{p.PropertyName}]! : default"
                : $"(p_{p.PropertyName} >= 0 && row[p_{p.PropertyName}] is not null) ? ({p.TypeDisplay})row[p_{p.PropertyName}]! : default!"
    };

    private static string GetToIcebergExpr(MappablePropertyModel p) => p.Conversion switch
    {
        ConversionKind.Decimal => $"v => v is decimal m ? (object)global::Iceberg.Net.Types.IcebergDecimal.FromDecimal(m, _scale_{p.PropertyName}) : v",
        ConversionKind.DateOnly => "v => v is global::System.DateOnly d ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(d) : v",
        ConversionKind.DateTimeDate => "v => v is global::System.DateTime dt ? (object)global::Iceberg.Net.Types.IcebergTime.DaysFromDate(dt) : v",
        ConversionKind.TimeOnly => "v => v is global::System.TimeOnly t ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTime(t) : v",
        ConversionKind.TimeSpan => "v => v is global::System.TimeSpan ts ? (object)(ts.Ticks / 10) : v",
        ConversionKind.DateTime or ConversionKind.DateTimeTz => "v => v is global::System.DateTime dt ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dt) : v",
        ConversionKind.DateTimeOffset or ConversionKind.DateTimeOffsetTimestamp => "v => v is global::System.DateTimeOffset dto ? (object)global::Iceberg.Net.Types.IcebergTime.MicrosFromTimestamp(dto) : v",
        ConversionKind.Short => "v => v is short s ? (object)(int)s : v",
        ConversionKind.Byte => "v => v is byte b ? (object)(int)b : v",
        ConversionKind.SByte => "v => v is sbyte sb ? (object)(int)sb : v",
        ConversionKind.UShort => "v => v is ushort us ? (object)(int)us : v",
        ConversionKind.UInt => "v => v is uint u ? (object)(long)u : v",
        ConversionKind.EnumInt => "v => v is null ? null : (object)global::System.Convert.ToInt32(v)",
        ConversionKind.EnumLong => "v => v is null ? null : (object)global::System.Convert.ToInt64(v)",
        _ => "v => v"
    };

    private static string GetFromIcebergExpr(MappablePropertyModel p) => p.Conversion switch
    {
        ConversionKind.Decimal => "v => v is global::Iceberg.Net.Types.IcebergDecimal id ? (object)id.ToDecimal() : v",
        ConversionKind.DateOnly => "v => v is int i ? (object)global::Iceberg.Net.Types.IcebergTime.DateFromDays(i) : v",
        ConversionKind.DateTimeDate => "v => v is int i ? (object)global::System.DateTime.SpecifyKind(global::Iceberg.Net.Types.IcebergTime.Epoch.AddDays(i), global::System.DateTimeKind.Unspecified) : v",
        ConversionKind.TimeOnly => "v => v is long l ? (object)global::Iceberg.Net.Types.IcebergTime.TimeFromMicros(l) : v",
        ConversionKind.TimeSpan => "v => v is long l ? (object)global::System.TimeSpan.FromTicks(l * 10) : v",
        ConversionKind.DateTime => "v => v is long l ? (object)global::System.DateTime.SpecifyKind(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(l), global::System.DateTimeKind.Unspecified) : v",
        ConversionKind.DateTimeTz => "v => v is long l ? (object)global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(l) : v",
        ConversionKind.DateTimeOffset or ConversionKind.DateTimeOffsetTimestamp => "v => v is long l ? (object)new global::System.DateTimeOffset(global::Iceberg.Net.Types.IcebergTime.TimestampFromMicros(l), global::System.TimeSpan.Zero) : v",
        ConversionKind.Short => "v => v is int i ? (object)(short)i : v",
        ConversionKind.Byte => "v => v is int i ? (object)(byte)i : v",
        ConversionKind.SByte => "v => v is int i ? (object)(sbyte)i : v",
        ConversionKind.UShort => "v => v is int i ? (object)(ushort)i : v",
        ConversionKind.UInt => "v => v is long l ? (object)(uint)l : v",
        ConversionKind.EnumInt => $"v => v is int i ? (object)({p.TypeDisplay})i : v",
        ConversionKind.EnumLong => $"v => v is long l ? (object)({p.TypeDisplay})l : v",
        _ => "v => v"
    };

    private static string EscapeString(string str) => str.Replace("\"", "\\\"");

    private sealed record TypeAndConversion(string TypeExpr, ConversionKind Conversion, int Scale);

    private enum ConversionKind
    {
        Identity,
        Int,
        Long,
        Short,
        Byte,
        SByte,
        UShort,
        UInt,
        EnumInt,
        EnumLong,
        Decimal,
        DateOnly,
        DateTimeDate,
        TimeOnly,
        TimeSpan,
        DateTime,
        DateTimeTz,
        DateTimeOffset,
        DateTimeOffsetTimestamp,
        Fixed
    }

    private sealed record MappableTypeModel(
        string? Namespace,
        string TypeName,
        string TypeKind,
        ImmutableArray<MappablePropertyModel> Properties,
        bool UseConstructor,
        ImmutableArray<string> ConstructorParameters,
        ImmutableArray<Diagnostic> Diagnostics
    );

    private sealed record MappablePropertyModel(
        string PropertyName,
        string ColumnName,
        int FieldId,
        int? ExplicitFieldId,
        string? CustomName,
        string? Doc,
        string TypeDisplay,
        bool IsNullable,
        bool IsValueType,
        bool HasPublicSetter,
        bool IsKey,
        string IcebergTypeExpr,
        ConversionKind Conversion,
        int Scale
    );
}
