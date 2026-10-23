using System.Globalization;
using System.Text.Json.Nodes;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Json;

/// <summary>
/// Unbound expressions in the REST spec's JSON form (Java's <c>ExpressionParser</c>): <c>true</c> / <c>false</c>,
/// <c>{"type":"and"|"or","left","right"}</c>, <c>{"type":"not","child"}</c>, <c>{"type":"lt","term":"id","value":5}</c>,
/// <c>{"type":"in","term":"id","values":[1,2]}</c>, <c>{"type":"is-null","term":"id"}</c>. Terms are column names
/// (<c>{"type":"reference","term":…}</c> is read too); transform terms are not supported. Literals are JSON numbers,
/// strings and booleans; decimals, UUIDs, dates and timestamps travel as their text forms, which the receiver converts
/// when it binds the expression to the column's type.
/// </summary>
public static class ExpressionJson
{
    private static readonly Dictionary<Operation, string> Names = new()
    {
        [Operation.IsNull] = "is-null", [Operation.NotNull] = "not-null", [Operation.IsNaN] = "is-nan", [Operation.NotNaN] = "not-nan",
        [Operation.Lt] = "lt", [Operation.LtEq] = "lt-eq", [Operation.Gt] = "gt", [Operation.GtEq] = "gt-eq",
        [Operation.Eq] = "eq", [Operation.NotEq] = "not-eq", [Operation.In] = "in", [Operation.NotIn] = "not-in",
        [Operation.StartsWith] = "starts-with", [Operation.NotStartsWith] = "not-starts-with",
    };

    private static readonly Dictionary<string, Operation> Ops = Names.ToDictionary(kv => kv.Value, kv => kv.Key);

    public static JsonNode ToJson(Expression e) => e switch
    {
        AlwaysTrue => JsonValue.Create(true),
        AlwaysFalse => JsonValue.Create(false),
        And a => new JsonObject { ["type"] = "and", ["left"] = ToJson(a.Left), ["right"] = ToJson(a.Right) },
        Or o => new JsonObject { ["type"] = "or", ["left"] = ToJson(o.Left), ["right"] = ToJson(o.Right) },
        Not n => new JsonObject { ["type"] = "not", ["child"] = ToJson(n.Child) },
        UnboundPredicate p => Predicate(p),
        _ => throw new NotSupportedException($"Only unbound expressions have a JSON form ({e.GetType().Name})"),
    };

    private static JsonObject Predicate(UnboundPredicate p)
    {
        var o = new JsonObject { ["type"] = Names[p.Op], ["term"] = p.Column };
        switch (p.Op)
        {
            case Operation.IsNull or Operation.NotNull or Operation.IsNaN or Operation.NotNaN: break;
            case Operation.In or Operation.NotIn: o["values"] = new JsonArray(p.Literals.Select(Literal).ToArray()); break;
            default: o["value"] = Literal(p.Literals[0]); break;
        }
        return o;
    }

    private static JsonNode? Literal(object? v) => v switch
    {
        null => throw new ArgumentException("Null literals have no JSON form: use is-null"),
        bool b => JsonValue.Create(b),
        int i => JsonValue.Create(i),
        long l => JsonValue.Create(l),
        float f => JsonValue.Create(f),
        double d => JsonValue.Create(d),
        decimal m => JsonValue.Create(m.ToString(CultureInfo.InvariantCulture)),
        string s => JsonValue.Create(s),
        IcebergDecimal d => JsonValue.Create(d.ToString()),
        Guid g => JsonValue.Create(g.ToString("D")),
        DateTime dt => JsonValue.Create(dt.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture)),
        DateTimeOffset dto => JsonValue.Create(dto.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "+00:00"),
        _ when v.GetType().FullName == "System.DateOnly" => JsonValue.Create(((IFormattable)v).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        _ when v.GetType().FullName == "System.TimeOnly" => JsonValue.Create(((IFormattable)v).ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture)),
        _ => throw new NotSupportedException($"No JSON form for a {v.GetType().Name} literal"),
    };

    public static Expression FromJson(JsonNode? node) => Corruption.Guard("expression JSON", () => Parse(node));

    private static Expression Parse(JsonNode? node)
    {
        if (node is JsonValue bv && bv.TryGetValue<bool>(out var b)) return b ? Expression.True : Expression.False;
        var o = node as JsonObject ?? throw new InvalidDataException($"Expression must be a boolean or an object, got {node?.ToJsonString() ?? "null"}");
        var type = o["type"]?.GetValue<string>() ?? throw new InvalidDataException("Expression without 'type'");
        switch (type)
        {
            case "true": return Expression.True;
            case "false": return Expression.False;
            case "and": return Expr.And(Parse(o["left"]), Parse(o["right"]));
            case "or": return Expr.Or(Parse(o["left"]), Parse(o["right"]));
            case "not": return new Not(Parse(o["child"]));
        }
        if (!Ops.TryGetValue(type, out var op)) throw new NotSupportedException($"Unknown expression type '{type}'");
        var term = Term(o["term"]);
        IReadOnlyList<object?> literals = op switch
        {
            Operation.IsNull or Operation.NotNull or Operation.IsNaN or Operation.NotNaN => [],
            Operation.In or Operation.NotIn => (o["values"] as JsonArray ?? throw new InvalidDataException($"'{type}' needs 'values'")).Select(Value).ToList(),
            _ => [Value(o["value"] ?? throw new InvalidDataException($"'{type}' needs 'value'"))],
        };
        return new UnboundPredicate(term, op, literals);
    }

    private static string Term(JsonNode? t) => t switch
    {
        JsonValue v when v.TryGetValue<string>(out var name) => name,
        JsonObject { } r when r["type"]?.GetValue<string>() == "reference" => r["term"]!.GetValue<string>(),
        JsonObject { } r => throw new NotSupportedException($"Expression term of type '{r["type"]}' is not supported"),
        _ => throw new InvalidDataException("Expression without 'term'"),
    };

    private static object? Value(JsonNode? v)
    {
        if (v is not JsonValue value) throw new InvalidDataException($"Literal must be a JSON value, got {v?.ToJsonString() ?? "null"}");
        if (value.TryGetValue<bool>(out var b)) return b;
        if (value.TryGetValue<string>(out var s)) return s;
        var text = value.ToJsonString();
        if (long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var l)) return l;
        return double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}
