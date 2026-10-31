using System.Collections;
using System.Globalization;
using System.Reflection;
using Iceberg.Net.Types;

namespace Iceberg.Net.DuckLake;

/// <summary>
/// Physical values (as <see cref="DuckLakeCatalog.ReadAsync"/> returns them) as natural CLR values, and rows as objects.
/// <list type="table">
/// <item><term><c>int8</c>…<c>int64</c>, <c>uint8</c>…<c>uint64</c></term><description><c>sbyte</c>, <c>short</c>, <c>int</c>, <c>long</c>, <c>byte</c>, <c>ushort</c>, <c>uint</c>, <c>ulong</c></description></item>
/// <item><term><c>decimal(p,s)</c></term><description><see cref="decimal"/> (<see cref="IcebergDecimal"/> beyond its 28 digits)</description></item>
/// <item><term><c>date</c>, <c>time</c></term><description><see cref="DateOnly"/>, <see cref="TimeSpan"/> (which holds 24:00:00, as DuckDB's time does)</description></item>
/// <item><term><c>timestamp</c>, <c>_s</c>, <c>_ms</c>, <c>_ns</c></term><description><see cref="DateTime"/> (unspecified kind; nanoseconds truncated to 100 ns ticks)</description></item>
/// <item><term><c>timestamptz</c></term><description><see cref="DateTimeOffset"/> at offset 0</description></item>
/// <item><term><c>list</c>, <c>map</c>, <c>struct</c></term><description><c>List&lt;object?&gt;</c>, <c>Dictionary&lt;object, object?&gt;</c>, <c>Dictionary&lt;string, object?&gt;</c> by field name</description></item>
/// </list>
/// Strings, blobs, booleans, floats, <see cref="Guid"/>, <see cref="DuckLakeTimeTz"/> and <see cref="DuckLakeInterval"/> are as read.
/// Values outside a CLR type's range (dates before year 1, for instance) throw <see cref="OverflowException"/>.
/// </summary>
public static class DuckLakeClr
{
    public static object? ToClr(DuckLakeType type, object? value) => value is null ? null : Converter(type)(value);

    /// <summary>The conversion for one type, chosen once (per column, not per value).</summary>
    internal static Func<object, object> Converter(DuckLakeType type)
    {
        switch (type)
        {
            case DuckLakeList l:
            {
                var element = Converter(l.Element);
                return v => ((IEnumerable)v).Cast<object?>().Select(x => x is null ? null : element(x)).ToList();
            }
            case DuckLakeMap m:
            {
                var (key, val) = (Converter(m.Key), Converter(m.Value));
                return v =>
                {
                    var map = new Dictionary<object, object?>();
                    foreach (DictionaryEntry e in (IDictionary)v) map[key(e.Key)] = e.Value is null ? null : val(e.Value);
                    return map;
                };
            }
            case DuckLakeStruct s:
            {
                var fields = s.Fields.Select(f => (f.Name, Convert: Converter(f.Type))).ToArray();
                return v =>
                {
                    var values = (IList)v;
                    var result = new Dictionary<string, object?>(fields.Length, StringComparer.Ordinal);
                    for (var i = 0; i < fields.Length; i++) result[fields[i].Name] = values[i] is { } x ? fields[i].Convert(x) : null;
                    return result;
                };
            }
        }
        var p = (DuckLakePrimitive)type;
        return p.Name switch
        {
            "int8" => v => checked((sbyte)(int)v),
            "int16" => v => checked((short)(int)v),
            "uint8" => v => checked((byte)(int)v),
            "uint16" => v => checked((ushort)(int)v),
            "uint32" => v => checked((uint)(long)v),
            "uint64" => v => checked((ulong)((IcebergDecimal)v).Unscaled),
            "date" => v => IcebergTime.DateFromDays((int)v),
            "time" => v => TimeSpan.FromTicks(checked((long)v * 10)),
            "timestamp" or "timestamp_s" or "timestamp_ms" => v => DateTime.SpecifyKind(IcebergTime.Epoch.AddTicks(checked((long)v * 10)), DateTimeKind.Unspecified),
            "timestamptz" => v => new DateTimeOffset(IcebergTime.Epoch.AddTicks(checked((long)v * 10)), TimeSpan.Zero),
            "timestamp_ns" => v => DateTime.SpecifyKind(IcebergTime.Epoch.AddTicks(IcebergTime.FloorDiv((long)v, 100)), DateTimeKind.Unspecified),
            _ when p.Name.StartsWith("decimal", StringComparison.Ordinal) => v => TryDecimal((IcebergDecimal)v),
            _ => v => v,
        };
    }

    private static object TryDecimal(IcebergDecimal d)
    {
        try { return d.ToDecimal(); }
        catch (OverflowException) { return d; }
    }

    /// <summary>A row's values as CLR values, by column.</summary>
    public static object?[] ToClr(IReadOnlyList<DuckLakeColumn> columns, object?[] row) => RowConverter(columns)(row);

    /// <summary>Converts whole rows, with each column's conversion chosen once.</summary>
    public static Func<object?[], object?[]> RowConverter(IReadOnlyList<DuckLakeColumn> columns)
    {
        var converters = columns.Select(c => Converter(c.Type)).ToArray();
        return row =>
        {
            var result = new object?[converters.Length];
            for (var i = 0; i < converters.Length; i++) result[i] = row[i] is { } v ? converters[i](v) : null;
            return result;
        };
    }

    /// <summary>
    /// A binder from rows of <paramref name="columns"/> to <typeparamref name="T"/>: through the constructor whose
    /// parameters all name columns (the one with most parameters; records' primary constructors), then settable
    /// properties naming the remaining columns. Names match ignoring case and underscores (<c>order_id</c> ↔ <c>OrderId</c>);
    /// columns no member names are skipped. Values are converted to the member's type where a CLR conversion exists
    /// (<c>long</c> to <c>int?</c>, <c>DateTime</c> kept as is).
    /// </summary>
    public static Func<object?[], T> Binder<T>(IReadOnlyList<DuckLakeColumn> columns)
    {
        static string Key(string name) => name.Replace("_", "", StringComparison.Ordinal).ToUpperInvariant();
        var index = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 0; i < columns.Count; i++) index.TryAdd(Key(columns[i].Name), i);
        var ctor = typeof(T).GetConstructors()
            .Where(c => c.GetParameters().All(p => p.Name is not null && index.ContainsKey(Key(p.Name))))
            .OrderByDescending(c => c.GetParameters().Length).FirstOrDefault()
            ?? throw new ArgumentException($"{typeof(T).Name} has no public constructor whose parameters all name columns ({string.Join(", ", columns.Select(c => c.Name))})");
        var parameters = ctor.GetParameters().Select(p => (Index: index[Key(p.Name!)], Type: p.ParameterType)).ToArray();
        var used = parameters.Select(p => p.Index).ToHashSet();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is { IsPublic: true } && index.TryGetValue(Key(p.Name), out var i) && !used.Contains(i))
            .Select(p => (Index: index[Key(p.Name)], Property: p)).ToArray();

        // Compiled once: row => new T(Arg(row[i], ...)) { P = Arg(row[j]) }, where Arg converts the physical value to the
        // member's type (a direct cast when the CLR value already has it).
        var row = System.Linq.Expressions.Expression.Parameter(typeof(object?[]), "row");
        System.Linq.Expressions.Expression Arg(int i, Type target)
        {
            var convert = Converter(columns[i].Type);
            var name = columns[i].Name;
            Func<object?, object?> f = v => Convert(v is null ? null : convert(v), target, name);
            var call = System.Linq.Expressions.Expression.Invoke(System.Linq.Expressions.Expression.Constant(f),
                System.Linq.Expressions.Expression.ArrayIndex(row, System.Linq.Expressions.Expression.Constant(i)));
            return System.Linq.Expressions.Expression.Convert(call, target);
        }
        var create = System.Linq.Expressions.Expression.New(ctor, parameters.Select(p => Arg(p.Index, p.Type)));
        var body = properties.Length == 0 ? (System.Linq.Expressions.Expression)create
            : System.Linq.Expressions.Expression.MemberInit(create, properties.Select(p =>
                (System.Linq.Expressions.MemberBinding)System.Linq.Expressions.Expression.Bind(p.Property, Arg(p.Index, p.Property.PropertyType))));
        return System.Linq.Expressions.Expression.Lambda<Func<object?[], T>>(body, row).Compile();
    }

    private static object? Convert(object? value, Type target, string column)
    {
        var underlying = Nullable.GetUnderlyingType(target) ?? target;
        if (value is null)
            return underlying == target && target.IsValueType ? throw new InvalidCastException($"Column {column} is NULL; {target.Name} cannot hold it") : null;
        if (value.GetType() == underlying || target.IsInstanceOfType(value)) return value;
        try
        {
            return value switch
            {
                DateTimeOffset dto when underlying == typeof(DateTime) => dto.UtcDateTime,
                DateTime dt when underlying == typeof(DateTimeOffset) => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
                TimeSpan ts when underlying == typeof(TimeOnly) => TimeOnly.FromTimeSpan(ts),
                DateOnly d when underlying == typeof(DateTime) => d.ToDateTime(TimeOnly.MinValue),
                string s when underlying == typeof(Guid) => Guid.Parse(s),
                IConvertible when typeof(IConvertible).IsAssignableFrom(underlying) => System.Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture),
                _ => throw new InvalidCastException(),
            };
        }
        catch (Exception e) when (e is InvalidCastException or OverflowException or FormatException or ArgumentOutOfRangeException)
        {
            throw new InvalidCastException($"Column {column}: a {value.GetType().Name} ({value}) cannot be a {target.Name}", e);
        }
    }
}
