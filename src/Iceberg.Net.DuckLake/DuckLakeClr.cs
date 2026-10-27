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
    public static object? ToClr(DuckLakeType type, object? value)
    {
        if (value is null) return null;
        switch (type)
        {
            case DuckLakeList l:
                return ((IEnumerable)value).Cast<object?>().Select(x => ToClr(l.Element, x)).ToList();
            case DuckLakeMap m:
                var map = new Dictionary<object, object?>();
                foreach (DictionaryEntry e in (IDictionary)value) map[ToClr(m.Key, e.Key)!] = ToClr(m.Value, e.Value);
                return map;
            case DuckLakeStruct s:
                var fields = (IList)value;
                return s.Fields.Select((f, i) => (f.Name, Value: ToClr(f.Type, fields[i]))).ToDictionary(x => x.Name, x => x.Value, StringComparer.Ordinal);
        }
        var p = (DuckLakePrimitive)type;
        return p.Name switch
        {
            "int8" => checked((sbyte)(int)value),
            "int16" => checked((short)(int)value),
            "uint8" => checked((byte)(int)value),
            "uint16" => checked((ushort)(int)value),
            "uint32" => checked((uint)(long)value),
            "uint64" => checked((ulong)((IcebergDecimal)value).Unscaled),
            "date" => IcebergTime.DateFromDays((int)value),
            "time" => TimeSpan.FromTicks(checked((long)value * 10)),
            "timestamp" or "timestamp_s" or "timestamp_ms" => DateTime.SpecifyKind(IcebergTime.Epoch.AddTicks(checked((long)value * 10)), DateTimeKind.Unspecified),
            "timestamptz" => new DateTimeOffset(IcebergTime.Epoch.AddTicks(checked((long)value * 10)), TimeSpan.Zero),
            "timestamp_ns" => DateTime.SpecifyKind(IcebergTime.Epoch.AddTicks(IcebergTime.FloorDiv((long)value, 100)), DateTimeKind.Unspecified),
            _ when value is IcebergDecimal d => TryDecimal(d),
            _ => value,
        };
    }

    private static object TryDecimal(IcebergDecimal d)
    {
        try { return d.ToDecimal(); }
        catch (OverflowException) { return d; }
    }

    /// <summary>A row's values as CLR values, by column.</summary>
    public static object?[] ToClr(IReadOnlyList<DuckLakeColumn> columns, object?[] row)
    {
        var result = new object?[columns.Count];
        for (var i = 0; i < columns.Count; i++) result[i] = ToClr(columns[i].Type, row[i]);
        return result;
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
        var parameters = ctor.GetParameters().Select(p => (Index: index[Key(p.Name!)], p.ParameterType)).ToArray();
        var used = parameters.Select(p => p.Index).ToHashSet();
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.SetMethod is { IsPublic: true } && index.TryGetValue(Key(p.Name), out var i) && !used.Contains(i))
            .Select(p => (Index: index[Key(p.Name)], Property: p)).ToArray();
        var types = columns.Select(c => c.Type).ToArray();
        return row =>
        {
            var args = parameters.Select(p => Convert(ToClr(types[p.Index], row[p.Index]), p.ParameterType, columns[p.Index].Name)).ToArray();
            var result = (T)ctor.Invoke(args);
            foreach (var (i, property) in properties) property.SetValue(result, Convert(ToClr(types[i], row[i]), property.PropertyType, columns[i].Name));
            return result;
        };
    }

    private static object? Convert(object? value, Type target, string column)
    {
        var underlying = Nullable.GetUnderlyingType(target) ?? target;
        if (value is null)
            return underlying == target && target.IsValueType ? throw new InvalidCastException($"Column {column} is NULL; {target.Name} cannot hold it") : null;
        if (target.IsInstanceOfType(value)) return value;
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
