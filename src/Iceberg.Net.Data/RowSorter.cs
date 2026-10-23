using Iceberg.Net.Expressions;
using Iceberg.Net.Partitioning;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Orders rows by a table sort order (spec "Sorting"): each field's transform is applied to its source column, then
/// compared ascending or descending with nulls first or last; floating point sorts
/// <c>-NaN &lt; -∞ &lt; … &lt; -0 &lt; 0 &lt; … &lt; ∞ &lt; NaN</c> (NaN treated as greater than every number).
/// </summary>
public sealed class RowSorter : IComparer<object?[]>
{
    private readonly (int Column, IcebergType SourceType, SortField Field)[] _keys;

    public RowSorter(Schema schema, SortOrder order)
    {
        var columns = schema.Columns.ToList();
        _keys = order.Fields.Select(f =>
        {
            var column = columns.FindIndex(c => c.Id == f.SourceId);
            if (column < 0) throw new NotSupportedException($"Sort field {f.SourceId} is not a top-level column");
            return (column, columns[column].Type, f);
        }).ToArray();
    }

    public int Compare(object?[]? a, object?[]? b)
    {
        foreach (var (column, type, field) in _keys)
        {
            var x = field.Transform.Apply(type, a![column]);
            var y = field.Transform.Apply(type, b![column]);
            int c;
            if (x is null || y is null)
            {
                if (x is null && y is null) continue;
                // Null placement is independent of direction.
                c = (x is null ? -1 : 1) * (field.NullOrder == NullOrder.NullsFirst ? 1 : -1);
                return c;
            }
            c = ValueComparer.IsNaN(x) || ValueComparer.IsNaN(y)
                ? (ValueComparer.IsNaN(x) ? 1 : 0) - (ValueComparer.IsNaN(y) ? 1 : 0)
                : ValueComparer.Compare(x, y);
            if (c != 0) return field.Direction == SortDirection.Asc ? c : -c;
        }
        return 0;
    }
}
