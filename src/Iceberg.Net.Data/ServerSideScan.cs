using Iceberg.Net.Catalog;
using Iceberg.Net.Expressions;
using Iceberg.Net.Types;

namespace Iceberg.Net.Data;

/// <summary>
/// Reads a scan the REST server planned (<see cref="RestCatalog.PlanTableScanAsync"/>): each task's data file with
/// exactly the delete files the server matched to it, then the request's filter on every row (the residual the
/// server sends is implied by it). Rows are in the table's current schema.
/// </summary>
public static class ServerSideScan
{
    /// <summary>Plan on the server and read the matching live rows.</summary>
    public static async Task<List<object?[]>> ReadAsync(RestCatalog catalog, Table table, Expression? filter = null, long? snapshotId = null, CancellationToken ct = default)
    {
        var plan = await catalog.PlanTableScanAsync(table, new ScanPlanRequest(filter, snapshotId), ct: ct);
        return await ReadAsync(table, plan, filter, ct);
    }

    /// <summary>Read the rows of an existing plan, keeping those that match <paramref name="filter"/>.</summary>
    public static async Task<List<object?[]>> ReadAsync(Table table, ServerScanPlan plan, Expression? filter = null, CancellationToken ct = default)
    {
        var schema = table.Schema;
        var keep = filter is null or AlwaysTrue ? null : RowEvaluator.Compile(Binder.Bind(schema, filter));
        var mapping = NameMapping.ForTable(table.Metadata.Properties);
        var deletes = DeleteIndex.ForResolvedDeletes(table);
        var rows = new List<object?[]>();
        foreach (var task in plan.Tasks)
        {
            var fileRows = await DataFiles.ReadAsync(table.IO, task.DataFile, schema, mapping, ct);
            var deleted = task.Deletes.Count == 0 ? null : await deletes.RowFilterAsync(task.DataFile, task.Deletes, schema, ct);
            for (var pos = 0; pos < fileRows.Count; pos++)
            {
                var row = fileRows[pos];
                if (deleted?.Invoke(pos, row) == true) continue;
                if (keep is not null && !keep(row)) continue;
                rows.Add(row);
            }
        }
        return rows;
    }
}
