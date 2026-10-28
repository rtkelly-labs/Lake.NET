using System.Data.Common;
using System.Globalization;
using Iceberg.Net.Catalog;

namespace Iceberg.Net.RestServer;

/// <summary>
/// <see cref="IServerStateStore"/> over the catalog's database, so server instances sharing it share idempotency keys
/// and plan IDs:
/// <code>
/// iceberg_rest_idempotency(catalog_name, principal, idempotency_key, fingerprint, created_ms, status, content_type, etag, body)
/// iceberg_rest_plans(catalog_name, plan_id, table_namespace, table_name, status, result, error, created_ms, finished_ms)
/// </code>
/// A claim is an <c>INSERT</c> that the primary key guards; <c>status</c> stays NULL until the response is final. Bodies
/// are stored base64-encoded, so any provider's text type holds them.
/// </summary>
public sealed partial class SqlCatalogStore
{
    private async Task InitializeServerStateAsync(DbConnection c, CancellationToken ct)
    {
        await ExecuteAsync(c, null, """
            CREATE TABLE IF NOT EXISTS iceberg_rest_idempotency (
              catalog_name VARCHAR(255) NOT NULL,
              principal VARCHAR(255) NOT NULL,
              idempotency_key VARCHAR(128) NOT NULL,
              fingerprint VARCHAR(64) NOT NULL,
              created_ms BIGINT NOT NULL,
              status INTEGER,
              content_type VARCHAR(255),
              etag VARCHAR(255),
              body TEXT,
              PRIMARY KEY (catalog_name, principal, idempotency_key))
            """, [], ct);
        await ExecuteAsync(c, null, """
            CREATE TABLE IF NOT EXISTS iceberg_rest_plans (
              catalog_name VARCHAR(255) NOT NULL,
              plan_id VARCHAR(64) NOT NULL,
              table_namespace VARCHAR(255) NOT NULL,
              table_name VARCHAR(255) NOT NULL,
              status VARCHAR(16) NOT NULL,
              result TEXT,
              error TEXT,
              created_ms BIGINT NOT NULL,
              finished_ms BIGINT,
              PRIMARY KEY (catalog_name, plan_id))
            """, [], ct);
    }

    private static long Ms(DateTimeOffset t) => t.ToUnixTimeMilliseconds();
    private static DateTimeOffset FromMs(string ms) => DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(ms, CultureInfo.InvariantCulture));

    public async Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore, CancellationToken ct = default)
        => await ClaimAsync(principal, key, fingerprint, now, expiredBefore, int.MaxValue, ct);

    public async Task<IdempotencyClaim?> ClaimAsync(string principal, string key, string fingerprint, DateTimeOffset now, DateTimeOffset expiredBefore,
        int maxClaimsPerPrincipal, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxClaimsPerPrincipal);
        await using var c = await OpenAsync(ct);
        (string, object?)[] id = [("@c", catalogName), ("@p", principal), ("@k", key)];
        await ExecuteAsync(c, null, "DELETE FROM iceberg_rest_idempotency WHERE catalog_name = @c AND principal = @p AND idempotency_key = @k AND created_ms < @e",
            [.. id, ("@e", Ms(expiredBefore))], ct);
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                var inserted = await ExecuteAsync(c, null, """
                    INSERT INTO iceberg_rest_idempotency (catalog_name, principal, idempotency_key, fingerprint, created_ms)
                    SELECT @c, @p, @k, @f, @t
                    WHERE (SELECT COUNT(*) FROM iceberg_rest_idempotency
                           WHERE catalog_name = @c AND principal = @p AND created_ms >= @e) < @limit
                    """, [.. id, ("@f", fingerprint), ("@t", Ms(now)), ("@e", Ms(expiredBefore)), ("@limit", maxClaimsPerPrincipal)], ct);
                if (inserted == 1) return null;
                // A conditional insert and its count are one database statement, so concurrent instances
                // cannot each reserve the last slot. Existing keys must remain replayable at capacity.
                var existing = await LoadClaimAsync(c, id, ct);
                return existing ?? throw new IdempotencyCapacityException();
            }
            catch (DbException) when (attempt < 3)
            {
                // The key is taken (primary key): read the claim. It may have been released meanwhile; then try again.
                var existing = await LoadClaimAsync(c, id, ct);
                if (existing is not null) return existing;
            }
        }
    }

    private static async Task<IdempotencyClaim?> LoadClaimAsync(DbConnection c, (string, object?)[] id, CancellationToken ct)
    {
        var rows = await QueryAsync(c, """
            SELECT fingerprint, created_ms, status, content_type, etag, body FROM iceberg_rest_idempotency
            WHERE catalog_name = @c AND principal = @p AND idempotency_key = @k
            """, id, ct);
        if (rows.Count == 0) return null;
        var r = rows[0];
        var response = r[2] is { } status
            ? new StoredResponse(int.Parse(status, CultureInfo.InvariantCulture), r[3], r[4], Convert.FromBase64String(r[5] ?? ""))
            : null;
        return new IdempotencyClaim(r[0]!, FromMs(r[1]!), response);
    }

    public async Task CompleteAsync(string principal, string key, DateTimeOffset claimed, StoredResponse response, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, null, """
            UPDATE iceberg_rest_idempotency SET status = @s, content_type = @ct, etag = @e, body = @b
            WHERE catalog_name = @c AND principal = @p AND idempotency_key = @k AND created_ms = @t
            """, [("@c", catalogName), ("@p", principal), ("@k", key), ("@t", Ms(claimed)), ("@s", response.Status), ("@ct", response.ContentType),
                ("@e", response.ETag), ("@b", Convert.ToBase64String(response.Body))], ct);
    }

    public async Task ReleaseAsync(string principal, string key, DateTimeOffset claimed, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, null, """
            DELETE FROM iceberg_rest_idempotency
            WHERE catalog_name = @c AND principal = @p AND idempotency_key = @k AND created_ms = @t AND status IS NULL
            """, [("@c", catalogName), ("@p", principal), ("@k", key), ("@t", Ms(claimed))], ct);
    }

    public async Task AddPlanAsync(PlanRecord plan, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, null, """
            INSERT INTO iceberg_rest_plans (catalog_name, plan_id, table_namespace, table_name, status, result, error, created_ms, finished_ms)
            VALUES (@c, @id, @n, @t, @s, @r, @e, @cr, @f)
            """, [("@c", catalogName), ("@id", plan.PlanId), ("@n", Ns(plan.Table.Namespace)), ("@t", plan.Table.Name), ("@s", plan.Status), ("@r", plan.Result),
                ("@e", plan.Error), ("@cr", Ms(plan.Created)), ("@f", plan.Finished is { } f ? Ms(f) : null)], ct);
    }

    public async Task<bool> FinishPlanAsync(string planId, string status, string? result, string? error, DateTimeOffset finished, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        return await ExecuteAsync(c, null, """
            UPDATE iceberg_rest_plans SET status = @s, result = @r, error = @e, finished_ms = @f
            WHERE catalog_name = @c AND plan_id = @id AND status = 'submitted'
            """, [("@c", catalogName), ("@id", planId), ("@s", status), ("@r", result), ("@e", error), ("@f", Ms(finished))], ct) == 1;
    }

    public async Task<PlanRecord?> LoadPlanAsync(string planId, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var rows = await QueryAsync(c, """
            SELECT table_namespace, table_name, status, result, error, created_ms, finished_ms FROM iceberg_rest_plans WHERE catalog_name = @c AND plan_id = @id
            """, [("@c", catalogName), ("@id", planId)], ct);
        if (rows.Count == 0) return null;
        var r = rows[0];
        return new PlanRecord(planId, new TableIdentifier(new Namespace(r[0]!.Split('.')), r[1]!), r[2]!, r[3], r[4], FromMs(r[5]!),
            r[6] is { } f ? FromMs(f) : null);
    }

    public async Task<bool> DeletePlanAsync(string planId, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        return await ExecuteAsync(c, null, "DELETE FROM iceberg_rest_plans WHERE catalog_name = @c AND plan_id = @id", [("@c", catalogName), ("@id", planId)], ct) > 0;
    }

    public async Task ExpireAsync(DateTimeOffset claimsBefore, DateTimeOffset plansFinishedBefore, DateTimeOffset plansSubmittedBefore, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await ExecuteAsync(c, null, "DELETE FROM iceberg_rest_idempotency WHERE catalog_name = @c AND created_ms < @t", [("@c", catalogName), ("@t", Ms(claimsBefore))], ct);
        await ExecuteAsync(c, null, """
            DELETE FROM iceberg_rest_plans
            WHERE catalog_name = @c AND ((finished_ms IS NOT NULL AND finished_ms < @f) OR (finished_ms IS NULL AND created_ms < @s))
            """, [("@c", catalogName), ("@f", Ms(plansFinishedBefore)), ("@s", Ms(plansSubmittedBefore))], ct);
    }

    public async Task<(int Claims, int Plans)> CountAsync(CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        var claims = await QueryAsync(c, "SELECT COUNT(*) FROM iceberg_rest_idempotency WHERE catalog_name = @c", [("@c", catalogName)], ct);
        var plans = await QueryAsync(c, "SELECT COUNT(*) FROM iceberg_rest_plans WHERE catalog_name = @c", [("@c", catalogName)], ct);
        return (int.Parse(claims[0][0]!, CultureInfo.InvariantCulture), int.Parse(plans[0][0]!, CultureInfo.InvariantCulture));
    }
}
