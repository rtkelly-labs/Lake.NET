using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;

namespace Iceberg.Net.RestServer;

/// <summary>
/// <c>Idempotency-Key</c> deduplication (spec 1.11+): the first mutating request with a key runs; its final response
/// (2xx, or a 4xx, which is deterministic) is kept for the advertised lifetime and replayed to every later request with
/// the same key from the same principal, without running it again. A 5xx is not kept, so a retry runs again. A
/// duplicate that arrives while the first is still running waits for it. Reusing a key for a different request
/// (method, path or body) is a 422. Claims and responses live in the <see cref="IServerStateStore"/> (the catalog's
/// database), so every server instance over one database honours them. A claim whose request never finished (its
/// server died) is taken over after <see cref="IdempotencyOptions.PendingTimeout"/>, like a 5xx.
/// </summary>
public sealed class IdempotencyStore(IdempotencyOptions options, IServerStateStore state, Func<DateTimeOffset>? clock = null)
{
    private readonly Func<DateTimeOffset> _clock = clock ?? (() => DateTimeOffset.UtcNow);
    private readonly int _capacity = options.MaxClaimsPerPrincipal > 0 ? options.MaxClaimsPerPrincipal
        : throw new ArgumentOutOfRangeException(nameof(options), "MaxClaimsPerPrincipal must be positive");
    private long _lastSweepTicks;

    public const string Header = "Idempotency-Key";

    /// <summary>Kept claims and responses (for tests and metrics).</summary>
    public async Task<int> CountAsync() => (await state.CountAsync()).Claims;

    private TimeSpan Retention => options.Lifetime + TimeSpan.FromMinutes(1);

    /// <summary>Whether this request takes part: a mutation with the header, other than planning and signing (which change nothing).</summary>
    public static bool Applies(HttpRequest req) =>
        (HttpMethods.IsPost(req.Method) || HttpMethods.IsDelete(req.Method) || HttpMethods.IsPut(req.Method))
        && req.Headers.ContainsKey(Header)
        && !req.Path.StartsWithSegments("/v1/oauth")
        && !(req.Path.Value is { } p && (p.EndsWith("/plan", StringComparison.Ordinal) || p.EndsWith("/tasks", StringComparison.Ordinal) || p.EndsWith("/sign", StringComparison.Ordinal) || p.Contains("/plan/", StringComparison.Ordinal)));

    public async Task InvokeAsync(HttpContext http, Func<Task> next)
    {
        var key = http.Request.Headers[Header].ToString().Trim();
        if (key.Length is 0 or > 128)
        {
            await ErrorModel.WriteAsync(http, 400, "BadRequestException", "Idempotency-Key must be 1 to 128 characters (a UUIDv7)");
            return;
        }
        http.Request.EnableBuffering();
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(System.Text.Encoding.UTF8.GetBytes($"{http.Request.Method} {http.Request.Path}{http.Request.QueryString}\n"));
        var buffer = new byte[81920];
        int read;
        while ((read = await http.Request.Body.ReadAsync(buffer, http.RequestAborted)) > 0) hash.AppendData(buffer, 0, read);
        http.Request.Body.Position = 0;
        var fingerprint = Convert.ToHexString(hash.GetHashAndReset());
        var principal = Principals.Name(http.User);
        await SweepAsync();

        var delay = TimeSpan.FromMilliseconds(10);
        while (true)
        {
            var now = Now();
            IdempotencyClaim? existing;
            try
            {
                existing = await state.ClaimAsync(principal, key, fingerprint, now, now - Retention, _capacity, http.RequestAborted);
            }
            catch (IdempotencyCapacityException)
            {
                http.Response.Headers.RetryAfter = "60";
                await ErrorModel.WriteAsync(http, 429, "TooManyRequestsException", "Idempotency capacity is full; retry after retained keys expire");
                return;
            }
            if (existing is null)
            {
                await RunAsync(http, next, principal, key, now);
                return;
            }
            if (existing.Fingerprint != fingerprint)
            {
                await ErrorModel.WriteAsync(http, 422, "UnprocessableEntityException", "This Idempotency-Key was already used for a different request");
                return;
            }
            if (existing.Response is { } stored)
            {
                http.Response.Headers["Idempotent-Replayed"] = "true";
                await WriteAsync(http, stored);
                return;
            }
            if (now - existing.Claimed > options.PendingTimeout)
            {
                // Its request never finished (the server running it went away): not final, so this one runs.
                await state.ReleaseAsync(principal, key, existing.Claimed, http.RequestAborted);
                continue;
            }
            // Still running, here or on another instance: wait for its outcome.
            await Task.Delay(delay, http.RequestAborted);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, TimeSpan.FromMilliseconds(250).Ticks));
        }
    }

    // Stored with millisecond precision, so a claim's time compares equal after a round trip through SQL.
    private DateTimeOffset Now() => DateTimeOffset.FromUnixTimeMilliseconds(_clock().ToUnixTimeMilliseconds());

    private async Task RunAsync(HttpContext http, Func<Task> next, string principal, string key, DateTimeOffset claimed)
    {
        var original = http.Response.Body;
        using var capture = new MemoryStream();
        http.Response.Body = capture;
        StoredResponse? stored = null;
        try
        {
            await next();
            var status = http.Response.StatusCode;
            if (status < 500 && capture.Length <= MaxStoredResponseBytes)
                stored = new StoredResponse(status, http.Response.ContentType, http.Response.Headers.ETag.ToString() is { Length: > 0 } e ? e : null,
                    WithoutCredentials(capture.ToArray(), http.Response.ContentType));
        }
        finally
        {
            http.Response.Body = original;
            if (stored is null) await state.ReleaseAsync(principal, key, claimed, CancellationToken.None);
            else await state.CompleteAsync(principal, key, claimed, stored, CancellationToken.None);
            capture.Position = 0;
            await capture.CopyToAsync(original);
        }
    }

    /// <summary>A larger response is sent but not kept: the retention store holds small documents.</summary>
    public const int MaxStoredResponseBytes = 1024 * 1024;

    private static readonly string[] CredentialKeys = ["s3.access-key-id", "s3.secret-access-key", "s3.session-token", "gcs.oauth2.token", "adls.sas-token"];

    /// <summary>
    /// The response as it is kept for replay: without vended storage credentials, which must not sit in the retention
    /// store. A replayed create or register carries no credentials; the client gets fresh ones from <c>loadCredentials</c>.
    /// </summary>
    private static byte[] WithoutCredentials(byte[] body, string? contentType)
    {
        if (body.Length == 0 || contentType is null || !contentType.StartsWith("application/json", StringComparison.OrdinalIgnoreCase)) return body;
        System.Text.Json.Nodes.JsonNode? node;
        try { node = System.Text.Json.Nodes.JsonNode.Parse(body); }
        catch (System.Text.Json.JsonException) { return body; }
        if (node is not System.Text.Json.Nodes.JsonObject json) return body;
        var changed = json.Remove("storage-credentials");
        if (json["config"] is System.Text.Json.Nodes.JsonObject config)
            foreach (var key in config.Select(kv => kv.Key).Where(k => CredentialKeys.Any(c => k.StartsWith(c, StringComparison.Ordinal))).ToList())
                changed |= config.Remove(key);
        return changed ? System.Text.Encoding.UTF8.GetBytes(json.ToJsonString()) : body;
    }

    private static async Task WriteAsync(HttpContext http, StoredResponse stored)
    {
        http.Response.StatusCode = stored.Status;
        if (stored.ContentType is not null) http.Response.ContentType = stored.ContentType;
        if (stored.ETag is not null) http.Response.Headers.ETag = stored.ETag;
        if (stored.Body.Length > 0) await http.Response.Body.WriteAsync(stored.Body);
    }

    /// <summary>At most once a minute per instance: drop claims older than the retention.</summary>
    private async Task SweepAsync()
    {
        var now = _clock();
        var last = Interlocked.Read(ref _lastSweepTicks);
        if (now.UtcTicks - last < TimeSpan.TicksPerMinute || Interlocked.CompareExchange(ref _lastSweepTicks, now.UtcTicks, last) != last) return;
        await state.ExpireAsync(now - Retention, DateTimeOffset.MinValue, DateTimeOffset.MinValue);
    }
}
