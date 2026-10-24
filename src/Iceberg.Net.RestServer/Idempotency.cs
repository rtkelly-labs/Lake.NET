using System.Security.Cryptography;
using Microsoft.AspNetCore.Http;

namespace Iceberg.Net.RestServer;

/// <summary>
/// <c>Idempotency-Key</c> deduplication (spec 1.11+): the first mutating request with a key runs; its final response
/// (2xx, or a 4xx, which is deterministic) is kept for the advertised lifetime and replayed to every later request with
/// the same key from the same principal, without running it again. A 5xx is not kept, so a retry runs again. A
/// duplicate that arrives while the first is still running waits for it. Reusing a key for a different request
/// (method, path or body) is a 422. Keys live in this process's memory, so several server instances behind one
/// database do not share them.
/// </summary>
public sealed class IdempotencyStore(IdempotencyOptions options, Func<DateTimeOffset>? clock = null)
{
    private sealed record Stored(int Status, string? ContentType, string? ETag, byte[] Body);

    private sealed class Entry(string fingerprint, DateTimeOffset created)
    {
        public string Fingerprint { get; } = fingerprint;
        public DateTimeOffset Created { get; } = created;
        public TaskCompletionSource<Stored?> Done { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private readonly Dictionary<(string Principal, string Key), Entry> _entries = [];
    private readonly Func<DateTimeOffset> _clock = clock ?? (() => DateTimeOffset.UtcNow);
    private DateTimeOffset _lastSweep = DateTimeOffset.MinValue;

    public const string Header = "Idempotency-Key";

    /// <summary>Kept responses (for tests and metrics).</summary>
    public int Count
    {
        get { lock (_entries) return _entries.Count; }
    }

    private TimeSpan Retention => options.Lifetime + TimeSpan.FromMinutes(1);

    /// <summary>Whether this request takes part: a mutation with the header, other than planning (which changes nothing).</summary>
    public static bool Applies(HttpRequest req) =>
        (HttpMethods.IsPost(req.Method) || HttpMethods.IsDelete(req.Method) || HttpMethods.IsPut(req.Method))
        && req.Headers.ContainsKey(Header)
        && !req.Path.StartsWithSegments("/v1/oauth")
        && !(req.Path.Value is { } p && (p.EndsWith("/plan", StringComparison.Ordinal) || p.EndsWith("/tasks", StringComparison.Ordinal) || p.Contains("/plan/", StringComparison.Ordinal)));

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
        var id = (Principals.Name(http.User), key);

        while (true)
        {
            Entry mine;
            Entry? existing;
            lock (_entries)
            {
                Sweep();
                if (_entries.TryGetValue(id, out existing) && _clock() - existing.Created > Retention)
                {
                    _entries.Remove(id);
                    existing = null;
                }
                mine = existing ?? (_entries[id] = new Entry(fingerprint, _clock()));
            }

            if (existing is not null)
            {
                if (existing.Fingerprint != fingerprint)
                {
                    await ErrorModel.WriteAsync(http, 422, "UnprocessableEntityException", "This Idempotency-Key was already used for a different request");
                    return;
                }
                var stored = await existing.Done.Task.WaitAsync(http.RequestAborted);
                if (stored is null) continue; // the first attempt was not finalized (5xx): run this one
                http.Response.Headers["Idempotent-Replayed"] = "true";
                await WriteAsync(http, stored);
                return;
            }

            await RunAsync(http, next, id, mine);
            return;
        }
    }

    private async Task RunAsync(HttpContext http, Func<Task> next, (string, string) id, Entry entry)
    {
        var original = http.Response.Body;
        using var capture = new MemoryStream();
        http.Response.Body = capture;
        Stored? stored = null;
        try
        {
            await next();
            var status = http.Response.StatusCode;
            if (status < 500)
                stored = new Stored(status, http.Response.ContentType, http.Response.Headers.ETag.ToString() is { Length: > 0 } e ? e : null, capture.ToArray());
        }
        finally
        {
            http.Response.Body = original;
            if (stored is null)
            {
                lock (_entries) _entries.Remove(id);
            }
            entry.Done.TrySetResult(stored);
            capture.Position = 0;
            await capture.CopyToAsync(original);
        }
    }

    private static async Task WriteAsync(HttpContext http, Stored stored)
    {
        http.Response.StatusCode = stored.Status;
        if (stored.ContentType is not null) http.Response.ContentType = stored.ContentType;
        if (stored.ETag is not null) http.Response.Headers.ETag = stored.ETag;
        if (stored.Body.Length > 0) await http.Response.Body.WriteAsync(stored.Body);
    }

    private void Sweep()
    {
        var now = _clock();
        if (now - _lastSweep < TimeSpan.FromMinutes(1)) return;
        _lastSweep = now;
        foreach (var (k, _) in _entries.Where(kv => kv.Value.Done.Task.IsCompleted && now - kv.Value.Created > Retention).ToList())
            _entries.Remove(k);
    }
}
