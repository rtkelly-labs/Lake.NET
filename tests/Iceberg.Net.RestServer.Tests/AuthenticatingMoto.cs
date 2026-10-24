using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Xml.Linq;
using Amazon.S3;
using Iceberg.Net.IO;

namespace Iceberg.Net.RestServer.Tests;

/// <summary>
/// A throwaway moto (one-off <c>docker run</c> on a free port) that enforces authentication: after three bootstrap
/// calls (an IAM user, its access key, an allow-all policy) every request must carry a valid SigV4 signature for that
/// user's key, so a wrong or missing signature is <c>SignatureDoesNotMatch</c> / 403. The shared emulator does not check
/// signatures; this one proves that signatures a remote signer issues are valid.
/// </summary>
public sealed class AuthenticatingMoto : IAsyncDisposable
{
    public const string Image = "motoserver/moto:5.2.4";
    public const string Region = "us-east-1";

    private readonly string _container;
    public string Endpoint { get; }
    public string AccessKeyId { get; }
    public string SecretAccessKey { get; }
    public string Bucket { get; }

    private AuthenticatingMoto(string container, string endpoint, string accessKeyId, string secretAccessKey, string bucket)
    {
        _container = container;
        Endpoint = endpoint;
        AccessKeyId = accessKeyId;
        SecretAccessKey = secretAccessKey;
        Bucket = bucket;
    }

    /// <summary>Why it cannot run here (no docker, no image), or null.</summary>
    public static string? SkipReason
    {
        get
        {
            try
            {
                var (code, _) = Run("docker", "image", "inspect", Image).GetAwaiter().GetResult();
                return code == 0 ? null : $"{Image} is not present (docker pull {Image})";
            }
            catch (Exception e) when (e is System.ComponentModel.Win32Exception or InvalidOperationException)
            {
                return "docker is not available";
            }
        }
    }

    public static async Task<AuthenticatingMoto> StartAsync()
    {
        var port = FreePort();
        var name = $"iceberg-moto-auth-{Guid.NewGuid():N}"[..32];
        var (code, output) = await Run("docker", "run", "-d", "--rm", "--name", name, "-p", $"127.0.0.1:{port}:5000", "-e", "INITIAL_NO_AUTH_ACTION_COUNT=3", Image);
        if (code != 0) throw new InvalidOperationException($"docker run {Image} failed: {output}");
        var endpoint = $"http://127.0.0.1:{port}";
        using var http = new HttpClient { BaseAddress = new Uri(endpoint) };
        for (var i = 0; ; i++)
        {
            try
            {
                using var ping = await http.GetAsync("/moto-api/");
                break;
            }
            catch (HttpRequestException) when (i < 100)
            {
                await Task.Delay(100);
            }
        }

        // Moto routes by the signature's credential scope, so the bootstrap calls are signed for IAM (any key will do yet).
        using var iam = new HttpClient(new Iceberg.Net.Catalog.SigV4Handler(new Amazon.Runtime.BasicAWSCredentials("bootstrap", "bootstrap"), Region, "iam"))
        {
            BaseAddress = new Uri(endpoint),
        };
        async Task<XDocument> IamAsync(params (string, string)[] form)
        {
            var fields = new Dictionary<string, string> { ["Version"] = "2010-05-08" };
            foreach (var (k, v) in form) fields[k] = v;
            using var resp = await iam.PostAsync("/", new FormUrlEncodedContent(fields));
            var text = await resp.Content.ReadAsStringAsync();
            if (!resp.IsSuccessStatusCode) throw new InvalidOperationException($"IAM {fields["Action"]} failed: {text}");
            return XDocument.Parse(text);
        }
        await IamAsync(("Action", "CreateUser"), ("UserName", "iceberg-server"));
        var key = await IamAsync(("Action", "CreateAccessKey"), ("UserName", "iceberg-server"));
        string Field(string n) => key.Descendants().First(e => e.Name.LocalName == n).Value;
        await IamAsync(("Action", "PutUserPolicy"), ("UserName", "iceberg-server"), ("PolicyName", "all"),
            ("PolicyDocument", """{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"""));

        var moto = new AuthenticatingMoto(name, endpoint, Field("AccessKeyId"), Field("SecretAccessKey"), "signed-warehouse");
        using var io = new S3FileIO(new S3FileIOOptions
        {
            Endpoint = endpoint, PathStyleAccess = true, Region = Region, AccessKeyId = moto.AccessKeyId, SecretAccessKey = moto.SecretAccessKey,
        });
        await io.Client.PutBucketAsync(moto.Bucket);
        // Authentication is on now: a request signed with the wrong secret is refused.
        using var forged = new S3FileIO(new S3FileIOOptions
        {
            Endpoint = endpoint, PathStyleAccess = true, Region = Region, AccessKeyId = moto.AccessKeyId, SecretAccessKey = "not-the-secret", MaxRetries = 0,
        });
        var refused = await Assert.ThrowsAsync<AmazonS3Exception>(() => forged.Client.GetObjectMetadataAsync(moto.Bucket, "probe"));
        Assert.Equal(HttpStatusCode.Forbidden, refused.StatusCode);
        return moto;
    }

    private static int FreePort()
    {
        using var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        return ((IPEndPoint)l.LocalEndpoint).Port;
    }

    private static async Task<(int Code, string Output)> Run(string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi) ?? throw new InvalidOperationException($"{file} did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(2));
        return (p.ExitCode, await stdout + await stderr);
    }

    public async ValueTask DisposeAsync() => await Run("docker", "rm", "-f", _container);
}
