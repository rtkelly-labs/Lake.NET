using Amazon;
using Amazon.EMRServerless;
using Amazon.Glue;
using Amazon.Runtime;
using Emr = Amazon.EMRServerless.Model;
using Glue = Amazon.Glue.Model;

namespace Iceberg.Net.Ecosystem.Tests;

/// <summary>Records requests and answers them with the handler registered for the request type.</summary>
public sealed class FakeCalls
{
    private readonly Dictionary<Type, Func<object, object>> _handlers = [];
    public List<object> Requests { get; } = [];

    public FakeCalls On<TRequest, TResponse>(Func<TRequest, TResponse> handler) where TRequest : class
    {
        _handlers[typeof(TRequest)] = r => handler((TRequest)r)!;
        return this;
    }

    public IEnumerable<T> Sent<T>() => Requests.OfType<T>();

    public Task<TResponse> Answer<TResponse>(object request)
    {
        Requests.Add(request);
        if (!_handlers.TryGetValue(request.GetType(), out var handler)) throw new NotSupportedException($"{request.GetType().Name} is not faked");
        try { return Task.FromResult((TResponse)handler(request)); }
        catch (Exception e) { return Task.FromException<TResponse>(e); }
    }
}

/// <summary>The SDK clients with the calls the runners make overridden (nothing reaches AWS).</summary>
public sealed class FakeEmrServerless() : AmazonEMRServerlessClient(new BasicAWSCredentials("test", "test"), RegionEndpoint.EUWest1)
{
    public FakeCalls Calls { get; } = new();
    public override Task<Emr.StartJobRunResponse> StartJobRunAsync(Emr.StartJobRunRequest request, CancellationToken ct = default) => Calls.Answer<Emr.StartJobRunResponse>(request);
    public override Task<Emr.GetJobRunResponse> GetJobRunAsync(Emr.GetJobRunRequest request, CancellationToken ct = default) => Calls.Answer<Emr.GetJobRunResponse>(request);
    public override Task<Emr.CancelJobRunResponse> CancelJobRunAsync(Emr.CancelJobRunRequest request, CancellationToken ct = default) => Calls.Answer<Emr.CancelJobRunResponse>(request);
}

public sealed class FakeGlue() : AmazonGlueClient(new BasicAWSCredentials("test", "test"), RegionEndpoint.EUWest1)
{
    public FakeCalls Calls { get; } = new();
    public override Task<Glue.GetJobResponse> GetJobAsync(Glue.GetJobRequest request, CancellationToken ct = default) => Calls.Answer<Glue.GetJobResponse>(request);
    public override Task<Glue.CreateJobResponse> CreateJobAsync(Glue.CreateJobRequest request, CancellationToken ct = default) => Calls.Answer<Glue.CreateJobResponse>(request);
    public override Task<Glue.UpdateJobResponse> UpdateJobAsync(Glue.UpdateJobRequest request, CancellationToken ct = default) => Calls.Answer<Glue.UpdateJobResponse>(request);
    public override Task<Glue.StartJobRunResponse> StartJobRunAsync(Glue.StartJobRunRequest request, CancellationToken ct = default) => Calls.Answer<Glue.StartJobRunResponse>(request);
    public override Task<Glue.GetJobRunResponse> GetJobRunAsync(Glue.GetJobRunRequest request, CancellationToken ct = default) => Calls.Answer<Glue.GetJobRunResponse>(request);
}
