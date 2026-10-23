using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace Iceberg.Net.Linq.Query;

/// <summary>
/// Runs streaming operators (Where, Select, Skip, Take) over the async scan without buffering, so <c>Take(10)</c> stops
/// the scan after ten rows. Anything else goes through LINQ to Objects after the scan is buffered.
/// </summary>
internal static class AsyncPipeline
{
    public static bool CanStream(IReadOnlyList<MethodCallExpression> ops) => ops.All(o => o.Method.Name switch
    {
        "Where" or "Select" => Lambda(o.Arguments[1]) is { Parameters.Count: 1 },
        "Skip" or "Take" => o.Arguments[1] is ConstantExpression { Value: int },
        _ => false,
    });

    public static async IAsyncEnumerable<TElement> Stream<T, TElement>(IAsyncEnumerable<T> source, IReadOnlyList<MethodCallExpression> ops,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var stages = ops.Select(Compile).ToArray();
        var counters = new int[stages.Length];
        await using var e = source.GetAsyncEnumerator(ct);
        while (await e.MoveNextAsync())
        {
            object? value = e.Current;
            var keep = true;
            for (var i = 0; i < stages.Length && keep; i++)
            {
                var s = stages[i];
                switch (s.Kind)
                {
                    case Kind.Where:
                        keep = s.Predicate!(value);
                        break;
                    case Kind.Select:
                        value = s.Selector!(value);
                        break;
                    case Kind.Skip:
                        keep = counters[i]++ >= s.Count;
                        break;
                    case Kind.Take:
                        if (counters[i] >= s.Count) yield break; // every later row is dropped here too: stop the scan
                        counters[i]++;
                        break;
                }
            }
            if (keep) yield return (TElement)value!;
        }
    }

    private enum Kind { Where, Select, Skip, Take }

    private sealed record Stage(Kind Kind, Func<object?, bool>? Predicate = null, Func<object?, object?>? Selector = null, int Count = 0);

    private static Stage Compile(MethodCallExpression op)
    {
        switch (op.Method.Name)
        {
            case "Skip": return new(Kind.Skip, Count: (int)((ConstantExpression)op.Arguments[1]).Value!);
            case "Take": return new(Kind.Take, Count: (int)((ConstantExpression)op.Arguments[1]).Value!);
        }
        var lambda = Lambda(op.Arguments[1])!;
        var p = Expression.Parameter(typeof(object), "o");
        var body = Expression.Invoke(lambda, Expression.Convert(p, lambda.Parameters[0].Type));
        return op.Method.Name == "Where"
            ? new(Kind.Where, Predicate: Expression.Lambda<Func<object?, bool>>(body, p).Compile())
            : new(Kind.Select, Selector: Expression.Lambda<Func<object?, object?>>(Expression.Convert(body, typeof(object)), p).Compile());
    }

    private static LambdaExpression? Lambda(Expression e) => e switch
    {
        UnaryExpression { NodeType: ExpressionType.Quote } q => q.Operand as LambdaExpression,
        LambdaExpression l => l,
        _ => null,
    };
}

/// <summary>Synchronous enumeration over the async scan (blocking, with no synchronization context captured).</summary>
internal static class Blocking
{
    public static IEnumerable<T> Enumerate<T>(IAsyncEnumerable<T> source)
    {
        var e = source.GetAsyncEnumerator();
        try
        {
            while (Run(() => e.MoveNextAsync().AsTask())) yield return e.Current;
        }
        finally
        {
            Run(async () => { await e.DisposeAsync(); return true; });
        }
    }

    public static TR Run<TR>(Func<Task<TR>> start)
    {
        var context = SynchronizationContext.Current;
        try
        {
            SynchronizationContext.SetSynchronizationContext(null);
            return start().GetAwaiter().GetResult();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }
    }
}
