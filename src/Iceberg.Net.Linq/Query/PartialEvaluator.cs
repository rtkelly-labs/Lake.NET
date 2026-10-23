using System.Linq.Expressions;

namespace Iceberg.Net.Linq.Query;

/// <summary>
/// Replaces every sub-expression that does not depend on a lambda parameter (captured variables, <c>DateTime.Today</c>,
/// <c>new[] { 1, 2 }</c>, …) with its value, so translation only sees members, operators and constants.
/// </summary>
internal sealed class PartialEvaluator : ExpressionVisitor
{
    private readonly HashSet<Expression> _dependent;

    private PartialEvaluator(HashSet<Expression> dependent) => _dependent = dependent;

    public static Expression Evaluate(Expression e)
    {
        var finder = new DependencyFinder();
        finder.Visit(e);
        return new PartialEvaluator(finder.Dependent).Visit(e)!;
    }

    public override Expression? Visit(Expression? node)
    {
        if (node is null) return null;
        if (_dependent.Contains(node) || node.NodeType is ExpressionType.Constant or ExpressionType.Lambda or ExpressionType.Quote or ExpressionType.Parameter)
            return base.Visit(node);
        if (typeof(IQueryable).IsAssignableFrom(node.Type)) return base.Visit(node);
        try
        {
            var value = Expression.Lambda(node).Compile(preferInterpretation: true).DynamicInvoke();
            return Expression.Constant(value, node.Type);
        }
        catch (Exception)
        {
            // Evaluation failed (e.g. a null captured reference): leave the node; it will not translate.
            return base.Visit(node);
        }
    }

    /// <summary>Marks every node that (transitively) refers to a parameter, so it cannot be evaluated up front.</summary>
    private sealed class DependencyFinder : ExpressionVisitor
    {
        public HashSet<Expression> Dependent { get; } = [];
        private bool _found;

        public override Expression? Visit(Expression? node)
        {
            if (node is null) return null;
            var outer = _found;
            _found = false;
            base.Visit(node);
            if (node.NodeType == ExpressionType.Parameter) _found = true;
            if (_found) Dependent.Add(node);
            _found |= outer;
            return node;
        }
    }
}
