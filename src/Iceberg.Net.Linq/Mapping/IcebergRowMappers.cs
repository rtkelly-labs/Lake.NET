using System.Reflection;

namespace Iceberg.Net.Linq;

/// <summary>
/// Resolves the row mapper for a type: prefers a compile-time <see cref="IIcebergMappable{T}"/> implementation
/// when available, otherwise defaults to <see cref="ReflectionRowMapper{T}"/>.
/// </summary>
public static class IcebergRowMappers
{
    public static IIcebergRowMapper<T> For<T>(IcebergMapperOptions? options = null)
    {
        return MappableResolver<T>.CreateMapper(options);
    }

    private static class MappableResolver<T>
    {
        private static readonly Func<IcebergMapperOptions?, IIcebergRowMapper<T>> Invoker = BuildInvoker();

        public static IIcebergRowMapper<T> CreateMapper(IcebergMapperOptions? options) => Invoker(options);

        private static Func<IcebergMapperOptions?, IIcebergRowMapper<T>> BuildInvoker()
        {
            if (typeof(IIcebergMappable<T>).IsAssignableFrom(typeof(T)))
            {
                var method = typeof(T).GetMethod(nameof(IIcebergMappable<T>.CreateRowMapper), BindingFlags.Public | BindingFlags.Static);
                if (method is not null)
                {
                    return (Func<IcebergMapperOptions?, IIcebergRowMapper<T>>)method.CreateDelegate(typeof(Func<IcebergMapperOptions?, IIcebergRowMapper<T>>));
                }
            }
            return opts => new ReflectionRowMapper<T>(opts);
        }
    }
}
