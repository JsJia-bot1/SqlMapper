using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;

namespace SqlMapper.Core
{
    public sealed class Identity
    {
        public string? Sql { get; set; }

        public Type? ResultType { get; set; }

        public Type? ParametersType { get; set; }

        public Identity(string? sql, Type? resultType, Type? parametersType)
        {
            Sql = sql;
            ResultType = resultType;
            ParametersType = parametersType;
        }

        public bool Equals(Identity other)
        {
            if (other is null)
            {
                return false;
            }
            return Sql == other.Sql
                && ResultType == other.ResultType
                && ParametersType == other.ParametersType;
        }

        public override bool Equals(object obj) => Equals(obj as Identity);

        public override int GetHashCode() => (Sql, ResultType, ParametersType).GetHashCode();
    }

    public class Cache
    {
        public Func<DbDataReader, object>? ResultDeserializer { get; set; }

        public Action<IDbCommand, object>? ParametersGenerator { get; set; }
    }

    public static partial class SqlExtension
    {
        private static readonly ConcurrentDictionary<Identity, Cache> _cache = new();

        private static Cache? GetCache(Identity identity)
        {
            _cache.TryGetValue(identity, out Cache? cache);
            return cache;
        }

        private static void AddCache(Identity identity,
                                     Func<DbDataReader, object>? resultDeserializer,
                                     Action<IDbCommand, object>? parametersGenerator)
        {
            _cache.TryAdd(identity, new Cache
            {
                ResultDeserializer = resultDeserializer,
                ParametersGenerator = parametersGenerator
            });
        }

    }
}
