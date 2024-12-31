using SqlMapper.Helpers;
using System.Collections;
using System.Data;
using System.Data.Common;

namespace SqlMapper.Core
{
    public static partial class SqlExtension
    {
        public static IEnumerable<dynamic> Query(this IDbConnection conn, string sql,
                                                 object? param = null, IDbTransaction? trans = null, bool lazy = false)
        {
            IEnumerable<dynamic> result = QueryImpl(conn, sql, param, trans);
            return lazy ? result : result.ToList();
        }

        public static IEnumerable<TResult> Query<TResult>(this IDbConnection conn, string sql,
                                                          object? param = null, IDbTransaction? trans = null, bool lazy = false)
        {
            IEnumerable<TResult> result = QueryImpl<TResult>(conn, sql, param, trans);
            return lazy ? result : result.ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="sql">SELECT TOP 1 ...</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static dynamic? QueryFirstOrDefault(this IDbConnection conn, string sql,
                                                   object? param = null, IDbTransaction? trans = null)
        {
            // CommandBehavior.SingleRow: 指定一次只返回一行
            IEnumerable<dynamic> result
                = QueryImpl(conn, sql, param, trans, behavior: CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            return result.FirstOrDefault();
        }

        public static TResult? QueryFirstOrDefault<TResult>(this IDbConnection conn, string sql,
                                                            object? param = null, IDbTransaction? trans = null)
        {
            IEnumerable<TResult> result
                = QueryImpl<TResult>(conn, sql, param, trans, behavior: CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            return result.FirstOrDefault();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="conn"></param>
        /// <param name="sql">e.g. SELECT TOP 1 1... / SELECT COUNT(1)...</param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static TResult? QuerySingleOrDefault<TResult>(this IDbConnection conn, string sql,
                                                             object? param = null, IDbTransaction? trans = null)
        {
            using var command = conn.CreateCommand();
            command.CommandText = sql;
            command.Transaction = trans;

            if (param != null)
            {
                InvokeParamtersGenerator(command, param);
            }

            object? res = command.ExecuteScalar();
            if (res == null || res is DBNull)
            {
                return default;
            }

            if (res is TResult)
            {
                return (TResult?)res;
            }

            return (TResult?)Convert.ChangeType(res, typeof(TResult));
        }

        public static int Execute(this IDbConnection conn, string sql, object? param = null, IDbTransaction? trans = null)
        {
            using var command = conn.CreateCommand();
            command.CommandText = sql;
            command.Transaction = trans;

            if (param != null)
            {
                if (param.IsObjectEnumerableUnprimitive())
                {
                    InvokeParamGeneratorsForMultipleExec(command, (IEnumerable)param);
                }
                else
                {
                    InvokeParamtersGenerator(command, param, ignoreNull: false);
                }
            }

            return command.ExecuteNonQuery();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="behavior">
        /// CommandBehavior.SequentialAccess: 按顺序读取列，在读取完一列后，会将该列从内存中删除。 避免一次性将一行数据读取到内存中.
        /// CommandBehavior.SingleResult: 指定一次只返回一个结果集
        /// </param>
        /// <returns></returns>
        private static IEnumerable<dynamic> QueryImpl(IDbConnection conn, string sql,
                                                      object? param = null, IDbTransaction? trans = null,
                                                      CommandBehavior behavior = CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)
        {
            using var command = conn.CreateCommand();
            command.CommandText = sql;
            command.Transaction = trans;

            if (param != null)
            {
                InvokeParamtersGenerator(command, param);
            }

            using var reader = command.ExecuteReader(behavior);
            while (reader.Read())
            {
                yield return reader.ConvertToDynamic();
            }
        }

        private static IEnumerable<TResult> QueryImpl<TResult>(IDbConnection conn, string sql,
                                                               object? param = null, IDbTransaction? trans = null,
                                                               CommandBehavior behavior = CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)
        {
            using var command = conn.CreateCommand();
            command.CommandText = sql;
            command.Transaction = trans;

            Identity identity = new(sql, typeof(TResult), param?.GetType());
            Cache? cache = GetCache(identity);
            Action<IDbCommand, object>? paramsGenerator = cache?.ParametersGenerator;
            if (param != null)
            {
                paramsGenerator ??= CreateParamGenerator(param, sql);
                paramsGenerator.Invoke(command, param);
            }

            using var reader = (DbDataReader)command.ExecuteReader(behavior);
            Func<DbDataReader, object>? deserializer = cache?.ResultDeserializer;
            if (deserializer == null)
            {
                deserializer = CreateDeserializer(reader, typeof(TResult));
                AddCache(identity, deserializer, paramsGenerator);
            }

            while (reader.Read())
            {
                var obj = deserializer.Invoke(reader);
                yield return (TResult)obj;
            }
        }
    }
}
