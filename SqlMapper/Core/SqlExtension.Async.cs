using Microsoft.Data.SqlClient;
using SqlMapper.Helpers;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace SqlMapper.Core
{
    public static partial class SqlExtension
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <param name="cancel">[EnumeratorCancellation] 控制异步迭代器的流程</param>
        /// <returns></returns>
        public static async IAsyncEnumerable<dynamic> QueryAsync(this SqlConnection conn,
                                                                 string sql,
                                                                 object? param = null,
                                                                 SqlTransaction? trans = null,
                                                                 [EnumeratorCancellation] CancellationToken cancel = default)
        {
            using var command = new SqlCommand(sql, conn, trans);

            if (param != null)
            {
                InvokeParamtersGenerator(command, param);
            }

            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, cancel);
            while (await reader.ReadAsync(cancel))
            {
                yield return reader.ConvertToDynamic();
            }
        }

        public static async IAsyncEnumerable<TResult> QueryAsync<TResult>(this SqlConnection conn,
                                                                          string sql,
                                                                          object? param = null,
                                                                          SqlTransaction? trans = null,
                                                                          [EnumeratorCancellation] CancellationToken cancel = default)
        {
            using var command = new SqlCommand(sql, conn, trans);

            Identity identity = new(sql, typeof(TResult), param?.GetType());
            Cache? cache = GetCache(identity);
            Action<IDbCommand, object>? paramsGenerator = cache?.ParametersGenerator;
            if (param != null)
            {
                paramsGenerator ??= CreateParamGenerator(param, sql);
                paramsGenerator.Invoke(command, param);
            }

            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, cancel);

            Func<DbDataReader, object>? deserializer = cache?.ResultDeserializer;
            if (deserializer == null)
            {
                deserializer = CreateDeserializer(reader, typeof(TResult));
                AddCache(identity, deserializer, paramsGenerator);
            }

            while (await reader.ReadAsync(cancel))
            {
                var obj = deserializer.Invoke(reader);
                yield return (TResult)obj;
            }
        }

        public static async Task<IList<dynamic>> QueryListAsync(this SqlConnection conn,
                                                                string sql,
                                                                object? param = null,
                                                                SqlTransaction? trans = null,
                                                                CancellationToken cancel = default)
        {
            return await conn.QueryListAsyncImpl(sql, param, trans, cancel);
        }

        public static async Task<IList<TResult>> QueryListAsync<TResult>(this SqlConnection conn,
                                                                         string sql,
                                                                         object? param = null,
                                                                         SqlTransaction? trans = null,
                                                                         CancellationToken cancel = default)
        {
            return await conn.QueryListAsyncImpl<TResult>(sql, param, trans, cancel);
        }

        public static async Task<dynamic?> QueryFirstOrDefaultAsync(this SqlConnection conn,
                                                                    string sql,
                                                                    object? param = null,
                                                                    SqlTransaction? trans = null,
                                                                    CancellationToken cancel = default)
        {
            IList<dynamic> res
                = await conn.QueryListAsyncImpl(sql, param, trans, cancel, behavior: CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            return res.FirstOrDefault();
        }

        public static async Task<TResult?> QueryFirstOrDefaultAsync<TResult>(this SqlConnection conn,
                                                                             string sql,
                                                                             object? param = null,
                                                                             SqlTransaction? trans = null,
                                                                             CancellationToken cancel = default)
        {
            IList<TResult> res
                = await conn.QueryListAsyncImpl<TResult>(sql, param, trans, cancel, behavior: CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);
            return res.FirstOrDefault();
        }

        public static async Task<TResult?> QuerySingleOrDefaultAsync<TResult>(this SqlConnection conn,
                                                                              string sql,
                                                                              object? param = null,
                                                                              SqlTransaction? trans = null,
                                                                              CancellationToken cancel = default)
        {
            using var command = new SqlCommand(sql, conn, trans);

            if (param != null)
            {
                InvokeParamtersGenerator(command, param);
            }

            object? res = await command.ExecuteScalarAsync(cancel);
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

        public static async Task<int> ExecuteAsync(this SqlConnection conn, string sql, object? param = null, SqlTransaction? trans = null,
                                                   CancellationToken cancel = default)
        {
            var command = new SqlCommand(sql, conn);

            if (param != null)
            {
                if (param.IsObjectEnumerableUnprimitive())
                {
                    InvokeParamGeneratorsForMultipleExec(command, (IEnumerable)param);
                }
                else
                {
                    InvokeParamtersGenerator(command, param);
                }
            }

            return await command.ExecuteNonQueryAsync(cancel);
        }

        private static async Task<IList<dynamic>> QueryListAsyncImpl(this SqlConnection conn,
                                                                     string sql,
                                                                     object? param = null,
                                                                     SqlTransaction? trans = null,
                                                                     CancellationToken cancel = default,
                                                                     CommandBehavior behavior = CommandBehavior.SequentialAccess
                                                                                              | CommandBehavior.SingleResult)
        {
            using var command = new SqlCommand(sql, conn, trans);

            if (param != null)
            {
                InvokeParamtersGenerator(command, param);
            }

            using var reader = await command.ExecuteReaderAsync(behavior, cancel);
            var res = new List<dynamic>();
            while (await reader.ReadAsync(cancel))
            {
                res.Add(reader.ConvertToDynamic());
            }
            return res;
        }

        private static async Task<IList<TResult>> QueryListAsyncImpl<TResult>(this SqlConnection conn,
                                                                              string sql,
                                                                              object? param = null,
                                                                              SqlTransaction? trans = null,
                                                                              CancellationToken cancel = default,
                                                                              CommandBehavior behavior = CommandBehavior.SequentialAccess
                                                                                                       | CommandBehavior.SingleResult)
        {
            using var command = new SqlCommand(sql, conn, trans);

            Identity identity = new(sql, typeof(TResult), param?.GetType());
            Cache? cache = GetCache(identity);
            Action<IDbCommand, object>? paramsGenerator = cache?.ParametersGenerator;
            if (param != null)
            {
                paramsGenerator ??= CreateParamGenerator(param, sql);
                paramsGenerator.Invoke(command, param);
            }

            using var reader = await command.ExecuteReaderAsync(behavior, cancel);

            Func<DbDataReader, object>? deserializer = cache?.ResultDeserializer;
            if (deserializer == null)
            {
                deserializer = CreateDeserializer(reader, typeof(TResult));
                AddCache(identity, deserializer, paramsGenerator);
            }

            var res = new List<TResult>();
            while (await reader.ReadAsync(cancel))
            {
                var obj = deserializer.Invoke(reader);
                res.Add((TResult)obj);
            }
            return res;
        }
    }
}
