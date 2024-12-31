using Microsoft.Data.SqlClient;
using System.Data;
using System.Data.Common;

namespace SqlMapper.Core
{
    public class TransactionalOperation(string connectionString)
    {
        private readonly string _connectionString = connectionString;

        private SqlConnection? _conn;

        private SqlTransaction? _trans;

        private SqlConnection OpenConnection()
        {
            var connection = new SqlConnection(_connectionString);

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }

        private async Task<SqlConnection> OpenConnectionAsync()
        {
            var connection = new SqlConnection(_connectionString);

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
            return connection;
        }

        public void TransactionalOperate(Action action, IsolationLevel level = IsolationLevel.ReadCommitted)
        {
            _conn = OpenConnection();
            _trans = _conn.BeginTransaction(level);
            try
            {
                action.Invoke();
                _trans.Commit();
            }
            catch (Exception ex)
            {
                _trans.Rollback();
                throw;
            }
            finally
            {
                _trans.Dispose();
                _conn.Close();
                _conn.Dispose();
            }
        }

        public async Task TransactionalOperateAsync(Func<Task> func, IsolationLevel level = IsolationLevel.ReadCommitted)
        {
            _conn = await OpenConnectionAsync();
            DbTransaction dbTrans = await _conn.BeginTransactionAsync(level);
            _trans = (SqlTransaction)dbTrans;
            try
            {
                await func.Invoke();
                await _trans.CommitAsync();
            }
            catch (Exception ex)
            {
                await _trans.RollbackAsync();
                throw;
            }
            finally
            {
                await _trans.DisposeAsync();
                await _conn.CloseAsync();
                await _conn.DisposeAsync();
            }
        }

        public IEnumerable<dynamic> Query(string sql, object? param = null, bool lazy = false)
        {
            return _conn!.Query(sql, param, _trans, lazy);
        }

        public IEnumerable<TResult> Query<TResult>(string sql, object? param = null, bool lazy = false)
        {
            return _conn!.Query<TResult>(sql, param, _trans, lazy);
        }

        public dynamic? QueryFirstOrDefault(string sql, object? param = null)
        {
            return _conn!.QueryFirstOrDefault(sql, param, _trans);
        }

        public TResult? QueryFirstOrDefault<TResult>(string sql, object? param = null)
        {
            return _conn!.QueryFirstOrDefault<TResult>(sql, param, _trans);
        }

        public TResult? QuerySingleOrDefault<TResult>(string sql, object? param = null)
        {
            return _conn!.QuerySingleOrDefault<TResult>(sql, param, _trans);
        }

        public int Execute(string sql, object? param = null)
        {
            return _conn!.Execute(sql, param, _trans);
        }

        public IAsyncEnumerable<dynamic> QueryAsync(string sql, object? param = null)
        {
            return _conn!.QueryAsync(sql, param, _trans);
        }

        public IAsyncEnumerable<TResult> QueryAsync<TResult>(string sql, object? param = null)
        {
            return _conn!.QueryAsync<TResult>(sql, param, _trans);
        }

        public async Task<IList<dynamic>> QueryListAsync(string sql, object? param = null)
        {
            return await _conn!.QueryListAsync(sql, param, _trans);
        }

        public async Task<IList<TResult>> QueryListAsync<TResult>(string sql, object? param = null)
        {
            return await _conn!.QueryListAsync<TResult>(sql, param, _trans);
        }

        public async Task<dynamic?> QueryFirstOrDefaultAsync(string sql, object? param = null)
        {
            return await _conn!.QueryFirstOrDefaultAsync(sql, param, _trans);
        }

        public async Task<TResult?> QueryFirstOrDefaultAsync<TResult>(string sql, object? param = null)
        {
            return await _conn!.QueryFirstOrDefaultAsync<TResult>(sql, param, _trans);
        }

        public async Task<TResult?> QuerySingleOrDefaultAsync<TResult>(string sql, object? param = null)
        {
            return await _conn!.QuerySingleOrDefaultAsync<TResult>(sql, param, _trans);
        }

        public async Task<int> ExecuteAsync(string sql, object? param = null)
        {
            return await _conn!.ExecuteAsync(sql, param, _trans);
        }

    }
}
