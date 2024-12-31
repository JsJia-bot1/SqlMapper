using Microsoft.Data.SqlClient;
using SqlMapper.Core;
using System.Data;
using System.Linq;

namespace Test.Tests.Base
{
    internal static class TestBase
    {
        internal const string CONNECTION_STRING = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=|DataDirectory|\\DB\\Test.mdf;Integrated Security=True;";

        private static SqlConnection GetConnection()
        {
            var connection = new SqlConnection(CONNECTION_STRING);

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;
        }

        private static async Task<SqlConnection> GetConnectionAsync()
        {
            var connection = new SqlConnection(CONNECTION_STRING);

            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
            return connection;
        }

        internal static void ExecuteTest(Action<SqlConnection> action, params string[] tableNames)
        {
            using var conn = GetConnection();
            try
            {
                action.Invoke(conn);
            }
            catch (Exception ex) 
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
            finally
            {
                if (tableNames.Any())
                {
                    conn.Execute(string.Join(';', tableNames.Select(x => $"DROP TABLE {x}")));
                }
            }
        }

        internal static async Task ExecuteTestAsync(Func<SqlConnection, Task> action, params string[] tableNames)
        {
            using var conn = await GetConnectionAsync();
            try
            {
                await action.Invoke(conn);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
            finally
            {
                if (tableNames.Any())
                {
                    await conn.ExecuteAsync(string.Join(';', tableNames.Select(x => $"DROP TABLE {x}")));
                }
            }
        }
    }
}
