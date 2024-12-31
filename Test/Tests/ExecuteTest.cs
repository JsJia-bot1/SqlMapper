using Microsoft.Data.SqlClient;
using SqlMapper.Core;
using Test.Tests.Base;
using Test.Tests.Models;

namespace Test.Tests
{
    public class ExecuteTest
    {
        [Fact]
        public void InsertNull_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"CREATE TABLE #User (Id int, Name varchar(20), Age int)");

                // 对于非查询操作，null值会默认转为DBNull.Value
                conn.Execute(@"INSERT #User VALUES(@Id,@Name,@Age)", new User { });

                IEnumerable<dynamic> res1 = conn.Query(@"SELECT * FROM #User WHERE Id = @Id", new { Id = 0 });
                Assert.Equal(0, res1.First().Id);
                Assert.Null(res1.First().Name);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void InsertInconsistent_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"CREATE TABLE #User (Id int, Name varchar(20))");

                // 对于sqlText中指定的参数和实体中属性不一致的情况，只会处理sqlText中指定的参数
                conn.Execute(@"INSERT #User VALUES(@Id,@Name)", new User { });

                IEnumerable<dynamic> res1 = conn.Query(@"SELECT * FROM #User WHERE Id = @Id", new { Id = 0 });
                Assert.Equal(0, res1.First().Id);
                Assert.Null(res1.First().Name);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void InsertMultiple_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"CREATE TABLE #User (Id int, Name varchar(20), Age int)");

                // 对于批量执行的情况 会把sqlText转换为批量形式
                // TODO 2100 parameter limit  
                conn.Execute(@"INSERT #User VALUES(@Id,@Name,@Age)", new List<object>()
                {
                    new { Id = 1, Name = "A", Age = 10 },
                    new { Id = 2, Name = "B", Age = 11 },
                    new { Id = 3, Name = "C", Age = 12 },
                });

                int count = conn.QuerySingleOrDefault<int>(@"SELECT COUNT(1) FROM #User");
                Assert.Equal(3, count);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void InsertMultipleLimit_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"CREATE TABLE #User (Id int)");

                var lst = Enumerable.Range(0, 2000).Select(e => new { Id = e });
                conn.Execute(@"INSERT #User VALUES(@Id)", lst);

                int count = conn.QuerySingleOrDefault<int>(@"SELECT COUNT(1) FROM #User");
                Assert.Equal(lst.Count(), count);
            }

            TestBase.ExecuteTest(action, "#User");
        }
    }
}
