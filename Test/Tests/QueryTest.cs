using Microsoft.Data.SqlClient;
using SqlMapper.Core;
using Test.Tests.Base;
using Test.Tests.Models;

namespace Test.Tests
{
    public class QueryTest
    {
        [Fact]
        public void DynamicQuery_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(1,'A',20),(2,'B',21);");

                IEnumerable<dynamic> res1 = conn.Query(@"SELECT * FROM #User WHERE Name = @Name", new { Name = "A" });
                Assert.Equal("A", res1.First().Name);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void QueryFirstOrDefault_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(1,'A',20),(2,'B',21);");

                var res1 = conn.QueryFirstOrDefault<User>(@"SELECT TOP 1 * FROM #User WHERE Name = @Name;", new { Name = "A" })!;
                Assert.Equal("A", res1.Name);

                var res2 = conn.QueryFirstOrDefault<User>(@"SELECT TOP 1 * FROM #User WHERE Name = @Name;", new { Name = "C" });
                Assert.Null(res2);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void QuerySingleOrDefault_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(1,'A',20),(2,'B',21);");

                bool bool1 = conn.QuerySingleOrDefault<bool>(@"SELECT TOP 1 1 FROM #User WHERE Name = @Name;", new { Name = "A" });
                Assert.True(bool1);

                bool bool2 = conn.QuerySingleOrDefault<bool>(@"SELECT TOP 1 1 FROM #User WHERE Name = @Name;", new { Name = "C" });
                Assert.False(bool2);

                int count1 = conn.QuerySingleOrDefault<int>(@"SELECT COUNT(1) FROM #User;");
                Assert.Equal(2, count1);

                int count2 = conn.QuerySingleOrDefault<int>(@"SELECT COUNT(1) FROM #User WHERE Name = @Name;", new { Name = "C" });
                Assert.Equal(0, count2);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void InQuery_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(1,'A',20),(2,'B',21),(3,'C',22);");

                IEnumerable<User> res1 = conn.Query<User>(@"SELECT * FROM #User WHERE Name IN @Names ORDER BY Name",
                                                          new string[] { "A", "B" });
                Assert.Equal("AB", string.Join(string.Empty, res1.Select(x => x.Name)));

                IEnumerable<User> res2 = conn.Query<User>(@"SELECT * FROM #User WHERE Id IN @Ids AND Name = @Name",
                                                          new { Name = "A", Ids = new int[] { 1, 2 } });
                Assert.Equal("A", res2.First().Name);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void LikeQuery_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(1,'ABC',20),(2,'BCD',21);");

                IEnumerable<User> res1 = conn.Query<User>(@"SELECT * FROM #User WHERE Name LIKE @Name",
                                                          new { Name = "AB%" });
                Assert.Equal("ABC", res1.First().Name);

                IEnumerable<User> res2 = conn.Query<User>(@"SELECT * FROM #User WHERE Name LIKE @Name ORDER BY Name",
                                                          new { Name = "%BC%" });
                Assert.Equal("ABCBCD", string.Join(string.Empty, res2.Select(x => x.Name)));

                IEnumerable<User> res3 = conn.Query<User>(@"SELECT * FROM #User WHERE Name LIKE @Name",
                                                          new { Name = "%BC" });
                Assert.Equal("ABC", res3.First().Name);
            }

            TestBase.ExecuteTest(action, "#User");
        }

        [Fact]
        public void ParamerterNullQuery_Test()
        {
            static void action(SqlConnection conn)
            {
                conn.Execute(@"
                CREATE TABLE #User (Id int,Name varchar(20),Age int);
                INSERT #User (Id,Name,Age) VALUES(0,'A',20),(1,'B',21);");

                // int Id = default会被默认加入parameter中, int? Age为Nullable类型，会被默认舍弃
                User? res1 = conn.QueryFirstOrDefault<User>(@"SELECT * FROM #User WHERE Name LIKE @Name",
                                                            new User { Name = "A" });
                Assert.Equal(0, res1!.Id);
            }

            TestBase.ExecuteTest(action, "#User");
        }
    }
}
