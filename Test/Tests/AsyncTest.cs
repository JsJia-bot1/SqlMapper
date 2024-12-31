using Microsoft.Data.SqlClient;
using SqlMapper.Core;
using Test.Tests.Base;

namespace Test.Tests
{
    public class AsyncTest
    {
        //[Fact]
        //public async Task Async_Test()
        //{
        //    static async Task action(SqlConnection conn)
        //    {
        //        await conn.ExecuteAsync(@"
        //        CREATE TABLE #Id1 (Id int,Name varchar(20),Age int);");

        //        IEnumerable<dynamic> res1 = conn.Query(@"SELECT * FROM #User WHERE Name = @Name", new { Name = "A" });
        //        Assert.Equal("A", res1.First().Name);
        //    }

        //    await TestBase.ExecuteTestAsync(action, "#User");
        //}
    }
}
