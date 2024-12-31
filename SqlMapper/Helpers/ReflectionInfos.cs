using SqlMapper.Core;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace SqlMapper.Helpers
{
    static class ReflectionInfos
    {
        internal static readonly MethodInfo DbDataReader_GetItem
            = Array.Find(typeof(DbDataReader).GetMethods(), m => m.Name == "get_Item")!;

        internal static readonly MethodInfo IDbCommand_CreateParameter
            = Array.Find(typeof(IDbCommand).GetMethods(), m => m.Name == "CreateParameter")!;

        internal static readonly MethodInfo IDbCommand_GetParameters
            = Array.Find(typeof(IDbCommand).GetMethods(), m => m.Name == "get_Parameters")!;

        internal static readonly MethodInfo IList_Add
            = Array.Find(typeof(IList).GetMethods(), m => m.Name == "Add")!;

        internal static readonly MethodInfo IDataParameter_SetParameterName
            = Array.Find(typeof(IDataParameter).GetMethods(), m => m.Name == "set_ParameterName")!;

        internal static readonly MethodInfo IDataParameter_SetValue
            = Array.Find(typeof(IDataParameter).GetMethods(), m => m.Name == "set_Value")!;

        internal static readonly MethodInfo SqlExtension_HandleEnumerableParam =
            Array.Find(typeof(SqlExtension).GetMethods(BindingFlags.NonPublic | BindingFlags.Static),
                       m => m.Name == "HandleEnumerableParam")!;

        internal static readonly FieldInfo DBNull_Value =
            typeof(DBNull).GetField("Value", BindingFlags.Public | BindingFlags.Static)!;
    }
}
