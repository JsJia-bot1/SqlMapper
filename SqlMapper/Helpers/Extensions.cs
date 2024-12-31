using System.Collections;

namespace SqlMapper.Helpers
{
    static class Extensions
    {
        internal static bool IsNullable(this Type type)
        {
            return type == typeof(string) || Nullable.GetUnderlyingType(type) != null;
        }

        internal static bool IsPrimitiveExtend(this Type type)
        {
            return type.IsPrimitive || type.IsEnum || type == typeof(string) || type == typeof(DateTime);
        }

        internal static bool IsEnumerable(this Type type)
        {
            return typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string);
        }

        internal static bool IsObjectEnumerableUnprimitive(this object obj)
        {
            if (!obj.GetType().IsEnumerable())
            {
                return false;
            }

            return !IsEnumerablePrimitive((IEnumerable)obj);
        }

        internal static bool IsEnumerablePrimitive(this IEnumerable enumerable)
        {
            object? firstObj = enumerable.FirstOrDefault();
            if (firstObj == null || !firstObj.GetType().IsPrimitiveExtend())
            {
                return false;
            }
            return true;
        }

        internal static object? FirstOrDefault(this IEnumerable enumerable)
        {
            object? firstObj = null;
            IEnumerator enumerator = enumerable.GetEnumerator();
            while (enumerator.MoveNext())
            {
                firstObj = enumerator.Current;
                break;
            }
            return firstObj;
        }
    }
}
