using SqlMapper.Helpers;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace SqlMapper.Core
{
    public static partial class SqlExtension
    {
        private static dynamic ConvertToDynamic(this IDataReader reader)
        {
            dynamic dynamicObj = new ExpandoObject();
            var dic = (IDictionary<string, object?>)dynamicObj;

            for (int i = 0; i < reader.FieldCount; i++)
            {
                // 在CommandBehavior.SequentialAccess下, reader[i]只能被访问一次
                dic.Add(reader.GetName(i), reader.IsDBNull(i) ? null : reader[i]);
            }
            return dynamicObj;
        }

        private static Func<DbDataReader, object> CreateDeserializer(DbDataReader reader, Type type)
        {
            IEnumerable<string> columnNames = Enumerable.Range(0, reader.FieldCount)
                                                        .Select(index => reader.GetName(index));

            PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            var members = columnNames.Select((columnName, index) =>
            {
                Type? propertyType = Array.Find(properties, p => p.Name.ToLower() == columnName.ToLower())?.PropertyType;

                return new
                {
                    Index = index,
                    ColumnName = columnName,
                    PropertyType = propertyType,
                };
            }).Where(p => p.PropertyType != null);

            LinkedList<Expression> exps = new();

            // T genericObj
            ParameterExpression genericObjExp = Expression.Variable(type, "genericObj");
            // genericObj = new T()
            BinaryExpression newTypeObjExp = Expression.Assign(genericObjExp, Expression.New(type));
            exps.AddLast(newTypeObjExp);

            // object value
            ParameterExpression valExp = Expression.Variable(typeof(object), "value");
            // value = defalut(object)
            BinaryExpression initValExp = Expression.Assign(valExp, Expression.Constant(null));
            exps.AddLast(initValExp);

            ParameterExpression dBReaderExp = Expression.Parameter(typeof(DbDataReader), "reader");

            foreach (var member in members)
            {
                // reader[index] 
                MethodCallExpression callGetItemExp = Expression.Call(dBReaderExp, ReflectionInfos.DbDataReader_GetItem, Expression.Constant(member.Index));
                // value = reader[index] 
                BinaryExpression setValExp = Expression.Assign(valExp, callGetItemExp);
                exps.AddLast(setValExp);

                // genericObj.Property
                MemberExpression genericObjPropExp = Expression.Property(genericObjExp, member.ColumnName);
                // (PropertyType)value
                UnaryExpression convertValTypeExp = Expression.Convert(valExp, member.PropertyType!);
                // genericObj.Property = (PropertyType)value
                BinaryExpression setPropValExp = Expression.Assign(genericObjPropExp, convertValTypeExp);

                // !(value is System.DBNull)
                UnaryExpression valNotDBNullExp = Expression.Not(Expression.TypeIs(valExp, typeof(DBNull)));
                // if(!(value is System.DBNull)) genericObj.Property = (PropertyType)value
                ConditionalExpression ifThenExp = Expression.IfThen(valNotDBNullExp, setPropValExp);
                exps.AddLast(ifThenExp);
            }

            // return genericObj
            exps.AddLast(genericObjExp);

            BlockExpression blockExp = Expression.Block([genericObjExp, valExp], exps);
            Expression<Func<DbDataReader, object>> lambda = Expression.Lambda<Func<DbDataReader, object>>(blockExp, dBReaderExp);
            return lambda.Compile();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="command"></param>
        /// <param name="param"></param>
        /// <param name="sql"></param>
        /// <param name="ignoreDefault">true: 实体中的null value将不会添加到SqlParameter中。 默认应用于SELECT中</param>
        /// <param name="multiExecIndex"></param>
        private static void InvokeParamtersGenerator(IDbCommand command,
                                                     object param,
                                                     string? sql = null,
                                                     bool ignoreNull = true,
                                                     int? multiExecIndex = null)
        {
            Identity identity = new(sql ?? command.CommandText, null, param.GetType());
            Action<IDbCommand, object>? paramsGenerator = GetCache(identity)?.ParametersGenerator;
            if (paramsGenerator == null)
            {
                paramsGenerator = CreateParamGenerator(param, identity.Sql!, ignoreNull, multiExecIndex);
                AddCache(identity, null, paramsGenerator);
            }

            paramsGenerator.Invoke(command, param);
        }

        private static void InvokeParamGeneratorsForMultipleExec(IDbCommand command, IEnumerable parameters)
        {
            Type type = parameters.FirstOrDefault()!.GetType();

            //创建要替换的子串与子串在sql中第一次出现位置的dict
            Dictionary<string, int> propIndexDict
                              = type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                    .Select(p => $"@{p.Name}")
                                    .Where(e => command.CommandText.Contains(e, StringComparison.OrdinalIgnoreCase))
                                    .ToDictionary(e => e, e => command.CommandText.IndexOf(e));

            StringBuilder sqlBuilder = new();
            int index = 0;
            foreach (var param in parameters)
            {
                string currentSql = new(command.CommandText);
                int suffixLength = index.ToString().Length + 1;
                int offset = 0;
                foreach (var propIndex in propIndexDict)
                {
                    currentSql = currentSql.Substring(0, propIndex.Value + offset * suffixLength)
                               + $"{propIndex.Key}_{index}"
                               + currentSql.Substring(propIndex.Value + propIndex.Key.Length + offset * suffixLength);
                    offset++;
                }
                sqlBuilder.Append($"{currentSql};");
                InvokeParamtersGenerator(command, param, currentSql, ignoreNull: false, multiExecIndex: index);
                index++;
            }

            command.CommandText = sqlBuilder.ToString();
        }

        private static Action<IDbCommand, object> CreateParamGenerator(object param, string sql, bool ignoreNull = true, int? multiExecIndex = null)
        {
            Type type = param.GetType();
            if (type.IsEnumerable())
            {
                return (_command, _param) =>
                {
                    HandleEnumerableParam(_command, (IEnumerable)_param);
                };
            }
            // 只处理存在于sqlText中的属性
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                 .Where(p => sql.Contains(p.Name, StringComparison.OrdinalIgnoreCase));

            LinkedList<Expression> exps = new();


            // Prepare: IDbDataParameter parameter = command.CreateParameter()
            ParameterExpression commandExp = Expression.Parameter(typeof(IDbCommand), "command");
            MethodCallExpression callCreateParamExp = Expression.Call(commandExp, ReflectionInfos.IDbCommand_CreateParameter);
            ParameterExpression parameterObjExp = Expression.Variable(typeof(IDbDataParameter), "parameter");
            BinaryExpression createParameterExp = Expression.Assign(parameterObjExp, callCreateParamExp);

            // Prepare: 传入参数的表达式
            ParameterExpression paramObjExp = Expression.Parameter(typeof(object), "param");
            UnaryExpression convertParamExp = Expression.Convert(paramObjExp, type);

            // 执行 IDataParameterCollection dataParameters = command.Parameters
            ParameterExpression dataParamExp = Expression.Variable(typeof(IDataParameterCollection), "dataParameters");
            MethodCallExpression callGetParamsExp = Expression.Call(commandExp, ReflectionInfos.IDbCommand_GetParameters);
            BinaryExpression setDataParamExp = Expression.Assign(dataParamExp, callGetParamsExp);
            exps.AddLast(setDataParamExp);

            foreach (var property in properties)
            {
                // Prepare： param.Property
                MemberExpression paramPropExp = Expression.Property(convertParamExp, property.Name);

                if (property.PropertyType.IsEnumerable())
                {
                    // param.EnumerableProp
                    UnaryExpression enumerableExp = Expression.Convert(paramPropExp, typeof(IEnumerable));

                    // call HandleEnumerableParam method 
                    MethodCallExpression callHandleEnumberableExp
                        = Expression.Call(null,
                                          ReflectionInfos.SqlExtension_HandleEnumerableParam,
                                          commandExp,
                                          enumerableExp,
                                          Expression.Constant(property.Name));
                    exps.AddLast(callHandleEnumberableExp);
                    continue;
                }

                // 提前准备好Expression，但不加入表达树集中
                // parameter.Value = (object)param.Property
                UnaryExpression convertObjExp = Expression.Convert(paramPropExp, typeof(object));
                MethodCallExpression setObjValueExp = Expression.Call(parameterObjExp, ReflectionInfos.IDataParameter_SetValue, convertObjExp);

                // parameter.ParameterName = propertyName
                MethodCallExpression setParamNameExp = Expression.Call(parameterObjExp,
                                                                       ReflectionInfos.IDataParameter_SetParameterName,
                                                                       Expression.Constant(multiExecIndex is null
                                                                                         ? $"@{property.Name}"
                                                                                         : $"@{property.Name}_{multiExecIndex}"));

                // dataParameters.Add(paramter)
                MethodCallExpression callAddParamExp = Expression.Call(dataParamExp, ReflectionInfos.IList_Add, parameterObjExp);

                if (!property.PropertyType.IsNullable())
                {
                    // 如果不是Nullable，直接赋值
                    exps.AddLast(Expression.Block(createParameterExp, setObjValueExp, setParamNameExp, callAddParamExp));
                    continue;
                }

                // parameter.Value = DBNull.Value
                MemberExpression dBNullExp = Expression.Field(null, ReflectionInfos.DBNull_Value);
                MethodCallExpression setDBNullValueExp = Expression.Call(parameterObjExp, ReflectionInfos.IDataParameter_SetValue, dBNullExp);

                // 如果为Nullable且prop是null, 判断ignoreNull。如果不忽略的话 set DBNull.Value
                BinaryExpression isPropNullExp = Expression.Equal(paramPropExp, Expression.Constant(null, property.PropertyType));
                ConstantExpression notIgnoreNullExp = Expression.Constant(!ignoreNull, typeof(bool));

                ConditionalExpression ifSetDBNullExp
                    = Expression.IfThen(notIgnoreNullExp,
                                        Expression.Block(createParameterExp, setDBNullValueExp, setParamNameExp, callAddParamExp));
                ConditionalExpression conditionalExp
                    = Expression.IfThenElse(isPropNullExp,
                                            ifSetDBNullExp,
                                            Expression.Block(createParameterExp, setObjValueExp, setParamNameExp, callAddParamExp));
                exps.AddLast(conditionalExp);
            }

            BlockExpression blockExp = Expression.Block([parameterObjExp, dataParamExp], exps);

            Expression<Action<IDbCommand, object>> lambda
                = Expression.Lambda<Action<IDbCommand, object>>(blockExp, commandExp, paramObjExp);
            return lambda.Compile();
        }

        private static void HandleEnumerableParam(IDbCommand command, IEnumerable parameters, string? paramName = null)
        {
            if (!parameters.IsEnumerablePrimitive())
            {
                throw new InvalidOperationException();
            }

            bool paramOnlyEnumerbale = false;
            if (paramName == null)
            {
                paramName = "param";
                paramOnlyEnumerbale = true;
            }
            paramName = $"@{paramName}";

            int index = 0;
            List<string> paramNames = new();
            foreach (var param in parameters)
            {
                IDbDataParameter _param = command.CreateParameter();
                string _paramName = $"{paramName}{index}";
                _param.ParameterName = _paramName;
                _param.Value = param;
                command.Parameters.Add(_param);
                paramNames.Add(_paramName);
                index++;
            }

            if (paramOnlyEnumerbale)
            {
                // Find the parameter formatted '@...' and replace it
                command.CommandText = ParameterRegex().Replace(command.CommandText, $"({string.Join(",", paramNames)})");
            }
            else
            {
                command.CommandText = command.CommandText.Replace(paramName, $"({string.Join(",", paramNames)})");
            }
        }

        [GeneratedRegex("@([^ ]+)")]
        private static partial Regex ParameterRegex();
    }
}
