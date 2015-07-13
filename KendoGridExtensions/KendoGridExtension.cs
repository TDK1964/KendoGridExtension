using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Dapper;

namespace KendoGridExtensions
{
    public class KendoGridExtension
    {
        public static IEnumerable<T> Query<T>(string sql, object param)
        {
            using (var c = Helper.GetConnection())
            {
                return c.Query<T>(sql, param);
            }
        }

        public static object ExecuteScalar(string sql, object param, CommandType? commandType)
        {
            using (var c = Helper.GetConnection())
            {
                return c.ExecuteScalar(sql, param, null, null, commandType);
            }
        }

        public static GridResult<T> Query<T>(string sql, string defaultSort, GridRequest request, object param)
        {
            using (var c = Helper.GetConnection())
            {
                var parameters = new DynamicParameters(param);
                var countSql = GetSqlForRequest(sql, defaultSort, request, parameters, true, null);
                var count = Query<int>(countSql, parameters).FirstOrDefault();

                parameters = new DynamicParameters(param);
                var resultSql = GetSqlForRequest(sql, defaultSort, request, parameters, false, null);
                var result = Query<T>(resultSql, parameters);

                var dataSourceRequest = new GridResult<T>
                {
                    Total = count,
                    Data = result.ToList()
                };
                return dataSourceRequest;
            }
        }

        public static string GetSqlForRequest(string sql, string defaultSort, GridRequest request, DynamicParameters param, bool returnCountSql, string idColumn)
        {
            var lastIndexOfFrom = IndexOfFrom(sql) - 1;
            var columns = sql.Substring(6, lastIndexOfFrom - 6);
            var mainSql = sql.Substring(lastIndexOfFrom + 6);
            var selectColumns = ProtectedSplit(columns, ',').Select(c => c.Trim()).ToList();
            var columnMap = GetColumnMap(sql, selectColumns);
            var hasWhere = mainSql.Contains(" WHERE ");

            columns = string.Join(", ", selectColumns.Where(c => idColumn == null || c == idColumn).ToArray());
            if (columns.Length > 2)
                columns.Remove(columns.Length - 2, 2);

            var sqlSort = new StringBuilder("ORDER BY ");
            var trimSort = false;
            if (request.sort != null && request.sort.Any())
            {
                var isAllColumnQuery = columns.Trim() == "*";
                foreach (var sort in request.sort)
                {
                    if (columnMap.ContainsKey(sort.field.ToUpper()) || isAllColumnQuery)
                    {
                        sqlSort.AppendFormat("{0} {1}, ", isAllColumnQuery ? sort.field : columnMap[sort.field.ToUpper()],
                        sort.dir == "asc" ? "ASC" : "DESC");
                        trimSort = true;
                    }
                }

                if (trimSort)
                    sqlSort.Remove(sqlSort.Length - 2, 2);
                else if (!string.IsNullOrEmpty(defaultSort))
                    sqlSort.AppendFormat("{0}", defaultSort);
            }
            else if (!string.IsNullOrEmpty(defaultSort))
            {
                sqlSort.AppendFormat("{0}", defaultSort);
            }
            else
                sqlSort = new StringBuilder();

            if (request.filter != null && request.filter.filters.Any())
            {
                var whereBuilder = new StringBuilder();
                whereBuilder.Append(hasWhere ? " AND " : " WHERE ");

                var index = 1;
                foreach (var filter in request.filter.filters)
                {
                    if (index > 1) whereBuilder.Append(request.filter.logic == "and" ? " AND " : " OR ");
                    WriteFilterDescriptor(columnMap, filter, whereBuilder, index, param);
                    index++;
                }
                sql += whereBuilder.ToString();
            }

            var countSql = sql.Remove(6, lastIndexOfFrom - 6).Insert(6, " COUNT(*) ");
            if (returnCountSql)
                return countSql;

            if (request.pageSize > 0)
            {
                var newSql = sql.Insert(7, string.Format("ROW_NUMBER() OVER(ORDER BY {0}) AS {1}, ", sqlSort.ToString().Substring(9), "_row_number"));
                var startValue = ((request.page - 1) * request.pageSize) + 1;
                var resultSql = string.Format("SELECT TOP({0}) {1} FROM ({2}) [_proj] WHERE {3} >= @_pageStartRow ORDER BY {3}", request.pageSize, columns.Trim(), newSql, "_row_number");
                param.Add("_pageStartRow", startValue, null, null, null);
                return resultSql;
            }

            var finalSql = string.IsNullOrEmpty(idColumn) ? sql : sql.Remove(6, lastIndexOfFrom - 6).Insert(6, " " + idColumn + " ");
            if (sqlSort.Length > 0)
                finalSql += " " + sqlSort;

            return finalSql;
        }

        private static void WriteFilterDescriptor(Dictionary<string, string> columnMap, GridFilterInstance simpleFilter, StringBuilder whereBuilder, int index, DynamicParameters dbArgs)
        {
            var member = columnMap[simpleFilter.field.ToUpper()];
            switch (simpleFilter.@operator)
            {
                case "contains":
                    whereBuilder.AppendFormat("{0} LIKE @_p{1}", member, index);
                    dbArgs.Add("_p" + index, "%" + simpleFilter.value + "%", null, null, null);
                    break;
                case "doesnotcontain":
                    whereBuilder.AppendFormat("{0} NOT LIKE @_p{1}", member, index);
                    dbArgs.Add("_p" + index, "%" + simpleFilter.value + "%", null, null, null);
                    break;
                case "endswith":
                    whereBuilder.AppendFormat("{0} LIKE @_p{1}", member, index);
                    dbArgs.Add("_p" + index, "%" + simpleFilter.value, null, null, null);
                    break;
                case "isequalto":
                    whereBuilder.AppendFormat("{0} = @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "isgreaterthan":
                    whereBuilder.AppendFormat("{0} > @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "isgreaterthanorequal":
                    whereBuilder.AppendFormat("{0} >= @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "islessthan":
                    whereBuilder.AppendFormat("{0} < @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "islessthanorequal":
                    whereBuilder.AppendFormat("{0} <= @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "isnotequalto":
                    whereBuilder.AppendFormat("{0} <> @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value, null, null, null);
                    break;
                case "startswith":
                    whereBuilder.AppendFormat("{0} LIKE @_p{1}", member, index);
                    dbArgs.Add("_p" + index, simpleFilter.value + "%", null, null, null);
                    break;
            }
        }

        private static Dictionary<string, string> GetColumnMap(string sql, List<string> selectColumns)
        {
            var columnMap = new Dictionary<string, string>();
            for (var i = 0; i < selectColumns.Count; i++)
            {
                var selectColumn = selectColumns[i];
                if (selectColumn.ToUpper().IndexOf(" AS ") > -1)
                {
                    selectColumns[i] = selectColumn.Substring(selectColumn.ToUpper().IndexOf(" AS ") + 4).Trim();
                    selectColumn = selectColumn.Substring(0, selectColumn.ToUpper().IndexOf(" AS "));
                }
                else if (selectColumn.IndexOf(".") > -1)
                    selectColumns[i] = selectColumn.Substring(selectColumn.IndexOf(".") + 1).Trim();

                columnMap.Add(selectColumns[i].Replace("[", "").Replace("]", "").ToUpper().Trim(), selectColumn);
            }
            return columnMap;
        }

        private static int IndexOfFrom(string text)
        {
            var search = text.ToUpper();
            var bracketCount = 0;
            for (var index = 0; index < search.Length; index++)
            {
                if (search[index] == '(')
                    bracketCount++;
                else if (search[index] == ')')
                    bracketCount--;
                else if (search[index] == 'F' && bracketCount == 0)
                {
                    if (index + 4 <= search.Length && search.Substring(index, 4) == "FROM")
                        return index;
                }
            }
            return -1;
        }

        private static string[] ProtectedSplit(string text, char separator)
        {
            var sep = "#####";
            char str = char.MinValue;
            var nb_brackets = 0;
            var new_str = "";
            for (var i = 0; i < text.Length; i++)
            {
                if (str == char.MinValue && Regex.IsMatch(text[i].ToString(), "['\"`]")) str = text[i];
                else if (str != char.MinValue && text[i] == str) str = char.MinValue;
                else if (str == char.MinValue && text[i] == '(') nb_brackets++;
                else if (str == char.MinValue && text[i] == ')') nb_brackets--;

                if (text[i] == separator && (nb_brackets > 0 || str != char.MinValue)) new_str += sep;
                else new_str += text[i];
            }

            return new_str.Split(separator).Select(t => t.Replace(sep, separator.ToString())).ToArray();
        }

        //private static int WriteCompositeFilter(Dictionary<string, string> columnMap, GridFilterInstance composite, StringBuilder whereBuilder, int index, DynamicParameters dbArgs)
        //{
        //    if (composite.FilterDescriptors.Any())
        //    {
        //        whereBuilder.Append("(");
        //        foreach (var subFilter in composite.FilterDescriptors)
        //        {
        //            if (subFilter is FilterDescriptor)
        //                WriteFilterDescriptor(columnMap, (FilterDescriptor)subFilter, whereBuilder, index, dbArgs);
        //            else if (subFilter is CompositeFilterDescriptor)
        //                index = WriteCompositeFilter(columnMap, (CompositeFilterDescriptor)subFilter, whereBuilder, index, dbArgs);

        //            switch (composite.LogicalOperator)
        //            {
        //                case FilterCompositionLogicalOperator.And:
        //                    whereBuilder.Append(" AND ");
        //                    break;
        //                case FilterCompositionLogicalOperator.Or:
        //                    whereBuilder.Append(" OR ");
        //                    break;
        //            }
        //            index++;
        //        }
        //        whereBuilder.Remove(whereBuilder.Length - 4, 4);
        //        whereBuilder.Append(")");
        //    }
        //    return index;
        //}
    }

    public class GridRequest
    {
        public int? take { get; set; }
        public int? skip { get; set; }
        public int? page { get; set; }
        public int? pageSize { get; set; }

        public GridFilter filter { get; set; }
        public GridSort[] sort { get; set; }
    }

    public class GridFilter
    {
        public string logic { get; set; }
        public GridFilterInstance[] filters { get; set; }
    }

    public class GridSort
    {
        public string dir { get; set; }
        public string field { get; set; }
    }

    public class GridFilterInstance
    {
        public string field { get; set; }
        public string @operator { get; set; }
        public string value { get; set; }
    }

    public class GridResult<T>
    {
        public int Total { get; set; }
        public List<T> Data { get; set; }
        public string[] Errors { get; set; }
        //public IEnumerable AggregateResults { get; set; }
    }

}