using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Utils;

namespace JobMaster.SqlBase.Scripts;

internal class SqlOrderByUtil
{
    internal static string BuildOrderByClause(SortByCriteria? sortByCriteria, string alias, string? defaultOrder = null)
    {
        var sortBy = defaultOrder ?? string.Empty;
        
        if (sortByCriteria == null) return sortBy;
        
        if (!string.IsNullOrWhiteSpace(sortByCriteria.Property))
        {
            var ascOrDesc = sortByCriteria.Ascending ? "ASC" : "DESC";
            sortBy = $" ORDER BY {alias}.{JobMasterStringUtils.ToSnakeCase(sortByCriteria.Property!)} {ascOrDesc} ";
        }
        
        return sortBy;
    }
}