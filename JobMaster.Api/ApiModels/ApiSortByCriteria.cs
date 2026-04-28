using System.Reflection;
using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.ApiModels;

public class ApiSortByCriteria
{
    public string Property { get; set; } = string.Empty;
    public bool Ascending { get; set; }

    public static ValueTask<ApiSortByCriteria?> BindAsync(HttpContext context, ParameterInfo parameter)
    {
        var query = context.Request.Query;

        static string? GetQueryValue(IQueryCollection q, params string[] keys)
        {
            foreach (var key in keys)
            {
                if (q.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v))
                    return v.ToString();
            }

            return null;
        }

        var property = GetQueryValue(
            query,
            "sortByProperty",
            "SortByProperty",
            "sortBy",
            "SortBy",
            "sortBy.property",
            "SortBy.Property");

        if (string.IsNullOrWhiteSpace(property))
            return ValueTask.FromResult<ApiSortByCriteria?>(null);

        var ascendingRaw = GetQueryValue(
            query,
            "sortByAscending",
            "SortByAscending",
            "sortByAsc",
            "SortByAsc",
            "sortBy.ascending",
            "SortBy.Ascending");

        var ascending = true;
        if (!string.IsNullOrWhiteSpace(ascendingRaw) && bool.TryParse(ascendingRaw, out var parsedAscending))
        {
            ascending = parsedAscending;
        }

        return ValueTask.FromResult<ApiSortByCriteria?>(new ApiSortByCriteria
        {
            Property = property,
            Ascending = ascending
        });
    }
}