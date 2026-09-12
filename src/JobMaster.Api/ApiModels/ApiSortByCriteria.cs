using System.Reflection;
using Microsoft.AspNetCore.Http;

namespace JobMaster.Api.ApiModels;

/// <summary>Specifies the property and direction to use when sorting query results.</summary>
public class ApiSortByCriteria
{
    /// <summary>Name of the property to sort by.</summary>
    public string Property { get; set; } = string.Empty;
    /// <summary><c>true</c> for ascending order; <c>false</c> for descending.</summary>
    public bool Ascending { get; set; }

    /// <summary>Minimal API model binder that reads sort parameters from query-string key variants.</summary>
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