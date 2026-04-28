using System.Reflection;

namespace JobMaster.Api.Endpoints;

internal static class EndpointSortingUtil
{
    internal static void ValidateSortingProperty<T>(string propertyName)
    {
        if (string.IsNullOrWhiteSpace(propertyName))
        {
            throw new ArgumentException($"Sort property name cannot be null or empty.", nameof(propertyName));
        }
        
        var propertyInfo = typeof(T).GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (propertyInfo == null)
        {
            throw new ArgumentException($"Invalid sort property '{propertyName}' for type '{typeof(T).Name}'. Property does not exist.", nameof(propertyName));
        }
    }
    
    internal static List<T> ApplySorting<T>(List<T> items, string propertyName, bool ascending)
    {
        ValidateSortingProperty<T>(propertyName);
        
        var propertyInfo = typeof(T).GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        
        if (ascending)
        {
            return items.OrderBy(x => propertyInfo!.GetValue(x)).ToList();
        }
        else
        {
            return items.OrderByDescending(x => propertyInfo!.GetValue(x)).ToList();
        }
    }
}
