namespace JobMaster.Contracts.Extensions;

internal static class EnumExtensions
{
    internal static string ToRecordString(this Enum value)
    {
        int intValue = Convert.ToInt32(value);
        string stringValue = intValue.ToString();
        return stringValue;
    }
    
    internal static T FromRecordString<T>(this string value) where T : struct, Enum
    {
        int intValue = Convert.ToInt32(value);
        T enumValue = (T)Enum.ToObject(typeof(T), intValue);
        return enumValue;
    }
    
    internal static Enum FromRecordString(this string value, Type enumType)
    {
        int intValue = Convert.ToInt32(value);
        Enum enumValue = (Enum)Enum.ToObject(enumType, intValue);
        return enumValue;
    }
}