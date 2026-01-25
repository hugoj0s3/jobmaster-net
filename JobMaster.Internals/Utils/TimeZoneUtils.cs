using System.Runtime.InteropServices;
using TimeZoneConverter;

#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatsJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.SqlBase.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal static class TimeZoneUtils
{
    public static string GetLocalIanaTimeZoneId()
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? TZConvert.WindowsToIana(TimeZoneInfo.Local.Id)
            : TimeZoneInfo.Local.Id;
    }
    
    public static bool IsValidIanaId(string ianaTimeZoneId)
    {
        return TZConvert.KnownIanaTimeZoneNames.Contains(ianaTimeZoneId);
    }

    public static DateTime ConvertUtcToDateTimeTz(DateTime utcDateTime, string ianaTimeZoneId)
    {
        if (string.IsNullOrWhiteSpace(ianaTimeZoneId))
            throw new ArgumentException("IANA time zone id is required", nameof(ianaTimeZoneId));

        // Normalize input to UTC
        var utc = utcDateTime.Kind switch
        {
            DateTimeKind.Utc => utcDateTime,
            DateTimeKind.Local => utcDateTime.ToUniversalTime(),
            _ => DateTime.SpecifyKind(utcDateTime, DateTimeKind.Utc)
        };

        var tz = TZConvert.GetTimeZoneInfo(ianaTimeZoneId);
        var local = TimeZoneInfo.ConvertTimeFromUtc(utc, tz);
        // Ensure the result is kind Unspecified (represents wall-clock in the specified TZ)
        return DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
    }
    
    public static DateTime ConvertDateTimeTzToUtc(DateTime dateTime, string ianaTimeZoneId)
    {
        if (string.IsNullOrWhiteSpace(ianaTimeZoneId))
            throw new ArgumentException("IANA time zone id is required", nameof(ianaTimeZoneId));

        // Treat the provided DateTime as a wall-clock time in the specified TZ
        var wallClock = dateTime.Kind == DateTimeKind.Unspecified
            ? dateTime
            : DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified);

        var tz = TZConvert.GetTimeZoneInfo(ianaTimeZoneId);
        var utc = TimeZoneInfo.ConvertTimeToUtc(wallClock, tz);
        return DateTime.SpecifyKind(utc, DateTimeKind.Utc);
    }
}