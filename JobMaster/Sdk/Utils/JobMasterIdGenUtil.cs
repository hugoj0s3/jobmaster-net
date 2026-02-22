using System.Text;

namespace JobMaster.Sdk.Utils;

internal class JobMasterIdGenUtil
{
    private const string NanoIdAlphabet = "abcdefghjkmnpqrstuvwxyz23456789";
    private const int NanoIdLength = 5;

    private const string Base32Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    
    private static readonly object IdLock = new object();

    public static string NewShortId()
    {
        lock (IdLock)
        {
            // 1. Use UTC ticks (100ns resolution)
            long ticks = DateTime.UtcNow.Ticks;

            // Convert to hex (up to 16 chars)
            string tickHex = ticks.ToString("x");

            // 2. Add short random base32 suffix for distributed uniqueness
            string rand = RandomBase32(4); // 4 chars ≈ 20 bits

            // 3. Combine — lexicographically sortable and StringUtils-safe
            return $"{tickHex}{rand}";   
        }
    }
    
    public static string TimestampId() {
        lock (IdLock)
        {
            var nanoIdEpoch = new DateTime(DateTime.UtcNow.Year - 1, 1, 1, 0, 0, 0);
            var timestamp = ((int)(DateTime.UtcNow - nanoIdEpoch).TotalSeconds).ToString("x");
            return timestamp;
        }
    }
    
    private static string RandomBase32(int length)
    {
        Span<byte> bytes = stackalloc byte[length];
        JobMasterRandomUtil.FillBytes(bytes);

        var sb = new StringBuilder(length);
        foreach (var b in bytes)
            sb.Append(Base32Alphabet[b % 32]);

        return sb.ToString();
    }
}