using System.Text;

#if JOBMASTER
namespace JobMaster.Internals;
#elif NATJS
namespace JobMaster.NatsJetStream.Internals.Utils;
#elif SQLPROV
namespace JobMaster.SqlBase.Internals.Utils;
#else
namespace JobMaster.Internal.Utils;
#endif

internal class JobMasterIdUtil
{
    private static readonly DateTime NanoIdEpoch = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private const string NanoIdAlphabet = "abcdefghjkmnpqrstuvwxyz23456789";
    private const int NanoIdLength = 3;

    private const string Base32Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    public static string NewShortId()
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

    private static string RandomBase32(int length)
    {
        Span<byte> bytes = stackalloc byte[length];
        JobMasterRandomUtil.FillBytes(bytes);

        var sb = new StringBuilder(length);
        foreach (var b in bytes)
            sb.Append(Base32Alphabet[b % 32]);

        return sb.ToString();
    }
    
    public static string NewNanoId() {
        
        var chars = new char[NanoIdLength];
        for (int i = 0; i < NanoIdLength; i++)
        {
            chars[i] = NanoIdAlphabet[JobMasterRandomUtil.GetInt(0, NanoIdAlphabet.Length)];
        }
    
        var seconds = ((int)(DateTime.UtcNow - NanoIdEpoch).TotalSeconds).ToString("x");
        var timestamp = seconds.Length > 5 ? seconds.Substring(seconds.Length - 5) : seconds.PadLeft(5, '0');
    
        return timestamp + new string(chars);
    }
}