using System.Buffers;
using System.Security.Cryptography;
#if NET
using SecurityDriven.Core;
#endif

namespace JobMaster.Sdk.Utils;

internal static class JobMasterRandomUtil
{
#if NETSTANDARD2_0
    private static readonly ThreadLocal<Random> ThreadLocalRandom = new ThreadLocal<Random>(() => new Random());
    private static Random Rnd => ThreadLocalRandom.Value;
    private static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create();
#endif

#if NET
    private static readonly CryptoRandom Rnd = new();
#endif
    public static int GetInt() => Rnd.Next();

    public static int GetInt(int fromInclusive, int toExclusive) => Rnd.Next(fromInclusive, toExclusive);

    public static int GetInt(int toExclusive) => Rnd.Next(toExclusive);

    public static double GetDouble() => Rnd.NextDouble();
    
    public static bool GetBoolean() => Rnd.Next() % 2 == 0;

    public static void FillBytes(byte[] buffer) => Rnd.NextBytes(buffer);

#if NET
    public static void FillBytes(Span<byte> buffer) => Rnd.NextBytes(buffer);
#endif
#if NETSTANDARD2_0
    public static void FillBytes(Span<byte> buffer)
    {
        // netstandard2.0 lacks RandomNumberGenerator.Fill(Span<byte>)
        byte[] rented = ArrayPool<byte>.Shared.Rent(buffer.Length);
        try
        {
            Rng.GetBytes(rented, 0, buffer.Length);
            new ReadOnlySpan<byte>(rented, 0, buffer.Length).CopyTo(buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }
#endif

    // 0 < probability < 1
    public static bool GetBoolean(double probability, bool probabilityOfTrueOrFalse = true)
    {
        if (probability < 0 || probability > 1)
        {
            throw new ArgumentException();
        }

        if (!probabilityOfTrueOrFalse)
        {
            probability = 1 - probability;
        }

        return GetDouble() < probability;
    }

    public static T GetEnum<T>() where T : struct
    {
        if (!typeof(T).IsEnum)
        {
            throw new ArgumentException();
        }

        var values = Enum.GetValues(typeof(T));
        if (values.Length == 0)
        {
            throw new ArgumentException();
        }
        
        var result = (T)(values.GetValue(GetInt(values.Length)) ?? throw new InvalidOperationException());

        return result;
    }
    
    public static Guid NewGuid() => Guid.NewGuid();
}