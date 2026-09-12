using System.Buffers;
using System.Security.Cryptography;
#if NET6_0_OR_GREATER
using SecurityDriven.Core;
#endif

namespace JobMaster.Sdk.Utils;

internal static class JobMasterRandomUtil
{
#if NETSTANDARD2_0
    private static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create();
    
    private static readonly ThreadLocal<Random> ThreadLocalRandom = new ThreadLocal<Random>(() => 
    {
        byte[] seedBytes = new byte[4];
        Rng.GetBytes(seedBytes);
        int seed = BitConverter.ToInt32(seedBytes, 0);
        return new Random(seed);
    });
    
    private static Random Rnd => ThreadLocalRandom.Value;
#endif

#if NET6_0_OR_GREATER
    private static readonly CryptoRandom Rnd = new();
#endif
    public static int GetInt() => Rnd.Next();

    public static int GetInt(int fromInclusive, int toExclusive) => Rnd.Next(fromInclusive, toExclusive);

    public static int GetInt(int toExclusive) => Rnd.Next(toExclusive);

    public static double GetDouble() => Rnd.NextDouble();

    // Fisher-Yates, in place. Used to randomize row-processing order for a retry after a deadlock --
    // a fresh random order gives the retry a different lock-acquisition path than whatever order just
    // collided, without needing to know anything about why the original order failed.
    public static void Shuffle<T>(IList<T> list)
    {
        for (var i = list.Count - 1; i > 0; i--)
        {
            var j = GetInt(i + 1);
            (list[i], list[j]) = (list[j], list[i]);
        }
    }

    public static bool GetBoolean() => Rnd.Next() % 2 == 0;

    
#if NET6_0_OR_GREATER
    public static void FillBytes(byte[] buffer) => Rnd.NextBytes(buffer);
    public static void FillBytes(Span<byte> buffer) => Rnd.NextBytes(buffer);
#endif

#if NETSTANDARD2_0
    // Use Rng here so it matches the security of the Span<byte> version below
    public static void FillBytes(byte[] buffer) => Rng.GetBytes(buffer); 

    public static void FillBytes(Span<byte> buffer)
    {
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

    public static T GetEnum<T>() where T : struct, Enum
    {
        var values = EnumCache<T>.Values;
        if (values.Length == 0)
        {
            throw new ArgumentException($"Enum {typeof(T).Name} has no values.");
        }
        
        return values[GetInt(values.Length)];
    }
    
    // This nested class automatically caches the array per-type the first time it is accessed!
    private static class EnumCache<T> where T : struct, Enum
    {
        public static readonly T[] Values = (T[])Enum.GetValues(typeof(T));
    }
    
    public static Guid NewGuid4() => Guid.NewGuid();
    
    public static Guid NewGuid7() => DoNewGuid7(DateTimeOffset.UtcNow);

    private static Guid DoNewGuid7(DateTimeOffset timestamp)
    {
        var unixMilliseconds = timestamp.ToUnixTimeMilliseconds();

        var timeHigh = (uint)(unixMilliseconds >> 16);
        var timeLow = (ushort)unixMilliseconds;

        Span<byte> bytes = stackalloc byte[10];
        FillBytes(bytes);

        var randA = (ushort)(0x7000u | ((bytes[0] & 0xF) << 8) | bytes[1]);

        return new Guid(timeHigh, timeLow, randA,
            (byte)(bytes[2] & 0x3F | 0x80), bytes[3], bytes[4], bytes[5],
            bytes[6], bytes[7], bytes[8], bytes[9]);
    }
}