namespace JobMaster.Sdk.Utils;

// Math.Clamp isn't available on netstandard2.0.
internal static class JobMasterMathUtil
{
    public static int Clamp(int value, int min, int max) => Math.Min(Math.Max(value, min), max);

    public static double Clamp(double value, double min, double max) => Math.Min(Math.Max(value, min), max);

    public static TimeSpan Clamp(TimeSpan value, TimeSpan min, TimeSpan max) => value < min ? min : value > max ? max : value;
}
