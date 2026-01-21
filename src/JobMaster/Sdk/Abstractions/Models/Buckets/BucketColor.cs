namespace JobMaster.Sdk.Abstractions.Models.Buckets;

public enum BucketColor
{
    Red = 1,
    Brown = 2,
    DarkBlue = 3,
    Teal = 4,
    SkyBlue = 5,
    Olive = 6,
    Green = 7,      // Uses Forest hex color (#228B22)
    Tomato = 8,
    Blue = 9,       // Replaces Navy
    Purple = 10,
    Pink = 11,      // Uses DeepPink hex color (#FF1493)
    Charcoal = 12
}

public static class BucketColorExtensions
{
    private static readonly Dictionary<BucketColor, string> ColorMap = new()
    {
        { BucketColor.Red, "#FF0000" },
        { BucketColor.Brown, "#A52A2A" },
        { BucketColor.DarkBlue, "#00008B" },
        { BucketColor.Teal, "#008080" },
        { BucketColor.SkyBlue, "#87CEEB" },
        { BucketColor.Olive, "#808000" },
        { BucketColor.Green, "#228B22" },
        { BucketColor.Tomato, "#FF6347" },
        { BucketColor.Blue, "#0000FF" },
        { BucketColor.Purple, "#800080" },
        { BucketColor.Pink, "#FF1493" },
        { BucketColor.Charcoal, "#36465F" }
    };
    public static string ToBucketColorHex(this BucketColor color)
    {
        return ColorMap[color];
    }
}