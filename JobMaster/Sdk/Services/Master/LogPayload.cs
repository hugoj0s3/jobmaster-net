namespace JobMaster.Sdk.Services.Master;

public sealed class LogPayload
{
    public int Level { get; set; }
    public string Message { get; set; } = string.Empty;
    public DateTime TimestampUtc { get; set; }
    public string? Host { get; set; }
    public string? SourceMember { get; set; }
    public string? SourceFile { get; set; }
    public int? SourceLine { get; set; }
}