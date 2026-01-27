namespace JobMaster.Sdk.Abstractions.Models;

internal class AddSavePendingResult
{
    public AddSavePendingResultCode ResultCode { get; }
    public string? PublishedMessageId { get; }
    public Exception? Exception { get; }
    
    public string? BucketId { get; }

    public AddSavePendingResult(
        AddSavePendingResultCode resultCode,
        string? bucketId = null,
        string? publishedMessageId = null,
        Exception? exception = null)
    {
        ResultCode = resultCode;
        PublishedMessageId = publishedMessageId;
        Exception = exception;
    }
}
