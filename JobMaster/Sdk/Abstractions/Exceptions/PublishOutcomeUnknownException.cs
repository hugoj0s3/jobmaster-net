namespace JobMaster.Sdk.Abstractions.Exceptions;

internal sealed class PublishOutcomeUnknownException : Exception
{
    public string SupposedPublishedId { get; }

    public PublishOutcomeUnknownException(string message, string supposedPublishedId, Exception inner)
        : base(message, inner)
    {
        SupposedPublishedId = supposedPublishedId;
    }
}
