using System.ComponentModel;
namespace JobMaster.Sdk.Abstractions.Exceptions;

[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class PublishOutcomeUnknownException : Exception
{
    public string SupposedPublishedId { get; }

    public PublishOutcomeUnknownException(string message, string supposedPublishedId, Exception inner)
        : base(message, inner)
    {
        SupposedPublishedId = supposedPublishedId;
    }
}
