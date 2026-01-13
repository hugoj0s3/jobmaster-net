using System;

namespace JobMaster.Sdk.Contracts.Exceptions;

public sealed class PublishOutcomeUnknownException : Exception
{
    public string SupposedPublishedId { get; }

    public PublishOutcomeUnknownException(string message, string supposedPublishedId, Exception inner)
        : base(message, inner)
    {
        SupposedPublishedId = supposedPublishedId;
    }
}
