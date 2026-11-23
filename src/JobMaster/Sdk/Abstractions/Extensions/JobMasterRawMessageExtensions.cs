using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Abstractions.Extensions;

internal static class JobMasterRawMessageExtensions
{
    // Never includes the payload (user data); its size only.
    public static string ToLogSummary(this JobMasterRawMessage message)
    {
        return FormatLogSummary(message.CorrelationId, message.MessageId, message.Payload.Length);
    }

    /// <summary>Shared by every transport's message log summary, so none of them can log the payload.</summary>
    public static string FormatLogSummary(string? correlationId, string? messageId, int payloadLength)
    {
        var id = Guid.TryParse(correlationId, out var guid) ? guid.ToBase64() : correlationId;
        return $"Id:{id} MessageId:{messageId} PayloadLength:{payloadLength}";
    }
}
