using JobMaster.Sdk.Abstractions.Extensions;
using NATS.Client.JetStream;

namespace JobMaster.NatsJetStream;

internal static class NatsJetStreamMsgExtensions
{
    // Never includes the payload (user data); its size only.
    public static string ToLogSummary(this INatsJSMsg<byte[]> msg)
    {
        var (_, correlationId, _, messageId) = NatsJetStreamUtils.GetHeaderValues(msg.Headers);
        return JobMasterRawMessageExtensions.FormatLogSummary(correlationId, messageId, msg.Data?.Length ?? 0);
    }
}
