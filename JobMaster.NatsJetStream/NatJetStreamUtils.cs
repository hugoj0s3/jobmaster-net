using System.Globalization;
using System.Text;
using NATS.Client.Core;

namespace JobMaster.NatsJetStream;

internal static class NatJetStreamUtils
{
    // public static string GetSubjectName(string fullBucketAddressId)
    // {
    //     var sanitizedBucketId = NatJetStreamUtils.SanitizeName(fullBucketAddressId);
    //     // Preserve dots to ensure routing matches stream subject wildcard (e.g., jobmaster.>)
    //     return $"{NatJetStreamConstants.Prefix}{sanitizedBucketId}";
    // }
    //
    // public static string GetConsumerName(string fullBucketAddressId)
    // {
    //     var sanitizedBucketId = NatJetStreamUtils.SanitizeName(fullBucketAddressId);
    //     return $"consumer_{sanitizedBucketId}";
    // }
    //
    // public static string GetStreamName(string agentConnectionId)
    // {
    //     return SanitizeName($"{NatJetStreamConfigKey.NamespaceUniqueKey}__{agentConnectionId}");
    // }
    //
    // public static string SanitizeName(string name)
    // {
    //     if (string.IsNullOrEmpty(name)) return string.Empty;
    //     var sb = new StringBuilder(name.Length);
    //     foreach (var ch in name)
    //     {
    //         if (char.IsLetterOrDigit(ch) || ch == '_' || ch == '-' || ch == ':')
    //         {
    //             sb.Append(char.ToLowerInvariant(ch));
    //         }
    //         else
    //         {
    //             sb.Append('_');
    //         }
    //     }
    //     return sb.ToString();
    // }
    
    public static string SanitizeName(string streamName)
    {
        return streamName.Replace(".", "_");
    }
    
    public static string GetSubjectName(string agentConnectionId, string fullBucketAddressId)
    {
        return $"{NatJetStreamConstants.Prefix}{agentConnectionId}._{fullBucketAddressId}";
    }
    
    public static string GetConsumerName(string fullAddressBucketId)
    {
        return NatJetStreamUtils.SanitizeName($"consumer_{fullAddressBucketId}");
    }
    
    public static string GetStreamName(string agentConnectionId)
    {
        return NatJetStreamUtils.SanitizeName($"{NatJetStreamConfigKey.NamespaceUniqueKey}__{agentConnectionId}");
    }

    
    public static string LogPreview(string s, int maxBytes)
    {
        if (string.IsNullOrEmpty(s) || maxBytes <= 0) return string.Empty;
        var utf8 = Encoding.UTF8;
        var bytes = utf8.GetBytes(s);
        if (bytes.Length <= maxBytes) return s;
        var truncated = utf8.GetString(bytes, 0, maxBytes);
        return truncated + "…";
    }
    
    public static (string? signature, string? correlationId, DateTime? referenceTimeUtc, string? messageId) GetHeaderValues(NatsHeaders? headers)
    {
        string? signature = GetHeaderMessageId(headers, NatJetStreamConstants.HeaderSignature);

        // CorrelationId (jm-correlation-id)
        string? correlationId = GetHeaderMessageId(headers, NatJetStreamConstants.HeaderCorrelationId);

        // Reference time (jm-reference-time) as DateTime
        DateTime? referenceTimeUtc = null;
        if (headers != null && headers.TryGetValue(NatJetStreamConstants.HeaderReferenceTime, out var rtimeRaw))
        {
            if (DateTime.TryParse(rtimeRaw, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
                referenceTimeUtc = parsed;
        }

        // MessageId (jm-message-id or configured alias)
        string? messageId = GetHeaderMessageId(headers, NatJetStreamConstants.HeaderMessageId);
        
        return (signature, correlationId, referenceTimeUtc, messageId);
    }
    
    public static string? GetHeaderMessageId(NatsHeaders? headers)
    {
        return GetHeaderMessageId(headers, NatJetStreamConstants.HeaderMessageId);
    }
    
    public static bool? GetConcurrencyRisk(NatsHeaders? headers)
    {
        var boolStr = GetHeaderMessageId(headers, NatJetStreamConstants.HeaderConcurrencyRisk);
        if (boolStr == null) return null;
        
        return string.Equals(boolStr, "true", StringComparison.OrdinalIgnoreCase);
    }
    
    public static string? GetHeaderMessageId(NatsHeaders? headers, string headerId)
    {
        if (headers != null && headers.TryGetValue(headerId, out var mid))
        {
            return mid;
        }
        
        return null;  
    }  
}