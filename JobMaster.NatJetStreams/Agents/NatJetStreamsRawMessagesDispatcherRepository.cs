using System.Collections.Concurrent;
using System.Threading;
using System.Text;
using JobMaster.Contracts.Utils;
using JobMaster.Sdk.Background;
using JobMaster.Sdk.Contracts;
using JobMaster.Sdk.Contracts.Config;
using JobMaster.Sdk.Contracts.Extensions;
using JobMaster.Sdk.Contracts.Models.GenericRecords;
using JobMaster.Sdk.Contracts.Models.Logs;
using JobMaster.Sdk.Contracts.Repositories.Agent;
using JobMaster.Sdk.Contracts.Exceptions;
using JobMaster.Sdk.Contracts.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Nito.AsyncEx;

namespace JobMaster.NatJetStreams.Agents;

internal class NatJetStreamsRawMessagesDispatcherRepository : 
    JobMasterClusterAwareComponent, 
    IAgentRawMessagesDispatcherRepository
{
    private readonly IJobMasterLogger logger;
    
    protected NatsJSContext? jsContext = null!;
    
    protected string? streamName = null!;
    
    protected readonly ConcurrentDictionary<string, bool> ensuredConsumers = new();
    
    protected JobMasterAgentConnectionConfig config = null!;
    
    public NatJetStreamsRawMessagesDispatcherRepository(JobMasterClusterConnectionConfig clusterConnConfig, IJobMasterLogger logger) : base(clusterConnConfig)
    {
        this.logger = logger;
    }

    public string PushMessage(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        return AsyncContext.Run(() => PushMessageAsync(fullBucketAddressId, payload, referenceTime, correlationId));
    }

    public async Task<string> PushMessageAsync(string fullBucketAddressId, string payload, DateTime referenceTime, string correlationId)
    {
        logger.Debug($"Pushing message to bucket {fullBucketAddressId} correlationId {correlationId}");
        
        await EnsureStreamAsync();
    
        var subject = GetSubjectName(fullBucketAddressId);
    
        return await DoPublishAsync(subject, payload, referenceTime, correlationId);
    }

    public async Task<IList<string>> BulkPushMessageAsync(string fullBucketAddressId, IList<(string payload, DateTime referenceTime, string correlationId)> messages)
    {
        await EnsureStreamAsync();
        await EnsureConsumerAsync(fullBucketAddressId);
        
        var subject = GetSubjectName(fullBucketAddressId);

        var results = new List<string>();
        foreach (var (payload, referenceTime, correlationId) in messages)
        {
            var result = await DoPublishAsync(subject, payload, referenceTime, correlationId);
            results.Add(result);
        }
        
        return results;
    }

    public Task<IList<JobMasterRawMessage>> DequeueMessagesAsync(string fullBucketAddressId, int numberOfJobs, DateTime? referenceTimeTo = null)
    {
        throw new NotSupportedException();
    }
    
    public async Task<bool> HasJobsAsync(string fullBucketAddressId)
    {
        await EnsureStreamAsync();
        await EnsureConsumerAsync(streamName!);
        
        var consumerName = GetConsumerName(streamName!);
        var consumer = await jsContext!.GetConsumerAsync(streamName!, consumerName);
            
        return consumer.Info.NumPending > 0;
    }

    public async Task CreateBucketAsync(string fullBucketAddressId)
    {
        EnsureNamesAreValid(fullBucketAddressId);
        await EnsureStreamAsync();
        await EnsureConsumerAsync(fullBucketAddressId);
    }
    

    public void Initialize(JobMasterAgentConnectionConfig config)
    {
        this.config = config;
        (_, jsContext, streamName) = NatJetStreamConnector.GetOrCreateConnection(config);
    }
    
    public async Task DestroyBucketAsync(string fullBucketAddressId)
    {
        await EnsureStreamAsync();
        
        var consumerName = GetConsumerName(fullBucketAddressId);
        try
        {
            // 1. Delete the consumer from the server.
            await jsContext!.DeleteConsumerAsync(streamName!, consumerName);
        }
        catch (NatsJSApiException)
        {
            // If nats does not exists anymore ignore.
            // TODO Log something 
        }

        // 2. Clear the local cache to keep the SDK in sync.
        ensuredConsumers.TryRemove(fullBucketAddressId, out _);
    }

    public bool IsAutoDequeue => true;
    public string AgentRepoTypeId => NatJetStreamConstants.RepositoryTypeId;
    
    protected async Task EnsureStreamAsync()
    {
        await NatJetStreamConnector.EnsureStreamAsync(config);
    }
    
    private async Task<string> DoPublishAsync(string subject, string payload, DateTime referenceTime, string correlationId)
    {
        var headers = BuildHeaders(referenceTime, correlationId);
        var data = Encoding.UTF8.GetBytes(payload);
        var supposedId = headers[NatJetStreamConstants.HeaderMessageId];

        const int maxAttempts = 3;
        int attempt = 0;
        while (true)
        {
            attempt++;
            try
            {
                using var pubCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                await jsContext!.PublishAsync(subject, data, headers: headers, cancellationToken: pubCts.Token);
                return supposedId; 
            }
            catch (NatsJSTimeoutException ex)
            {
                if (attempt < maxAttempts)
                {
                    var jitter = JobMaster.Contracts.Utils.JobMasterRandomUtil.GetInt(0, 200);
                    var backoffMs = Math.Min(400 * (1 << (attempt - 1)), 2500) + jitter;
                    logger.Error($"JetStream publish transient failure ({ex.GetType().Name}): '{ex.Message}'. Attempt {attempt}/{maxAttempts}. Retrying in {backoffMs}ms.");
                    await Task.Delay(backoffMs);
                    continue;
                }
                logger.Error($"JetStream publish failed after {attempt} attempts.", exception: ex);
                
                // Ambiguous outcome: throw special exception with supposed published id for dedup correlation.
                throw new PublishOutcomeUnknownException("Publish timed out; outcome unknown.", supposedPublishedId: supposedId, ex);
            } 
            catch (Exception ex)
            {
                logger.Error($"JetStream publish failed after {attempt} attempts.", exception: ex);
                throw;
            }
        }
    }
    
    internal static NatsHeaders BuildHeaders(DateTime referenceTime, string correlationId)
    {
        var headers = new NatsHeaders
        {
            [NatJetStreamConstants.HeaderCorrelationId] = correlationId,
            [NatJetStreamConstants.HeaderReferenceTime] = referenceTime.ToUniversalTime().ToString("O"),
            [NatJetStreamConstants.HeaderSignature] = NatJetStreamConfigKey.NamespaceUniqueKey.ToString(),
            // Keep internal correlation id header
            [NatJetStreamConstants.HeaderMessageId] = Guid.NewGuid().ToString()
        };
        
        return headers;
    }

    // Centralized retry for JetStream publish with dedup heade
    
    private async Task EnsureConsumerAsync(string fullBucketAddressId)
    {
        // Check in-memory cache first to avoid NATS API overhead
        if (ensuredConsumers.ContainsKey(fullBucketAddressId)) return;

        await EnsureStreamAsync(); 
        
        await NatJetStreamConnector.GetOrCreateConsumerAsync(config, fullBucketAddressId);
    }
    
    private void EnsureNamesAreValid(string fullBucketAddressId)
    {
        var subjectName = GetSubjectName(fullBucketAddressId);
        var consumerName = GetConsumerName(streamName!);

        if (string.IsNullOrEmpty(subjectName) || subjectName.Length > 255)
        {
            throw new ArgumentException($"Invalid subject name: {subjectName}");
        }

        if (string.IsNullOrEmpty(consumerName) || consumerName.Length > 255)
        {
            throw new ArgumentException($"Invalid consumer name: {consumerName}");
        }
        
        if (string.IsNullOrEmpty(streamName) || streamName?.Length > 255)
        {
            throw new ArgumentException($"Invalid stream name: {streamName}");
        }
    }

    private string GetSubjectName(string fullBucketAddressId) => NatJetStreamUtils.GetSubjectName(config.Id, fullBucketAddressId);
    
    private string GetConsumerName(string steamName) => NatJetStreamUtils.GetConsumerName(steamName);
}

