using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;

namespace JobMaster.Sdk.Services.Master;

internal class MasterAgentConnectionService : JobMasterClusterAwareComponent, IMasterAgentConnectionService
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly JobMasterInMemoryKeys cacheKeys;
    private readonly JobMasterSentinelKeys sentinelKeys;
    private readonly JobMasterLockKeys lockKeys;
    private readonly IJobMasterInMemoryCache jobMasterInMemoryCache;
    private readonly IMasterChangesSentinelService masterChangesSentinelService;
    private readonly IMasterHeartbeatService masterHeartbeatService;
    private readonly IMasterBucketsService masterBucketsService;
    private readonly IMasterDistributedLockerService masterDistributedLockerService;
    private readonly RetryDeadlockPolicy retryDeadlockPolicy;
    private readonly SentinelCachedReader sentinelCachedReader;

    public MasterAgentConnectionService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterChangesSentinelService masterChangesSentinelService,
        IKnownExceptionIdentifier knownExceptionIdentifier,
        IMasterHeartbeatService masterHeartbeatService,
        IMasterBucketsService masterBucketsService,
        IMasterDistributedLockerService masterDistributedLockerService) : base(clusterConnConfig)
    {
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.cacheKeys = new JobMasterInMemoryKeys(clusterConnConfig.ClusterId);
        this.sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
        this.lockKeys = new JobMasterLockKeys(clusterConnConfig.ClusterId);
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.masterHeartbeatService = masterHeartbeatService;
        this.masterBucketsService = masterBucketsService;
        this.masterDistributedLockerService = masterDistributedLockerService;

        this.retryDeadlockPolicy =
            new RetryDeadlockPolicy(knownExceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);

        this.sentinelCachedReader =
            new SentinelCachedReader(jobMasterInMemoryCache, masterChangesSentinelService);
    }

    public async Task<AgentConnectionModel> SaveConnectionAsync(
        AgentConnectionId agentConnectionId,
        string repositoryTypeId,
        string fingerprint,
        bool protectChanges)
    {
        var lockKey = lockKeys.AgentConnectionLock(agentConnectionId.IdValue);
        var lockToken = await AcquireConnectionLockAsync(lockKey);
        try
        {
            var agentConnectionRecord = await GetRecordAsync(agentConnectionId);
            if (agentConnectionRecord is null)
            {
                agentConnectionRecord = new AgentConnectionRecord(ClusterConnConfig.ClusterId)
                {
                    Id = agentConnectionId.IdValue,
                    Fingerprint = fingerprint,
                    CreatedAt = DateTime.UtcNow,
                    FingerprintCreatedAt = DateTime.UtcNow,
                    RepositoryTypeId = repositoryTypeId,
                };
            }

            // Always sync to the caller's current setting so protection can be toggled by
            // changing config and restarting, not just fixed at first creation.
            agentConnectionRecord.ProtectConnectionChanges = protectChanges;

            if (agentConnectionRecord.Fingerprint != fingerprint)
            {
                agentConnectionRecord.Fingerprint = fingerprint;
                agentConnectionRecord.FingerprintCreatedAt = DateTime.UtcNow;
            }

            var record = GenericRecordEntry
                .Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.AgentConnection, agentConnectionRecord.Id,
                    agentConnectionRecord);

            masterChangesSentinelService.NotifyChanges(sentinelKeys.AgentConnections());
            await masterGenericRecordRepository.UpsertAsync(record);

            var lastHeartbeat =
                masterHeartbeatService.GetLastHeartbeat(ResourceHeartbeatType.AgentConnection, agentConnectionId.IdValue);
            return ToModel(agentConnectionRecord, lastHeartbeat);
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKey, lockToken);
        }
    }

    /// <summary>
    /// Acquires the per-connection lock, retrying briefly to ride out concurrent bootstrap from
    /// multiple processes registering the same shared connection at the same time.
    /// </summary>
    private async Task<string> AcquireConnectionLockAsync(string lockKey)
    {
        const int maxAttempts = 10;
        var delay = TimeSpan.FromMilliseconds(200);

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var token = masterDistributedLockerService.TryLock(lockKey, TimeSpan.FromSeconds(30));
            if (token != null)
            {
                return token;
            }

            if (attempt < maxAttempts)
            {
                await Task.Delay(delay);
            }
        }

        throw new InvalidOperationException($"Could not acquire lock '{lockKey}' to save the agent connection.");
    }

    public async Task<IList<AgentConnectionModel>> QueryAllAsync(bool useCache = true)
    {
        var records = await GetAllAgentConnectionsAsync(useCache);

        var heartbeats = masterHeartbeatService.GetLastHeartbeats(
            ResourceHeartbeatType.AgentConnection,
            records.Select(x => x.Id).ToList());

        return records.Select(x => ToModel(x, heartbeats.GetOrDefault(x.Id))).ToList();
    }

    public async Task<bool> SafeDeleteConnectionAsync(AgentConnectionId agentConnectionId)
    {
        // Shares the same lock key as SaveConnectionAsync so a delete can never race a concurrent
        // fingerprint save/recreation for the same connection.
        var lockKey = lockKeys.AgentConnectionLock(agentConnectionId.IdValue);
        var lockToken = masterDistributedLockerService.TryLock(lockKey, TimeSpan.FromSeconds(30));
        if (lockToken is null)
        {
            // Someone else is touching this connection right now — skip, the cleanup runner will retry.
            return false;
        }

        try
        {
            if (await HasBucketsAsync(agentConnectionId))
            {
                return false;
            }

            masterChangesSentinelService.NotifyChanges(sentinelKeys.AgentConnections());
            await this.masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.AgentConnection, agentConnectionId.IdValue);

            return true;
        }
        finally
        {
            masterDistributedLockerService.ReleaseLock(lockKey, lockToken);
        }
    }

    public async Task<bool> HasBucketsAsync(AgentConnectionId agentConnectionId)
    {
        var buckets = await this.masterBucketsService.QueryAsync(new MasterBucketQueryCriteria()
        {
            AgentConnectionId = agentConnectionId.IdValue
        });

        if (buckets.Any())
        {
            return true;
        }

        return false;
    }

    public async Task<AgentConnectionModel?> GetConnectionAsync(AgentConnectionId agentConnectionId,
        bool useCache = true)
    {
        if (useCache)
        {
            var all = await this.QueryAllAsync(useCache);
            return all.FirstOrDefault(x => x.Id?.IdValue == agentConnectionId?.IdValue);
        }

        var agentConnectionRecord = await GetRecordAsync(agentConnectionId);
        if (agentConnectionRecord is null)
        {
            return null;
        }

        var lastHeartbeatAt =
            masterHeartbeatService.GetLastHeartbeat(ResourceHeartbeatType.AgentConnection, agentConnectionId.IdValue);

        return ToModel(agentConnectionRecord, lastHeartbeatAt);
    }

    private async Task<IList<AgentConnectionRecord>> GetAllAgentConnectionsAsync(bool useCache = true)
    {
        if (!useCache)
        {
            // Bypass the sentinel check but still warm the cache so subsequent cached
            // reads don't double-fetch.
            var fresh = await retryDeadlockPolicy.ExecAsync(FetchAllAgentConnectionsAsync);
            jobMasterInMemoryCache.Set(cacheKeys.AgentConnections, fresh);
            return fresh;
        }

        return await retryDeadlockPolicy.ExecAsync(() => sentinelCachedReader.GetOrFetchAsync(
            cacheKey: cacheKeys.AgentConnections,
            sentinelKey: sentinelKeys.AgentConnections(),
            factory: FetchAllAgentConnectionsAsync,
            allowedDiscrepancy: JobMasterConstants.AgentConnectionsAllowDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: TimeSpan.FromMinutes(5)));
    }

    private async Task<IList<AgentConnectionRecord>> FetchAllAgentConnectionsAsync()
    {
        var records = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.AgentConnection);
        return records.Select(x => x.ToObject<AgentConnectionRecord>()).ToList();
    }


    private AgentConnectionModel ToModel(AgentConnectionRecord agentConnectionRecord, DateTime? lastHeartbeatAt)
    {
        AgentConnectionId agentConnectionId = new AgentConnectionId(agentConnectionRecord.Id);
        var resolvedLastHeartbeatAt = lastHeartbeatAt.HasValue && lastHeartbeatAt.Value > agentConnectionRecord.FingerprintCreatedAt
            ? lastHeartbeatAt.Value
            : agentConnectionRecord.FingerprintCreatedAt;

        return new AgentConnectionModel(ClusterConnConfig.ClusterId)
        {
            Id = agentConnectionId,
            Fingerprint = agentConnectionRecord.Fingerprint,
            CreatedAt = agentConnectionRecord.CreatedAt,
            FingerprintCreatedAt = agentConnectionRecord.FingerprintCreatedAt,
            LastHeartbeatAt = resolvedLastHeartbeatAt,
            RepositoryTypeId = agentConnectionRecord.RepositoryTypeId,
            ProtectConnectionChanges = agentConnectionRecord.ProtectConnectionChanges,
        };
    }


    private async Task<AgentConnectionRecord?> GetRecordAsync(AgentConnectionId agentConnectionId)
    {
        var record = await this.masterGenericRecordRepository
            .GetAsync(MasterGenericRecordGroupIds.AgentConnection, agentConnectionId.IdValue);

        if (record is null)
        {
            return null;
        }

        var agentConnectionRecord = record.ToObject<AgentConnectionRecord>();
        return agentConnectionRecord;
    }

    private class AgentConnectionRecord : JobMasterBaseModel
    {
        public AgentConnectionRecord(string clusterId) : base(clusterId)
        {
        }

        protected AgentConnectionRecord()
        {
        }

        public string Id { get; set; } = string.Empty;
        public string Fingerprint { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime FingerprintCreatedAt { get; set; }
        public string RepositoryTypeId { get; set; } = string.Empty;
        public bool ProtectConnectionChanges { get; set; }
    }
}
