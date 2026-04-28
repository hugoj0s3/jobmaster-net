using System.Diagnostics;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Exceptions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Cache;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;
using System.Runtime;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;


internal class MasterHostService : JobMasterClusterAwareComponent, IMasterHostService
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly IMasterHeartbeatService masterHeartbeatService;
    private readonly IMasterChangesSentinelService masterChangesSentinelService;
    private readonly IHostStatsProvider hostStatsProvider;
    private readonly JobMasterInMemoryKeys cacheKeys;
    private readonly JobMasterSentinelKeys sentinelKeys;
    private readonly IJobMasterInMemoryCache jobMasterInMemoryCache;
    private readonly RetryDeadlockPolicy retryDeadlockPolicy;
    private readonly SentinelCachedReader sentinelCachedReader;

    public MasterHostService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        IMasterHeartbeatService masterHeartbeatService,
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterChangesSentinelService masterChangesSentinelService,
        IKnownExceptionIdentifier knownExceptionIdentifier,
        IHostStatsProvider hostStatsProvider) : base(clusterConnConfig)
    {
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.masterHeartbeatService = masterHeartbeatService;
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterChangesSentinelService = masterChangesSentinelService;
        this.hostStatsProvider = hostStatsProvider;

        cacheKeys = new JobMasterInMemoryKeys(clusterConnConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);

        this.retryDeadlockPolicy =
            new RetryDeadlockPolicy(knownExceptionIdentifier, TimeSpan.FromMilliseconds(250), 3);

        this.sentinelCachedReader =
            new SentinelCachedReader(jobMasterInMemoryCache, masterChangesSentinelService);
    }

    public async Task<IList<HostModel>> QueryAllAsync()
    {
        var allHostRecords = await QueryAllRecordsAsync();

        var resourceIds = allHostRecords.Select(x => x.Id).ToList();
        var allHeartbeats =
            masterHeartbeatService.GetLastHeartbeats(ResourceHeartbeatType.Host, resourceIds);

        return allHostRecords.Select(x =>
        {
            var lastHeartbeat = allHeartbeats.GetOrDefault(x.Id);
            return ToHostModel(x, lastHeartbeat);
        }).ToList();
    }

    public async Task<HostId> RegisterNewHostAsync()
    {
        var hostId = new HostId(ClusterConnConfig.ClusterId, Environment.MachineName);
        var process = Process.GetCurrentProcess();
        var hostStats = await CaptureStatsAsync(hostId, process);
       
        var record = new HostRecord(ClusterConnConfig.ClusterId)
        {
            Id = hostId.IdValue,
            HostDisplayName = hostId.HostDisplayName,
            ProcessId = process.Id.ToString(),
            CreatedAt = DateTime.UtcNow,
            StatisticsAt = DateTime.UtcNow,
            OsDescription = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
            CpuUsagePercent = hostStats.CpuUsagePercent,
            DiskAvailableBytes = hostStats.DiskAvailableBytes,
            MemoryTotalBytes = hostStats.MemoryTotalBytes,
            MemoryUsedBytes = hostStats.MemoryUsedBytes,
            ProcessorCount = hostStats.ProcessorCount,
        };
        
        var genericRecord = 
            GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Host, record.Id, record); 
        
        await masterGenericRecordRepository.InsertAsync(genericRecord);
        
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Hosts());

        return hostId;
    }

    public async Task UpdateStatsAsync(string hostId)
    {
        var hosts = await QueryAllRecordsAsync();
        var host = hosts.FirstOrDefault(x => x.Id == hostId);
        if (host == null)
        {
            return;
        }
            
        var hostStats = await CaptureStatsAsync(HostId.Recover(host.HostDisplayName, hostId));
        host.CpuUsagePercent = hostStats.CpuUsagePercent;
        host.DiskAvailableBytes = hostStats.DiskAvailableBytes;
        host.MemoryTotalBytes = hostStats.MemoryTotalBytes;
        host.MemoryUsedBytes = hostStats.MemoryUsedBytes;
        host.StatisticsAt = hostStats.StatisticsAt;
        
        var genericRecord =
            GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Host, host.Id, host);

        await masterGenericRecordRepository.UpsertAsync(genericRecord);

        masterChangesSentinelService.NotifyChanges(sentinelKeys.Hosts());

        masterHeartbeatService.Heartbeat(ResourceHeartbeatType.Host, hostId);
    }

    public async Task DeleteHostsAsync(IList<string> hostIds)
    {
        foreach (var hostId in hostIds)
        {
            await this.masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.Host, hostId);
        }

        masterChangesSentinelService.NotifyChanges(sentinelKeys.Hosts());
    }

    private async Task<IList<HostRecord>> QueryAllRecordsAsync()
    {
        return await retryDeadlockPolicy.ExecAsync(() => sentinelCachedReader.GetOrFetchAsync(
            cacheKey: cacheKeys.AllHosts(),
            sentinelKey: sentinelKeys.Hosts(),
            factory: FetchAllHostRecordsAsync,
            allowedDiscrepancy: JobMasterConstants.HostsAllowDiscrepancy,
            valueFactoryLockDuration: TimeSpan.FromSeconds(2),
            durationToExpire: TimeSpan.FromMinutes(5)));
    }

    private async Task<IList<HostRecord>> FetchAllHostRecordsAsync()
    {
        var allHostRecords = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.Host);
        return allHostRecords.Select(x => x.ToObject<HostRecord>()).ToList();
    }
    
    private async Task<HostStatsInfo> CaptureStatsAsync(HostId hostId, Process? process = null)
    {
        return await hostStatsProvider.GetStatsAsync();
    }
    
    private static HostModel ToHostModel(HostRecord record, DateTime? lastHeartbeatAt)
    {
        var hostId = HostId.Recover(record.HostDisplayName, record.Id);
        return new HostModel(record.ClusterId)
        {
            Id = hostId,
            ProcessId = record.ProcessId,
            OsDescription = record.OsDescription,
            CreatedAt = record.CreatedAt,
            CpuUsagePercent =  record.CpuUsagePercent,
            StatisticsAt =  record.StatisticsAt,
            DiskAvailableBytes =  record.DiskAvailableBytes,
            MemoryTotalBytes =  record.MemoryTotalBytes,
            MemoryUsedBytes =  record.MemoryUsedBytes,
            ProcessorCount = record.ProcessorCount,
            LastHeartbeat = lastHeartbeatAt,
        };
    }
    
    private class HostRecord : JobMasterBaseModel
    {
        public HostRecord(string clusterId) : base(clusterId)
        {
        }
        
        protected HostRecord() {}
        
        public string Id { get; internal set; } = null!;
        public string ProcessId { get; set; } = null!;
        public int? ProcessorCount { get; set; }
        public string? OsDescription { get; set; }
        public string HostDisplayName { get; internal set; } = null!;
    
        public Guid LastStatsId { get; set; }
        public DateTime StatisticsAt { get; set; }
    
        public double? CpuUsagePercent { get; set; }
        public long? MemoryTotalBytes { get; set; }
        public long? MemoryUsedBytes { get; set; }
    
        public long? DiskAvailableBytes { get; set; }
        
        public DateTime CreatedAt { get; set; }
    }
}