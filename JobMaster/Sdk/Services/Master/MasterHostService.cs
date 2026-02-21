using System.Diagnostics;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Ioc.Markups;
using JobMaster.Sdk.Utils.Extensions;
using System.Runtime;
using JobMaster.Sdk.Abstractions.Keys;
using JobMaster.Sdk.Abstractions.LocalCache;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Services.Master;


internal class MasterHostService : JobMasterClusterAwareComponent, IMasterHostService
{
    private readonly IMasterGenericRecordRepository masterGenericRecordRepository;
    private readonly IMasterHeartbeatService masterHeartbeatService;
    private readonly IMasterChangesSentinelService masterChangesSentinelService;
    private readonly JobMasterInMemoryKeys cacheKeys;
    private readonly JobMasterSentinelKeys sentinelKeys;
    private readonly IJobMasterInMemoryCache jobMasterInMemoryCache;

    public MasterHostService(
        JobMasterClusterConnectionConfig clusterConnConfig,
        IMasterGenericRecordRepository masterGenericRecordRepository,
        IMasterHeartbeatService masterHeartbeatService,
        IJobMasterInMemoryCache jobMasterInMemoryCache,
        IMasterChangesSentinelService masterChangesSentinelService) : base(clusterConnConfig)
    {
        this.masterGenericRecordRepository = masterGenericRecordRepository;
        this.masterHeartbeatService = masterHeartbeatService;
        this.jobMasterInMemoryCache = jobMasterInMemoryCache;
        this.masterChangesSentinelService = masterChangesSentinelService;
        
        cacheKeys = new JobMasterInMemoryKeys(clusterConnConfig.ClusterId);
        sentinelKeys = new JobMasterSentinelKeys(clusterConnConfig.ClusterId);
    }

    public async Task<IList<HostModel>> QueryAllAsync()
    {
        var allHostRecords = await QueryAllRecordsAsync();

        var resourceIds = allHostRecords.Select(x => x.Id).ToList();
        var allHeartbeats =
            masterHeartbeatService.GetLastHeartbeats(ResourceHeartbeatType.Host, resourceIds);

        return allHostRecords.Select(x =>
        {
            var lastHeartbeat = allHeartbeats.GetOrDefault(x.Id) ?? x.CreatedAt;
            return ToHostModel(lastHeartbeat, x);
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
            ProcessorCount =  Environment.ProcessorCount,
            CpuUsagePercent = hostStats.CpuUsagePercent,
            DiskAvailableBytes = hostStats.DiskAvailableBytes,
            GcTotalMemory = hostStats.GcTotalMemory,
            HandleCount = hostStats.HandleCount,
            MemoryTotalBytes = hostStats.MemoryTotalBytes,
            MemoryUsedBytes = hostStats.MemoryUsedBytes,
            ThreadCount = hostStats.ThreadCount,
            LastStatsId = hostStats.Id,
        };
        
        var genericRecord = 
            GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Host, record.Id, record); 
        
        await masterGenericRecordRepository.InsertAsync(genericRecord);
        
        await InsertStatsAsync(hostStats);
        
        this.masterChangesSentinelService.NotifyChanges(sentinelKeys.Hosts());

        return hostId;
    }

    public async Task<IList<HostStatsModel>> QueryAllStatsAsync(string hostId)
    {
        var queryCriteria = new GenericRecordQueryCriteria()
        {
            Filters = new List<GenericRecordValueFilter>()
            {
                new()
                {
                    Key = nameof(HostStatsModel.HostId),
                    Operation = GenericFilterOperation.Eq,
                    Value = hostId,
                }
            },
            ReadIsolationLevel = ReadIsolationLevel.FastSync,
            OrderBy = GenericRecordQueryOrderByTypeId.CreatedAtDesc,
        };
        
        var hostStatsRecords = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.HostStats, queryCriteria);
        return hostStatsRecords.Select(x => x.ToObject<HostStatsModel>()).ToList();
    }

    public async Task AddStatsAsync(string hostId)
    {
        var hosts = await QueryAllRecordsAsync();
        var host = hosts.FirstOrDefault(x => x.Id == hostId);
        if (host == null)
        {
            return;
        }
            
        var hostStats = await CaptureStatsAsync(HostId.Recover(host.HostDisplayName, hostId));
        await InsertStatsAsync(hostStats);

        host.CpuUsagePercent = hostStats.CpuUsagePercent;
        host.DiskAvailableBytes = hostStats.DiskAvailableBytes;
        host.GcTotalMemory = hostStats.GcTotalMemory;
        host.HandleCount = hostStats.HandleCount;
        host.MemoryTotalBytes = hostStats.MemoryTotalBytes;
        host.MemoryUsedBytes = hostStats.MemoryUsedBytes;
        host.ThreadCount = hostStats.ThreadCount;
        host.LastStatsId = hostStats.Id;
        host.StatisticsAt = hostStats.StatisticsAt;
        
        var genericRecord = 
            GenericRecordEntry.Create(ClusterConnConfig.ClusterId, MasterGenericRecordGroupIds.Host, host.Id, host); 
        
        await masterGenericRecordRepository.UpdateAsync(genericRecord);
        
        masterHeartbeatService.Heartbeat(ResourceHeartbeatType.Host, hostId);
    }

    public async Task DeleteHostsAsync(IList<string> hostIds)
    {
        foreach (var hostId in hostIds)
        {
            await this.masterGenericRecordRepository.DeleteAsync(MasterGenericRecordGroupIds.Host, hostId);
        }
    }

    private async Task<IList<HostRecord>> QueryAllRecordsAsync()
    {
        var cacheKey = cacheKeys.AllHosts();
        var sentinelKey = sentinelKeys.Hosts();

        var inCacheValue = jobMasterInMemoryCache.Get<IList<HostRecord>>(cacheKey);
        if (inCacheValue == null ||
            inCacheValue.Value == null ||
            masterChangesSentinelService.HasChangesAfter(sentinelKey, inCacheValue.CreatedAt, allowedDiscrepancy: TimeSpan.FromSeconds(10)))
        {
            var allHostRecords = await masterGenericRecordRepository.QueryAsync(MasterGenericRecordGroupIds.Host);
            var allHosts = allHostRecords.Select(x => x.ToObject<HostRecord>()).ToList();

            jobMasterInMemoryCache.Set(cacheKey, allHosts);
            return allHosts;
        }

        return inCacheValue.Value;
    }
    

    private async Task InsertStatsAsync(HostStatsModel hostStats)
    {
        var statsGenericRecord = 
            GenericRecordEntry.Create(
                ClusterConnConfig.ClusterId, 
                MasterGenericRecordGroupIds.HostStats, 
                hostStats.Id, 
                hostStats,
                expiresAt: DateTime.UtcNow.AddHours(1));

        await masterGenericRecordRepository.InsertAsync(statsGenericRecord);
    }
    
    private async Task<HostStatsModel> CaptureStatsAsync(HostId hostId, Process? process = null)
    {
        process ??= Process.GetCurrentProcess();
        var cpuUsage = await CpuUsageUtil.GetProcessCpuPercentTotalAsync(process);
        
        var hostStats = new HostStatsModel(ClusterConnConfig.ClusterId)
        {
            Id = Guid.NewGuid(),
            HostId = hostId,
            StatisticsAt = DateTime.UtcNow,
            CpuUsagePercent = cpuUsage,
#if NET8_0_OR_GREATER
            MemoryTotalBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
#else
            MemoryTotalBytes = -1,
#endif
            MemoryUsedBytes = process.WorkingSet64,
            ThreadCount = process.Threads.Count,
            HandleCount = process.HandleCount,
            GcTotalMemory = GC.GetTotalMemory(false),
            DiskAvailableBytes = new DriveInfo(Environment.CurrentDirectory).AvailableFreeSpace,
        };

        return hostStats;
    }
    
    private static HostModel ToHostModel(DateTime lastHeartbeatAt, HostRecord record)
    {
        var hostId = HostId.Recover(record.HostDisplayName, record.Id);
        return new HostModel(record.ClusterId)
        {
            Id = hostId,
            ProcessId = record.ProcessId,
            ProcessorCount = record.ProcessorCount,
            OsDescription = record.OsDescription,
            CreatedAt = record.CreatedAt,
            LastStats = new HostStatsModel(record.ClusterId)
            {
                CpuUsagePercent =  record.CpuUsagePercent,
                StatisticsAt =  record.StatisticsAt,
                DiskAvailableBytes =  record.DiskAvailableBytes,
                GcTotalMemory =  record.GcTotalMemory,
                HandleCount =  record.HandleCount,
                MemoryTotalBytes =  record.MemoryTotalBytes,
                MemoryUsedBytes =  record.MemoryUsedBytes,
                ThreadCount =  record.ThreadCount,
                HostId = hostId,
                Id = record.LastStatsId,
            },
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
        public int ProcessorCount { get; set; }
        public string? OsDescription { get; set; }
        public string HostDisplayName { get; internal set; } = null!;
    
        public Guid LastStatsId { get; set; }
        public DateTime StatisticsAt { get; set; }
    
        public double? CpuUsagePercent { get; set; }
        public long? MemoryTotalBytes { get; set; }
        public long? MemoryUsedBytes { get; set; }
    
        public int ThreadCount { get; set; }
        public int HandleCount { get; set; }
   
        public long GcTotalMemory { get; set; }
    
        public long? DiskAvailableBytes { get; set; }
        
        public DateTime CreatedAt { get; set; }
    }
}