using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Models.Buckets;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Hosts;
using JobMaster.Sdk.Abstractions.Models.Logs;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Models.RecurringSchedules;
using JobMaster.Sdk.Abstractions.Repositories.Master;
using JobMaster.Sdk.Abstractions.Services;
using JobMaster.Sdk.Abstractions.Services.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;
using JobMaster.Sdk.Utils;

namespace JobMaster.UnitTests.Background.Runners;

internal static class RunnerFakes
{
    // â”€â”€ FakeDistributedLocker â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeDistributedLocker : IMasterDistributedLockerService
    {
        private readonly Dictionary<string, (string Token, DateTime ExpiresAt)> _locks = new();

        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        /// <summary>
        /// When true, TryLock always returns null (simulates all keys pre-locked).
        /// </summary>
        public bool BlockAllLocks { get; set; } = false;

        public bool IsLocked(string key)
            => _locks.TryGetValue(key, out var entry) && entry.ExpiresAt > DateTime.UtcNow;

        public string? TryLock(string key, TimeSpan leaseDuration)
        {
            if (BlockAllLocks)
                return null;

            if (IsLocked(key))
                return null;

            var token = JobMasterRandomUtil.NewGuid4().ToString("N");
            _locks[key] = (token, DateTime.UtcNow.Add(leaseDuration));
            return token;
        }

        /// <summary>Pre-locks a key so IsLocked returns true without going through TryLock.</summary>
        public void ForceAddLock(string key, TimeSpan duration)
            => _locks[key] = (JobMasterRandomUtil.NewGuid4().ToString("N"), DateTime.UtcNow.Add(duration));

        public bool ReleaseLock(string key, string? lockToken)
        {
            if (_locks.TryGetValue(key, out var entry) && entry.Token == lockToken)
            {
                _locks.Remove(key);
                return true;
            }

            return false;
        }

        public bool ForceReleaseLock(string key)
            => _locks.Remove(key);
    }

    // â”€â”€ FakeAgentWorkersService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeAgentWorkersService : IMasterAgentWorkersService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<AgentWorkerModel> Workers { get; } = new();

        public IList<AgentWorkerModel> QueryWorkers(string? agentConnectionId = null, bool useCache = true)
            => agentConnectionId == null
                ? Workers
                : Workers.Where(w => w.AgentConnectionId?.IdValue == agentConnectionId).ToList();

        public Task<IList<AgentWorkerModel>> QueryWorkersAsync(string? agentConnectionId = null, bool useCache = true)
            => Task.FromResult<IList<AgentWorkerModel>>(QueryWorkers(agentConnectionId));

        public AgentWorkerModel? GetWorker(string workerId)
            => Workers.FirstOrDefault(w => w.Id == workerId);

        public Task<AgentWorkerModel?> GetWorkerAsync(string workerId)
            => Task.FromResult(GetWorker(workerId));

        public Task<(string workerId, HostId hostId)> RegisterWorkerAsync(
            string agentConnectionId, string? workerName, string? workerLane, AgentWorkerMode mode, double parallelismFactor)
            => throw new NotImplementedException();

        public void DeleteWorker(string workerId)
            => Workers.RemoveAll(w => w.Id == workerId);

        public Task DeleteWorkerAsync(string workerId)
        {
            DeleteWorker(workerId);
            return Task.CompletedTask;
        }

        public Task StopGracefulWorkerAsync(string workerId, TimeSpan? gracePeriod = null)
            => throw new NotImplementedException();
    }

    // â”€â”€ FakeJobsService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeJobsService : IMasterJobsService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<JobRawModel> Jobs { get; } = new();

        /// <summary>All BulkJobUpdateRequest calls recorded for assertion.</summary>
        public List<BulkJobUpdateRequest> BulkUpdateRequests { get; } = new();

        private static IEnumerable<JobRawModel> ApplyFilters(IEnumerable<JobRawModel> source, JobQueryCriteria c)
        {
            if (c.Status.HasValue)
                source = source.Where(j => j.Status == c.Status);

            if (c.Statuses.Count > 0)
                source = source.Where(j => c.Statuses.Contains(j.Status));

            if (c.ProcessDeadlineTo.HasValue)
                source = source.Where(j => j.ProcessDeadline.HasValue && j.ProcessDeadline <= c.ProcessDeadlineTo);

            if (c.BucketId != null)
                source = source.Where(j => j.BucketId == c.BucketId);

            if (c.ExcludeBucketIds.Count > 0)
                source = source.Where(j => j.BucketId == null || !c.ExcludeBucketIds.Contains(j.BucketId));

            if (c.SortBy != null)
            {
                source = c.SortBy.Property switch
                {
                    nameof(JobRawModel.ProcessDeadline) => c.SortBy.Ascending
                        ? source.OrderBy(j => j.ProcessDeadline)
                        : source.OrderByDescending(j => j.ProcessDeadline),
                    _ => source
                };
            }

            if (c.Offset > 0)
                source = source.Skip(c.Offset);

            return source.Take(c.CountLimit);
        }

        private static IEnumerable<JobRawModel> ApplyFiltersNoPage(IEnumerable<JobRawModel> source, JobQueryCriteria c)
        {
            if (c.Status.HasValue)
                source = source.Where(j => j.Status == c.Status);

            if (c.ProcessDeadlineTo.HasValue)
                source = source.Where(j => j.ProcessDeadline.HasValue && j.ProcessDeadline <= c.ProcessDeadlineTo);

            if (c.BucketId != null)
                source = source.Where(j => j.BucketId == c.BucketId);

            if (c.ExcludeBucketIds.Count > 0)
                source = source.Where(j => j.BucketId == null || !c.ExcludeBucketIds.Contains(j.BucketId));

            return source;
        }

        public long Count(JobQueryCriteria criteria)
            => ApplyFiltersNoPage(Jobs, criteria).LongCount();

        public Task<JobProbeResult> ProbeForBucketAssignmentAsync(JobQueryCriteria criteria)
        {
            var filtered = ApplyFiltersNoPage(Jobs, criteria);
            if (criteria.NextPlanExecutionAtTo.HasValue)
                filtered = filtered.Where(j => j.NextPlanExecutionAt <= criteria.NextPlanExecutionAtTo);
            var list = filtered.ToList();
            return Task.FromResult(new JobProbeResult
            {
                Count = list.Count,
                MinNextPlanExecutionAt = list.Count > 0 ? list.Min(j => j.NextPlanExecutionAt) : null,
            });
        }

        public IList<JobRawModel> Query(JobQueryCriteria criteria)
            => ApplyFilters(Jobs, criteria).ToList();

        public Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria criteria)
            => Task.FromResult<IList<JobRawModel>>(Query(criteria));

        public Task<IList<JobRawModel>> AcquireAndFetchAsync(JobQueryCriteria criteria, DateTime expiresAtUtc)
        {
            var fetched = ApplyFilters(Jobs, criteria).ToList();
            foreach (var job in fetched)
                job.ProcessDeadline = expiresAtUtc;
            return Task.FromResult<IList<JobRawModel>>(fetched);
        }

        public Task BulkUpdateAsync(BulkJobUpdateRequest request)
        {
            BulkUpdateRequests.Add(request);
            return Task.CompletedTask;
        }

        public Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobs)
        {
            foreach (var updated in jobs)
            {
                var idx = Jobs.FindIndex(j => j.Id == updated.Id);
                if (idx >= 0) Jobs[idx] = updated;
            }
            return Task.FromResult<IList<JobRawModel>>(jobs);
        }

        public List<JobExecution> JobExecutions { get; } = new();

        public void Add(JobRawModel jobRaw) => Jobs.Add(jobRaw);
        public Task AddAsync(JobRawModel jobRaw) { Jobs.Add(jobRaw); return Task.CompletedTask; }
        public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null) => throw new NotImplementedException();
        public Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null) => throw new NotImplementedException();
        public Task AddJobExecutionAsync(JobExecution jobExecution) { JobExecutions.Add(jobExecution); return Task.CompletedTask; }
        public Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId) => Task.FromResult<IList<JobExecution>>(JobExecutions.Where(e => e.JobId == jobId).ToList());
        public void ReleasePartitionLock(Guid jobId) => throw new NotImplementedException();
        public bool CheckVersion(Guid jobId, string? version) => throw new NotImplementedException();
        public Task<bool> CheckVersionAsync(Guid jobId, string? version) => throw new NotImplementedException();
        public JobRawModel? Get(Guid jobId) => Jobs.FirstOrDefault(j => j.Id == jobId);
        public Task<JobRawModel?> GetAsync(Guid jobId) => Task.FromResult(Get(jobId));
    }

    // â”€â”€ FakeBucketsService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeBucketsService : IMasterBucketsService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<BucketModel> Buckets { get; } = new();
        public List<string> DestroyedBucketIds { get; } = new();
        public List<BucketModel> UpdatedBuckets { get; } = new();

        public BucketModel? Get(string bucketId, TimeSpan? allowedDiscrepancy)
            => Buckets.FirstOrDefault(b => b.Id == bucketId);

        public IList<BucketModel> Query(MasterBucketQueryCriteria criteria, TimeSpan? allowedDiscrepancy = null)
        {
            IEnumerable<BucketModel> result = Buckets;

            if (criteria.Statuses.Count > 0)
                result = result.Where(b => criteria.Statuses.Contains(b.Status));

            if (criteria.Status.HasValue)
                result = result.Where(b => b.Status == criteria.Status);

            return result.ToList();
        }

        public Task<IList<BucketModel>> QueryAsync(MasterBucketQueryCriteria criteria, TimeSpan? allowedDiscrepancy = null)
            => Task.FromResult(Query(criteria, allowedDiscrepancy));

        public Task<IList<BucketModel>> QueryAllNoCacheAsync(BucketStatus? bucketStatus = null)
        {
            IEnumerable<BucketModel> result = Buckets;
            if (bucketStatus.HasValue)
                result = result.Where(b => b.Status == bucketStatus.Value);
            return Task.FromResult<IList<BucketModel>>(result.ToList());
        }

        public IList<BucketModel> QueryAllNoCache(BucketStatus? bucketStatus = null)
        {
            IEnumerable<BucketModel> result = Buckets;
            if (bucketStatus.HasValue)
                result = result.Where(b => b.Status == bucketStatus.Value);
            return result.ToList();
        }

        public Task UpdateAsync(BucketModel model)
        {
            UpdatedBuckets.Add(model);
            var idx = Buckets.FindIndex(b => b.Id == model.Id);
            if (idx >= 0) Buckets[idx] = model;
            else Buckets.Add(model);
            return Task.CompletedTask;
        }

        public Task DestroyAsync(string bucketId)
        {
            DestroyedBucketIds.Add(bucketId);
            Buckets.RemoveAll(b => b.Id == bucketId);
            return Task.CompletedTask;
        }

        public BucketModel? SelectBucket(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
            => Buckets.FirstOrDefault(b => b.Status == BucketStatus.Active);

        public Task<BucketModel?> SelectBucketAsync(TimeSpan? allowedDiscrepancy, JobMasterPriority? jobPriority = null, string? workerLane = null)
            => Task.FromResult(SelectBucket(allowedDiscrepancy, jobPriority, workerLane));

        public Task<BucketModel> CreateAsync(
            AgentConnectionId agentConnectionId, string workerId, JobMasterPriority priority, BucketType type = BucketType.Standard)
            => throw new NotImplementedException();

        public void Update(BucketModel model) => throw new NotImplementedException();

        public Task<int> CountAsync(MasterBucketQueryCriteria criteria) => throw new NotImplementedException();
        public int Count(MasterBucketQueryCriteria criteria) => throw new NotImplementedException();
    }

    // â”€â”€ FakeClusterConfigService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeClusterConfigService : IMasterClusterConfigurationService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public ClusterConfigurationModel? Config { get; set; }

        public ClusterConfigurationModel? Get() => Config;
        public ClusterConfigurationModel? GetFresh() => Config;
        public void Save(ClusterConfigurationModel cfg) => Config = cfg;
        public bool IsSaved() => Config != null;
    }

    // â”€â”€ FakeJobsRepository â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeJobsRepository : IMasterJobsRepository
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;
        public string MasterRepoTypeId { get; } = "fake";

        public List<JobRawModel> Jobs { get; } = new();

        public Task<int> PurgeFinalizedAsync(DateTime cutoffUtc, int limit)
        {
            var toRemove = Jobs
                .Where(j => j.Status.IsFinalStatus()
                         && j.FinalizedAt.HasValue
                         && j.FinalizedAt.Value <= cutoffUtc)
                .Take(limit)
                .ToList();

            foreach (var j in toRemove) Jobs.Remove(j);
            return Task.FromResult(toRemove.Count);
        }

        public List<JobExecution> JobExecutions { get; } = new();

        public void Add(JobRawModel jobRaw) => Jobs.Add(jobRaw);
        public Task AddAsync(JobRawModel jobRaw) { Jobs.Add(jobRaw); return Task.CompletedTask; }
        public JobRawModel? Get(Guid jobId) => Jobs.FirstOrDefault(j => j.Id == jobId);
        public Task<JobRawModel?> GetAsync(Guid jobId) => Task.FromResult(Get(jobId));

        public void Update(JobRawModel jobRaw, JobExecution? addJobExecution = null) => throw new NotImplementedException();
        public Task UpdateAsync(JobRawModel jobRaw, JobExecution? addJobExecution = null) => throw new NotImplementedException();
        public Task AddJobExecutionAsync(JobExecution jobExecution) { JobExecutions.Add(jobExecution); return Task.CompletedTask; }
        public Task<IList<JobExecution>> QueryJobExecutionsAsync(Guid jobId) => Task.FromResult<IList<JobExecution>>(JobExecutions.Where(e => e.JobId == jobId).ToList());
        public bool Exists(Guid jobId) => throw new NotImplementedException();
        public Task<bool> ExistsAsync(Guid jobId) => throw new NotImplementedException();
        public IList<JobRawModel> Query(JobQueryCriteria criteria) => throw new NotImplementedException();
        public Task<IList<JobRawModel>> QueryAsync(JobQueryCriteria criteria) => throw new NotImplementedException();
        public long Count(JobQueryCriteria criteria) => throw new NotImplementedException();
        public Task<JobProbeResult> ProbeForBucketAssignmentAsync(JobQueryCriteria criteria) => throw new NotImplementedException();
        public void ReleasePartitionLock(Guid jobId) => throw new NotImplementedException();
        public Task BulkUpdateAsync(BulkJobUpdateRequest request) => throw new NotImplementedException();
        public Task<IList<JobRawModel>> BulkUpdateAsync(IList<JobRawModel> jobRawModels) => throw new NotImplementedException();

        public Task<IList<JobRawModel>> AcquireAndFetchAsync(
            JobQueryCriteria criteria, Guid partitionLockId, DateTime expiresAtUtc)
            => throw new NotImplementedException();
    }

    // â”€â”€ FakeGenericRecordRepository â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeGenericRecordRepository : IMasterGenericRecordRepository
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;
        public string MasterRepoTypeId { get; } = "fake";

        public List<GenericRecordEntry> Records { get; } = new();

        /// <summary>Number of records to return from <see cref="DeleteExpiredAsync"/>.</summary>
        public int DeleteExpiredReturnValue { get; set; } = 0;

        /// <summary>Number of times <see cref="DeleteExpiredAsync"/> has been called.</summary>
        public int DeleteExpiredCallCount { get; private set; }

        public Task<int> DeleteByCreatedAtAsync(string groupId, DateTime createdAtTo, int limit)
        {
            var toRemove = Records
                .Where(r => r.GroupId == groupId && r.CreatedAt <= createdAtTo)
                .Take(limit)
                .ToList();

            foreach (var r in toRemove) Records.Remove(r);
            return Task.FromResult(toRemove.Count);
        }

        public Task<int> DeleteExpiredAsync(DateTime expiresAtTo, int limit)
        {
            DeleteExpiredCallCount++;
            return Task.FromResult(Math.Min(DeleteExpiredReturnValue, limit));
        }

        public GenericRecordEntry? Get(string groupId, string entryId, bool includeExpired = false)
            => throw new NotImplementedException();

        public Task<GenericRecordEntry?> GetAsync(string groupId, string entryId, bool includeExpired = false)
            => throw new NotImplementedException();

        public IList<GenericRecordEntry> Query(string groupId, GenericRecordQueryCriteria? criteria = null)
            => throw new NotImplementedException();

        public Task<IList<GenericRecordEntry>> QueryAsync(string groupId, GenericRecordQueryCriteria? criteria = null)
            => throw new NotImplementedException();

        public void Upsert(GenericRecordEntry recordEntry) => throw new NotImplementedException();
        public Task UpsertAsync(GenericRecordEntry recordEntry) => throw new NotImplementedException();
        public void Insert(GenericRecordEntry recordEntry) => throw new NotImplementedException();
        public Task InsertAsync(GenericRecordEntry recordEntry) => throw new NotImplementedException();
        public void Delete(string groupId, string id) => throw new NotImplementedException();
        public Task DeleteAsync(string groupId, string id) => throw new NotImplementedException();
        public Task BulkInsertAsync(IList<GenericRecordEntry> records) => throw new NotImplementedException();
        public int Count(string groupId, GenericRecordQueryCriteria? criteria = null) => throw new NotImplementedException();
        public Task<int> CountAsync(string groupId, GenericRecordQueryCriteria? criteria = null) => throw new NotImplementedException();
    }

    // â”€â”€ FakeLogsRepository â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeLogsRepository : IMasterLogsRepository
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;
        public string MasterRepoTypeId { get; } = "fake";

        public List<LogItem> Logs { get; } = new();

        public Task BulkInsertAsync(IList<LogItem> items)
        {
            Logs.AddRange(items);
            return Task.CompletedTask;
        }

        public Task<int> DeleteByTimestampAsync(DateTime timestampTo, int limit)
        {
            var toRemove = Logs
                .Where(r => r.TimestampUtc <= timestampTo)
                .Take(limit)
                .ToList();

            foreach (var r in toRemove) Logs.Remove(r);
            return Task.FromResult(toRemove.Count);
        }

        public Task<List<LogItem>> QueryAsync(LogItemQueryCriteria criteria) => throw new NotImplementedException();
        public Task<int> CountAsync(LogItemQueryCriteria criteria) => throw new NotImplementedException();
        public Task<LogItem?> GetAsync(Guid id) => throw new NotImplementedException();
    }

    // â”€â”€ FakeRecurringSchedulesRepository â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeRecurringSchedulesRepository : IMasterRecurringSchedulesRepository
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;
        public string MasterRepoTypeId { get; } = "fake";

        public List<RecurringScheduleRawModel> Schedules { get; } = new();

        public Task<int> PurgeTerminatedAsync(DateTime cutoffUtc, int limit)
        {
            var toRemove = Schedules
                .Where(s => s.TerminatedAt.HasValue && s.TerminatedAt.Value <= cutoffUtc)
                .Take(limit)
                .ToList();

            foreach (var s in toRemove) Schedules.Remove(s);
            return Task.FromResult(toRemove.Count);
        }

        public void Add(RecurringScheduleRawModel scheduleRaw) => throw new NotImplementedException();
        public Task AddAsync(RecurringScheduleRawModel scheduleRaw) => throw new NotImplementedException();
        public void Update(RecurringScheduleRawModel scheduleRaw) => throw new NotImplementedException();
        public Task UpdateAsync(RecurringScheduleRawModel scheduleRaw) => throw new NotImplementedException();
        public bool Exists(Guid recurringScheduleId) => throw new NotImplementedException();
        public Task<bool> ExistsAsync(Guid recurringScheduleId) => throw new NotImplementedException();
        public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();
        public Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();
        public RecurringScheduleRawModel? Get(Guid recurringScheduleId) => throw new NotImplementedException();
        public Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId) => throw new NotImplementedException();
        public RecurringScheduleRawModel? GetByStaticId(string staticId) => throw new NotImplementedException();

        public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(
            RecurringScheduleQueryCriteria queryCriteria, Guid partitionLockId, DateTime expiresAtUtc)
            => throw new NotImplementedException();

        public long Count(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();

        public void BulkUpdateStaticDefinitionLastEnsuredByStaticIds(IList<string> staticDefinitionIds, DateTime ensuredAt)
            => throw new NotImplementedException();

        public Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
            => throw new NotImplementedException();
    }

    // â”€â”€ FakeAgentJobsDispatcherService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeAgentJobsDispatcherService : IAgentJobsDispatcherService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        /// <summary>Jobs waiting to be pulled (keyed by BucketId).</summary>
        public List<JobRawModel> PendingQueue { get; } = new();

        /// <summary>Jobs that were re-queued by the runner on processing failure.</summary>
        public List<JobRawModel> RequeuedJobs { get; } = new();

        /// <summary>Recurring schedules waiting to be pulled.</summary>
        public List<RecurringScheduleRawModel> PendingRecurQueue { get; } = new();

        /// <summary>Recurring schedules that were re-queued by the runner on processing failure.</summary>
        public List<RecurringScheduleRawModel> RequeuedRecurSchedules { get; } = new();

        /// <summary>Return value for <see cref="HasJobsAsync"/>.</summary>
        public bool HasJobsResult { get; set; } = false;

        public Task<IList<JobRawModel>> PullSavePendingJobsAsync(
            AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs)
        {
            var pulled = PendingQueue
                .Where(j => j.BucketId == bucketId)
                .Take(numberOfJobs)
                .ToList();

            foreach (var j in pulled) PendingQueue.Remove(j);
            return Task.FromResult<IList<JobRawModel>>(pulled);
        }

        public string AddSavePendingJob(JobRawModel jobRaw, bool notifyAgent = true) { PendingQueue.Add(jobRaw); return "ok"; }

        public Task<string> AddSavePendingJobAsync(JobRawModel jobRaw, bool notifyAgent = true)
        {
            RequeuedJobs.Add(jobRaw);
            return Task.FromResult("ok");
        }

        public Task<IList<string>> BulkAddSavePendingJobAsync(List<JobRawModel> jobRawModels)
            => throw new NotImplementedException();

        public string AddSavePendingRecur(RecurringScheduleRawModel recurringScheduleRaw)
            => throw new NotImplementedException();

        public Task<string> AddSavePendingRecurAsync(RecurringScheduleRawModel recurringScheduleRaw)
        {
            RequeuedRecurSchedules.Add(recurringScheduleRaw);
            return Task.FromResult("ok");
        }

        public Task<string> AddForProcessingAsync(JobRawModel jobRaw)
            => throw new NotImplementedException();

        public Task<IList<JobRawModel>> PullForProcessingAsync(
            AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs, DateTime? scheduleTo)
            => throw new NotImplementedException();

        public Task<IList<RecurringScheduleRawModel>> PullSavePendingRecurAsync(
            AgentConnectionId agentConnectionId, string bucketId, int numberOfJobs)
        {
            var pulled = PendingRecurQueue.Take(numberOfJobs).ToList();
            foreach (var s in pulled) PendingRecurQueue.Remove(s);
            return Task.FromResult<IList<RecurringScheduleRawModel>>(pulled);
        }

        public Task<bool> HasJobsAsync(AgentConnectionId agentConnectionId, string bucketId)
            => Task.FromResult(HasJobsResult);

        public Task CreateBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
            => throw new NotImplementedException();

        public Task DestroyBucketAsync(AgentConnectionId agentConnectionId, string bucketId)
            => throw new NotImplementedException();
    }

    // â”€â”€ FakeHeartbeatService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeHeartbeatService : IMasterHeartbeatService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        /// <summary>All heartbeat calls recorded in order.</summary>
        public List<(ResourceHeartbeatType Type, string ResourceId)> HeartbeatCalls { get; } = new();

        public void Heartbeat(ResourceHeartbeatType type, string resourceId)
            => HeartbeatCalls.Add((type, resourceId));

        public DateTime? GetLastHeartbeat(ResourceHeartbeatType type, string resourceId) => null;

        public IDictionary<string, DateTime?> GetLastHeartbeats(ResourceHeartbeatType type, IList<string> resourceIds)
            => new Dictionary<string, DateTime?>();
    }

    // â”€â”€ FakeHostService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeHostService : IMasterHostService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<HostModel> Hosts { get; } = new();
        public List<string> DeletedHostIds { get; } = new();
        public int UpdateStatsCallCount { get; private set; }

        public Task<IList<HostModel>> QueryAllAsync()
            => Task.FromResult<IList<HostModel>>(Hosts);

        public Task DeleteHostsAsync(IList<string> hostIds)
        {
            Hosts.RemoveAll(h => hostIds.Contains(h.Id.IdValue));
            DeletedHostIds.AddRange(hostIds);
            return Task.CompletedTask;
        }

        public Task UpdateStatsAsync(string hostId)
        {
            UpdateStatsCallCount++;
            return Task.CompletedTask;
        }

        public Task<HostId> RegisterNewHostAsync() => throw new NotImplementedException();
    }

    // â”€â”€ FakeAgentConnectionService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeAgentConnectionService : IMasterAgentConnectionService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<AgentConnectionModel> Connections { get; } = new();
        public List<string> DeletedConnectionIds { get; } = new();

        /// <summary>Key = AgentConnectionId.IdValue â†’ whether connection owns buckets.</summary>
        public Dictionary<string, bool> HasBucketsMap { get; } = new();

        public Task<IList<AgentConnectionModel>> QueryAllAsync(bool useCache = true)
            => Task.FromResult<IList<AgentConnectionModel>>(Connections);

        public Task<bool> SafeDeleteConnectionAsync(AgentConnectionId agentConnectionId)
        {
            var removed = Connections.RemoveAll(c => c.Id.IdValue == agentConnectionId.IdValue) > 0;
            if (removed)
                DeletedConnectionIds.Add(agentConnectionId.IdValue);
            return Task.FromResult(removed);
        }

        public Task<bool> HasBucketsAsync(AgentConnectionId agentConnectionId)
        {
            HasBucketsMap.TryGetValue(agentConnectionId.IdValue, out var hasBuckets);
            return Task.FromResult(hasBuckets);
        }

        public Task<AgentConnectionModel> SaveConnectionAsync(
            AgentConnectionId agentConnectionId, string repositoryTypeId, string fingerprint, bool protectChanges)
            => throw new NotImplementedException();

        public Task<AgentConnectionModel?> GetConnectionAsync(AgentConnectionId agentConnectionId, bool useCache = true)
            => throw new NotImplementedException();
    }

    // â”€â”€ FakeMasterRecurringSchedulesService â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeMasterRecurringSchedulesService : IMasterRecurringSchedulesService
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<RecurringScheduleRawModel> Schedules { get; } = new();

        /// <summary>All BulkUpdateStaticDefinitionLastEnsured calls recorded for assertion.</summary>
        public List<(IList<string> Ids, DateTime EnsuredAt)> BulkUpdateLastEnsuredCalls { get; } = new();

        /// <summary>Number of times InactivateStaticDefinitionsOlderThanAsync was called.</summary>
        public int InactivateCallCount { get; private set; }

        public long Count(RecurringScheduleQueryCriteria queryCriteria)
            => ApplyFilters(Schedules, queryCriteria).LongCount();

        public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchAsync(
            RecurringScheduleQueryCriteria queryCriteria, DateTime expiresAtUtc)
            => Task.FromResult<IList<RecurringScheduleRawModel>>(
                ApplyFilters(Schedules, queryCriteria).ToList());

        public Task<IList<RecurringScheduleRawModel>> AcquireAndFetchByIdsAsync(
            IList<Guid> ids, DateTime expiresAtUtc)
            => Task.FromResult<IList<RecurringScheduleRawModel>>(
                Schedules.Where(s => ids.Contains(s.Id)).ToList());

        public void BulkUpdateStaticDefinitionLastEnsured(IList<string> staticDefinitionIds, DateTime ensuredAt)
            => BulkUpdateLastEnsuredCalls.Add((staticDefinitionIds, ensuredAt));

        public Task<int> InactivateStaticDefinitionsOlderThanAsync(DateTime cutoff)
        {
            InactivateCallCount++;
            return Task.FromResult(0);
        }

        private static IEnumerable<RecurringScheduleRawModel> ApplyFilters(
            IEnumerable<RecurringScheduleRawModel> source, RecurringScheduleQueryCriteria c)
        {
            if (c.Status.HasValue)
                source = source.Where(s => s.Status == c.Status);

            if (c.CanceledOrInactive == true)
                source = source.Where(s =>
                    s.Status == RecurringScheduleStatus.Inactive ||
                    s.Status == RecurringScheduleStatus.Canceled);

            if (c.IsJobCancellationPending == true)
                source = source.Where(s => s.IsJobCancellationPending == true);

            if (c.CoverageUntil.HasValue)
                source = source.Where(s =>
                    s.LastPlanCoverageUntil == null ||
                    s.LastPlanCoverageUntil <= c.CoverageUntil);

            return source.Take(c.CountLimit);
        }

        public Task UpdateAsync(RecurringScheduleRawModel scheduleRaw)
        {
            var idx = Schedules.FindIndex(s => s.Id == scheduleRaw.Id);
            if (idx >= 0) Schedules[idx] = scheduleRaw;
            return Task.CompletedTask;
        }

        public void Update(RecurringScheduleRawModel scheduleRaw)
        {
            var idx = Schedules.FindIndex(s => s.Id == scheduleRaw.Id);
            if (idx >= 0) Schedules[idx] = scheduleRaw;
        }

        public Task AddAsync(RecurringScheduleRawModel scheduleRaw)
        {
            Schedules.Add(scheduleRaw);
            return Task.CompletedTask;
        }

        public void Add(RecurringScheduleRawModel scheduleRaw)
        {
            Schedules.Add(scheduleRaw);
        }

        public void UpsertStatic(StaticRecurringScheduleDefinition definition) => throw new NotImplementedException();
        public IList<RecurringScheduleRawModel> Query(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();
        public Task<IList<RecurringScheduleRawModel>> QueryAsync(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();
        public Task<IList<Guid>> QueryIdsAsync(RecurringScheduleQueryCriteria queryCriteria) => throw new NotImplementedException();
        public RecurringScheduleRawModel? Get(Guid recurringScheduleId) => throw new NotImplementedException();
        public Task<RecurringScheduleRawModel?> GetAsync(Guid recurringScheduleId) => throw new NotImplementedException();
    }

    // â”€â”€ FakeRecurringSchedulePlanner â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeRecurringSchedulePlanner : IRecurringSchedulePlanner
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        /// <summary>All schedules that were planned, in order.</summary>
        public List<RecurringScheduleRawModel> PlannedSchedules { get; } = new();

        public Task ScheduleNextJobsAsync(RecurringScheduleRawModel schedule)
        {
            PlannedSchedules.Add(schedule);
            return Task.CompletedTask;
        }
    }

    // â”€â”€ FakeRecentlyInsertedRecurringScheduleQueue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    internal sealed class FakeRecentlyInsertedRecurringScheduleQueue : IRecentlyInsertedRecurringScheduleQueue
    {
        public JobMasterClusterConnectionConfig ClusterConnConfig { get; } = null!;

        public List<Guid> Queue { get; } = new();

        public void Enqueue(Guid scheduleId)
            => Queue.Add(scheduleId);

        public IReadOnlyList<Guid> Dequeue(int maxCount)
        {
            var result = Queue.Take(maxCount).ToList();
            foreach (var id in result) Queue.Remove(id);
            return result;
        }
    }
}
