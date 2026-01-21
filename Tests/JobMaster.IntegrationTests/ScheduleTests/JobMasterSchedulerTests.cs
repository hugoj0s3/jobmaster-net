using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.IntegrationTests.Fixtures.SchedulerFixture;
using JobMaster.IntegrationTests.Utils;
using JobMaster.Internals;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Models.GenericRecords;
using JobMaster.Sdk.Abstractions.Models.Jobs;
using JobMaster.Sdk.Abstractions.Services.Master;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace JobMaster.IntegrationTests.ScheduleTests;

public abstract class JobMasterSchedulerTestsBase<TFixture> : IClassFixture<TFixture>
    where TFixture : JobMasterBaseSchedulerFixture
{
    private readonly JobMasterBaseSchedulerFixture fixture;
    private readonly ITestOutputHelper output;

    protected JobMasterSchedulerTestsBase(TFixture fixture, ITestOutputHelper output)
    {
        this.fixture = fixture;
        this.output = output;
    }
    
    
    protected async Task RunDrainModeTest(
        int qtyJobs,
        int timeoutInMinutes,
        int secondsToStopWorkers,
        int scheduleParallelLimit = 10)
    {
        var fromTimestamp = DateTime.UtcNow;
        var testExecutionId = Guid.NewGuid().ToString("N");
        fixture.CurrentTestExecutionId = testExecutionId;
        
        var sessionMetadataFilters = new List<GenericRecordValueFilter>()
        {
            new GenericRecordValueFilter()
            {
                Key = "TestExecutionId",
                Value = testExecutionId,
                Operation = GenericFilterOperation.Eq,
            }
        };
        
        await Task.Delay(1000);
        
        string? logRootDir = null;
        
        try
        {
            var runtime = JobMasterRuntimeSingleton.Instance;
            var workers = runtime.GetAllWorkers();
            var drainWorkers = workers.Where(w => w.Mode == AgentWorkerMode.Drain).ToList();
            var coordinatorWorkers = workers.Where(w => w.Mode == AgentWorkerMode.Coordinator).ToList();
            var normalWorkers = workers.Where(w => w.Mode != AgentWorkerMode.Drain && w.Mode != AgentWorkerMode.Coordinator).ToList();
            
            // Identify one survivor worker per cluster
            var workersToStop = normalWorkers;
            
            output.WriteLine($"Total workers: {workers.Count} (Drain: {drainWorkers.Count}, Normal: {normalWorkers.Count}, Coordinator: {coordinatorWorkers.Count},  ToStop: {workersToStop.Count})");
            
            var scheduler = fixture.Services.GetRequiredService<IJobMasterScheduler>();
            Assert.NotNull(runtime);

            // Schedule all jobs and stop workers randomly in the background
            var semaphore = new SemaphoreSlim(scheduleParallelLimit);
            var qtys = RandomQtys(qtyJobs);
            var expectedTotal = qtys.Sum(x => x.QtyJobs);
            var schedulingStopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            output.WriteLine($"Scheduling {expectedTotal} jobs across {qtys.Count} configurations...");
            output.WriteLine($"Workers will be stopped randomly after {secondsToStopWorkers} seconds during scheduling...");

            // Start background task to stop workers in two phases (time-based, not waiting for scheduling)
            var workerStopTask = Task.Run(async () =>
            {
                var shuffledWorkers = workersToStop.OrderBy(_ => Guid.NewGuid()).ToList();
                var firstBatchCount = (int)(shuffledWorkers.Count * 0.90); // 90% of workers
                var firstBatch = shuffledWorkers.Take(firstBatchCount).ToList();
                var secondBatch = shuffledWorkers.Skip(firstBatchCount).ToList();
                
                // Phase 1: Wait secondsToStopWorkers, then stop 90%
                output.WriteLine($"Phase 1: Waiting {secondsToStopWorkers}s before stopping {firstBatch.Count} workers (90%)...");
                await Task.Delay(TimeSpan.FromSeconds(secondsToStopWorkers));
                
                output.WriteLine($"Starting Phase 1: Stopping {firstBatch.Count} workers...");
                foreach (var worker in firstBatch)
                {
                    output.WriteLine($"  Stopping worker: {worker.AgentWorkerId}, Cluster: {worker.ClusterConnConfig.ClusterId}");
                    _ = worker.StopImmediatelyAsync(); // Fire and forget
                    
                    // Quick succession for first batch (100-500ms between stops)
                    var delayMs = Random.Shared.Next(100, 500);
                    await Task.Delay(delayMs);
                }
                
                // Phase 2: Wait secondsToStopWorkers * 2, then stop remaining 10%
                var phase2Delay = secondsToStopWorkers * 2;
                output.WriteLine($"Phase 1 complete. Waiting {phase2Delay}s before Phase 2...");
                await Task.Delay(TimeSpan.FromSeconds(phase2Delay));
                
                output.WriteLine($"Starting Phase 2: Stopping remaining {secondBatch.Count} workers (10%)...");
                foreach (var worker in secondBatch)
                {
                    output.WriteLine($"  Stopping worker: {worker.AgentWorkerId}, Cluster: {worker.ClusterConnConfig.ClusterId}");
                    _ = worker.StopImmediatelyAsync(); // Fire and forget
                    
                    // Slower for second batch (2-3 seconds between stops)
                    var delayMs = JobMasterRandomUtil.GetInt(2000, 3000);
                    await Task.Delay(delayMs);
                }
                
                output.WriteLine("All workers stop commands issued.");
            });

            foreach (var qty in qtys)
            {
                for (int i = 0; i < qty.QtyJobs; i++)
                {
                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync();
                        try
                        {
                            var afterSeconds = JobMasterRandomUtil.GetInt(1, 120);
                            var metadata = WritableMetadata.New();
                            metadata.SetStringValue("TestExecutionId", testExecutionId);
                            await scheduler.OnceAfterAsync<JobHandlerForTests>(
                                TimeSpan.FromSeconds(afterSeconds),
                                metadata: metadata,
                                clusterId: qty.ClusterId,
                                workerLane: qty.WorkerLane,
                                priority: qty.Priority);
                        }
                        finally
                        {
                            semaphore.Release();
                            await Task.Delay(50);
                        }
                    }));
                }
            }

            await Task.WhenAll(tasks);
            schedulingStopwatch.Stop();
            output.WriteLine($"Scheduling completed in {schedulingStopwatch.Elapsed}");

            // Wait for worker stop task to complete
            output.WriteLine("Waiting for all worker stop commands to complete...");
            await workerStopTask;
            
            output.WriteLine("Waiting for drain to complete...");

            // Poll job statuses until all jobs are in final states or timeout is reached
            var timeoutAt = DateTime.UtcNow.AddMinutes(timeoutInMinutes);
            var checkInterval = TimeSpan.FromSeconds(60);
            var drainStopwatch = Stopwatch.StartNew();
            
            Dictionary<string, (int succeeded, int heldOnMaster, int other)> validationResults;
            int totalSucceeded = 0;
            int totalHeldOnMaster = 0;
            int totalOther = 0;
            int totalInDb = 0;
            
            output.WriteLine($"Starting drain polling. Timeout at {timeoutAt:HH:mm:ss}, checking every {checkInterval.TotalSeconds}s");
            
            while (true)
            {
                var now = DateTime.UtcNow;
                if (now >= timeoutAt)
                {
                    output.WriteLine($"Timeout reached after {drainStopwatch.Elapsed}");
                    break;
                }
                
                var breakFlag = true;
                var clusterProgress = new List<string>();
                
                foreach (var clusterId in fixture.ClusterIds)
                {
                    var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                    var masterJobsService = factory.GetComponent<IMasterJobsService>();

                    var countHeldOnMaster = masterJobsService.Count(new JobQueryCriteria()
                    {
                        Status = JobMasterJobStatus.HeldOnMaster, 
                        MetadataFilters = sessionMetadataFilters,
                    });
                    
                    var countSucceeded = masterJobsService.Count(new JobQueryCriteria() { 
                        Status = JobMasterJobStatus.Succeeded, 
                        MetadataFilters = sessionMetadataFilters,
                    });
                    
                    var expectedTotalForCluster = qtys.Where(x => x.ClusterId == clusterId).Sum(x => x.QtyJobs);
                    var totalForCluster = countHeldOnMaster + countSucceeded;
                    
                    clusterProgress.Add($"{clusterId}: {totalForCluster}/{expectedTotalForCluster} (S={countSucceeded}, H={countHeldOnMaster})");
                    
                    if (totalForCluster != expectedTotalForCluster)
                    {
                        breakFlag = false;
                    }
                    
                    await Task.Delay(50);
                }

                output.WriteLine($"[{drainStopwatch.Elapsed:hh\\:mm\\:ss}] {string.Join(" | ", clusterProgress)}");

                if (breakFlag)
                {
                    output.WriteLine($"All jobs completed in {drainStopwatch.Elapsed}");
                    break;
                }

                await Task.Delay(checkInterval);
            }
            
            drainStopwatch.Stop();
            
            validationResults = new Dictionary<string, (int succeeded, int heldOnMaster, int other)>();
                
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();

                // Query all jobs for this cluster
                var allJobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });

                var succeeded = allJobs.Count(j => j.Status == JobMasterJobStatus.Succeeded);
                var heldOnMaster = allJobs.Count(j => j.Status == JobMasterJobStatus.HeldOnMaster);
                var other = allJobs.Count(j => j.Status != JobMasterJobStatus.Succeeded && j.Status != JobMasterJobStatus.HeldOnMaster);

                validationResults[clusterId] = (succeeded, heldOnMaster, other);
            }

            totalSucceeded = validationResults.Sum(x => x.Value.succeeded);
            totalHeldOnMaster = validationResults.Sum(x => x.Value.heldOnMaster);
            totalOther = validationResults.Sum(x => x.Value.other);
            totalInDb = totalSucceeded + totalHeldOnMaster + totalOther;

            output.WriteLine($"[{drainStopwatch.Elapsed:hh\\:mm\\:ss}] Drain progress: Succeeded={totalSucceeded}, HeldOnMaster={totalHeldOnMaster}, Other={totalOther}, Total={totalInDb}/{expectedTotal}");

            // Check if all jobs are in final states
            if (totalOther == 0 && totalInDb == expectedTotal)
            {
                output.WriteLine($"Drain completed successfully in {drainStopwatch.Elapsed}");
                return;
            }

            // Final status report per cluster
            foreach (var clusterId in fixture.ClusterIds)
            {
                if (validationResults.TryGetValue(clusterId, out var result))
                {
                    output.WriteLine($"Cluster {clusterId}: Succeeded={result.succeeded}, HeldOnMaster={result.heldOnMaster}, Other={result.other}");
                }
            }

            // Get execution count for this test
            var executionCount = JobHandlerForTests.GetExecutionCount(testExecutionId);
            Assert.NotNull(executionCount);
            
            output.WriteLine("==== Drain Mode Test Report ====");
            output.WriteLine($"TestExecutionId={testExecutionId}");
            output.WriteLine($"ExpectedTotal={expectedTotal}, TotalInDb={totalInDb}");
            output.WriteLine($"Succeeded={totalSucceeded}, HeldOnMaster={totalHeldOnMaster}, Other={totalOther}");
            output.WriteLine($"JobHandlerExecuted={executionCount.JobExecutionCounts.Count}");
            output.WriteLine($"TotalExecuted={executionCount.TotalExecuted}");
            output.WriteLine($"TotalDuplicates={executionCount.TotalDuplicates}");
            output.WriteLine("================================");

            // ASSERTION 1: Total jobs in DB must match qtyJobs parameter
            Assert.Equal(qtyJobs, totalInDb);
            output.WriteLine($"✓ Total jobs in DB ({totalInDb}) matches expected ({qtyJobs})");
            
            // ASSERTION 2: Total per cluster must match qtys variable
            foreach (var clusterId in fixture.ClusterIds)
            {
                var expectedForCluster = qtys.Where(x => x.ClusterId == clusterId).Sum(x => x.QtyJobs);
                var actualForCluster = validationResults[clusterId].succeeded + 
                                      validationResults[clusterId].heldOnMaster + 
                                      validationResults[clusterId].other;
                Assert.Equal(expectedForCluster, actualForCluster);
                output.WriteLine($"✓ Cluster {clusterId}: DB count ({actualForCluster}) matches expected ({expectedForCluster})");
            }
            
            // ASSERTION 3: No jobs in other statuses (only Succeeded or HeldOnMaster allowed)
            Assert.Equal(0, totalOther);
            output.WriteLine($"✓ No jobs in intermediate states (Other={totalOther})");
            
            // ASSERTION 4: Total executed matches succeeded count
            Assert.Equal(totalSucceeded, executionCount.TotalExecuted);
            output.WriteLine($"✓ Total executed ({executionCount.TotalExecuted}) matches succeeded ({totalSucceeded})");
            
            // ASSERTION 5: Unique jobs executed matches succeeded count
            Assert.Equal(totalSucceeded, executionCount.JobExecutionCounts.Count);
            output.WriteLine($"✓ Unique jobs executed ({executionCount.JobExecutionCounts.Count}) matches succeeded ({totalSucceeded})");
            
            // ASSERTION 6: All succeeded job IDs must be in JobExecuted dictionary
            var allSucceededJobs = new List<Guid>();
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();
                var succeededJobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    Status = JobMasterJobStatus.Succeeded,
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });
                allSucceededJobs.AddRange(succeededJobs.Select(j => j.Id));
            }
            
            var missingFromExecuted = allSucceededJobs.Where(id => !executionCount.JobExecutionCounts.ContainsKey(id)).ToList();
            Assert.Empty(missingFromExecuted);
            output.WriteLine($"✓ All {allSucceededJobs.Count} succeeded job IDs are in execution count dictionary");
            
            // ASSERTION 7: No duplicate executions (all JobExecutionCounts values should be 1)
            Assert.Equal(0, executionCount.TotalDuplicates);
            var duplicates = executionCount.JobExecutionCounts.Where(kvp => kvp.Value > 1).ToList();
            if (duplicates.Any())
            {
                output.WriteLine($"WARNING: Found {duplicates.Count} jobs executed multiple times:");
                foreach (var dup in duplicates.Take(10))
                {
                    output.WriteLine($"  Job {dup.Key} executed {dup.Value} times");
                }
            }
            Assert.Empty(duplicates);
            output.WriteLine($"✓ No duplicate executions - all jobs executed exactly once");
            
            output.WriteLine("==== All Assertions Passed ====");
            
            // Wait 2.5 minutes to ensure no additional jobs are being inserted (race condition check)
            output.WriteLine("");
            output.WriteLine("Waiting 2.5 minutes to verify no additional jobs are inserted...");
            await Task.Delay(TimeSpan.FromMinutes(2.5));
            
            // Re-validate totals after waiting
            output.WriteLine("==== Re-validation After 1 Minute Wait ====");
            
            var revalidationResults = new Dictionary<string, (int succeeded, int heldOnMaster, int other)>();
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();

                var allJobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });

                var succeeded = allJobs.Count(j => j.Status == JobMasterJobStatus.Succeeded);
                var heldOnMaster = allJobs.Count(j => j.Status == JobMasterJobStatus.HeldOnMaster);
                var other = allJobs.Count(j => j.Status != JobMasterJobStatus.Succeeded && j.Status != JobMasterJobStatus.HeldOnMaster);

                revalidationResults[clusterId] = (succeeded, heldOnMaster, other);
                
                await Task.Delay(50);
            }

            var revalidatedTotalSucceeded = revalidationResults.Sum(x => x.Value.succeeded);
            var revalidatedTotalHeldOnMaster = revalidationResults.Sum(x => x.Value.heldOnMaster);
            var revalidatedTotalOther = revalidationResults.Sum(x => x.Value.other);
            var revalidatedTotalInDb = revalidatedTotalSucceeded + revalidatedTotalHeldOnMaster + revalidatedTotalOther;
            
            output.WriteLine($"Initial: TotalInDb={totalInDb}, Succeeded={totalSucceeded}, HeldOnMaster={totalHeldOnMaster}, Other={totalOther}");
            output.WriteLine($"After 1min: TotalInDb={revalidatedTotalInDb}, Succeeded={revalidatedTotalSucceeded}, HeldOnMaster={revalidatedTotalHeldOnMaster}, Other={revalidatedTotalOther}");
            
            // Validate totals haven't changed
            Assert.Equal(totalInDb, revalidatedTotalInDb);
            output.WriteLine($"✓ Total in DB unchanged ({totalInDb})");
            
            Assert.Equal(totalSucceeded, revalidatedTotalSucceeded);
            output.WriteLine($"✓ Succeeded count unchanged ({totalSucceeded})");
            
            Assert.Equal(totalHeldOnMaster, revalidatedTotalHeldOnMaster);
            output.WriteLine($"✓ HeldOnMaster count unchanged ({totalHeldOnMaster})");
            
            Assert.Equal(totalOther, revalidatedTotalOther);
            output.WriteLine($"✓ Other count unchanged ({totalOther})");
            
            // Validate per-cluster totals haven't changed
            foreach (var clusterId in fixture.ClusterIds)
            {
                var initial = validationResults[clusterId];
                var revalidated = revalidationResults[clusterId];
                
                Assert.Equal(initial.succeeded, revalidated.succeeded);
                Assert.Equal(initial.heldOnMaster, revalidated.heldOnMaster);
                Assert.Equal(initial.other, revalidated.other);
                
                output.WriteLine($"✓ Cluster {clusterId}: counts unchanged (Succeeded={revalidated.succeeded}, HeldOnMaster={revalidated.heldOnMaster}, Other={revalidated.other})");
            }
            
            output.WriteLine("==== Re-validation Passed - No Additional Jobs Inserted ====");
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            logRootDir = DumpLogs();
        }
    }

    protected async Task RunExecutionTest(
        int qtyJobs, 
        bool scheduleAfter, 
        int timeoutInMinutes,
        int scheduleParallelLimit = 25)
    {
        var fromTimestamp = DateTime.UtcNow.AddMinutes(-1);
        var testExecutionId = Guid.NewGuid().ToString();
        
        // Set current test execution ID for log flushing
        fixture.CurrentTestExecutionId = testExecutionId;
        
        // Clear dictionary logs from previous tests to prevent memory accumulation
        foreach (var kvp in fixture.Dictionarylogs)
        {
            lock (kvp.Value)
            {
                kvp.Value.Clear();
            }
        }
        
        // Start performance monitoring (sample every 500ms)
        using var perfMonitor = new PerformanceMonitor(TimeSpan.FromMilliseconds(500));
        string? logRootDir = null;
        
        try
        {
            var scheduler = fixture.Services.GetRequiredService<IJobMasterScheduler>();

            var semaphore = new SemaphoreSlim(scheduleParallelLimit);
            var qtys = RandomQtys(qtyJobs);
            var expectedTotal = qtys.Sum(x => x.QtyJobs);
            var schedulingStopwatch = Stopwatch.StartNew();
            var tasks = new List<Task>();

            foreach (var qty in qtys)
            {
                for (int i = 0; i < qty.QtyJobs; i++)
                {
                    // Dispatch scheduling tasks in parallel, but limited by the semaphore 
                    // to maintain database stability and avoid connection pool exhaustion.
                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync();
                        try
                        {
                            var metadata = WritableMetadata.New();
                            metadata.SetStringValue("TestExecutionId", testExecutionId);
                            
                            if (!scheduleAfter)
                            {
                                // Schedules job for immediate execution.
                                await scheduler.OnceNowAsync<JobHandlerForTests>(
                                    metadata: metadata,
                                    clusterId: qty.ClusterId,
                                    workerLane: qty.WorkerLane,
                                    priority: qty.Priority);
                            }
                            else
                            {
                                // Schedules job with a specific delay.
                                await scheduler.OnceAfterAsync<JobHandlerForTests>(
                                    TimeSpan.FromMinutes(2),
                                    metadata: metadata,
                                    clusterId: qty.ClusterId,
                                    workerLane: qty.WorkerLane,
                                    priority: qty.Priority);
                            }
                        }
                        finally
                        {
                            // Releases the semaphore slot so the next task can proceed.
                            semaphore.Release();
                        }
                    }));
                }
            }

            var schedulingAll = Task.WhenAll(tasks);
            var timeoutToSchedule = timeoutInMinutes / 2;
            var schedulingTimeout = Task.Delay(TimeSpan.FromMinutes(timeoutToSchedule));

            var completed = await Task.WhenAny(schedulingAll, schedulingTimeout);
            if (completed != schedulingAll)
            {
                throw new TimeoutException($"Scheduling did not complete within {timeoutToSchedule} minutes.");
            }

            await schedulingAll;
            schedulingStopwatch.Stop();
            output.WriteLine($"SchedulerTest completed in {schedulingStopwatch.Elapsed}");

            // Get execution count for this test
            var executionCount = JobHandlerForTests.GetExecutionCount(testExecutionId);
            
            var timeoutAt = DateTime.UtcNow.AddMinutes(timeoutInMinutes);
            while (DateTime.UtcNow < timeoutAt && (executionCount == null || executionCount.JobExecutionCounts.Count < expectedTotal))
            {
                executionCount = JobHandlerForTests.GetExecutionCount(testExecutionId);
                
                // Fail fast: stop at the first detected duplicate execution
                if (executionCount != null && executionCount.TotalDuplicates > 0)
                {
                    var dup = executionCount.JobExecutionCounts.FirstOrDefault(kv => kv.Value > 1);
                    if (!dup.Equals(default(KeyValuePair<Guid, int>)))
                    {
                        Assert.True(false, $"Duplicate execution detected early. JobId={dup.Key} Count={dup.Value}");
                    }
                }
                await Task.Delay(200);
            }

            Assert.NotNull(executionCount);
            var executedTotal = executionCount.JobExecutionCounts.Count;
            var maxExecCount = executionCount.JobExecutionCounts.Count == 0 ? 0 : executionCount.JobExecutionCounts.Values.Max();

            var expectedByCluster = qtys
                .GroupBy(x => x.ClusterId)
                .ToDictionary(g => g.Key, g => g.Sum(x => x.QtyJobs));

            output.WriteLine("==== SchedulerTest Report ====");
            output.WriteLine($"TestExecutionId={testExecutionId}");
            output.WriteLine($"ExpectedTotal={expectedTotal}, ExecutedTotal={executedTotal}, Diff={(executedTotal - expectedTotal)}");
            output.WriteLine($"TotalExecuted={executionCount.TotalExecuted}");
            output.WriteLine($"TotalDuplicates={executionCount.TotalDuplicates}");
            output.WriteLine($"MaxExecutionCountPerJobId={maxExecCount}");
            output.WriteLine("-- By Cluster --");
            foreach (var clusterId in fixture.ClusterIds.OrderBy(x => x))
            {
                expectedByCluster.TryGetValue(clusterId, out var exp);
                output.WriteLine($"{clusterId}: Expected={exp}");
            }

            output.WriteLine("=============================");
            
            // Output performance report
            output.WriteLine(perfMonitor.GetReportString());

            // Query database to validate job statuses
            var sessionMetadataFilters = new List<GenericRecordValueFilter>()
            {
                new GenericRecordValueFilter()
                {
                    Key = "TestExecutionId",
                    Value = testExecutionId,
                    Operation = GenericFilterOperation.Eq,
                }
            };
            
            var dbValidationResults = new Dictionary<string, (int succeeded, int other)>();
            var allJobsFromDb = new List<Guid>();
            
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();

                var allJobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });

                var succeeded = allJobs.Count(j => j.Status == JobMasterJobStatus.Succeeded);
                var other = allJobs.Count(j => j.Status != JobMasterJobStatus.Succeeded);

                dbValidationResults[clusterId] = (succeeded, other);
                allJobsFromDb.AddRange(allJobs.Select(j => j.Id));
            }

            var totalSucceededInDb = dbValidationResults.Sum(x => x.Value.succeeded);
            var totalOtherInDb = dbValidationResults.Sum(x => x.Value.other);
            var totalInDb = allJobsFromDb.Count;
            
            output.WriteLine("==== Database Validation ====");
            output.WriteLine($"TotalInDb={totalInDb}, Succeeded={totalSucceededInDb}, Other={totalOtherInDb}");
            foreach (var clusterId in fixture.ClusterIds)
            {
                if (dbValidationResults.TryGetValue(clusterId, out var result))
                {
                    output.WriteLine($"Cluster {clusterId}: Succeeded={result.succeeded}, Other={result.other}");
                }
            }
            output.WriteLine("=============================");

            // ASSERTION 1: Total in DB matches expected
            Assert.Equal(expectedTotal, totalInDb);
            output.WriteLine($"✓ Total in DB ({totalInDb}) matches expected ({expectedTotal})");
            
            // ASSERTION 2: All jobs in DB are Succeeded status
            Assert.Equal(0, totalOtherInDb);
            output.WriteLine($"✓ All jobs in DB have Succeeded status (Other={totalOtherInDb})");
            
            // ASSERTION 3: Total executed matches expected
            Assert.Equal(expectedTotal, executedTotal);
            output.WriteLine($"✓ Total executed ({executedTotal}) matches expected ({expectedTotal})");
            
            // ASSERTION 4: TotalExecuted matches unique job count
            Assert.Equal(executionCount.TotalExecuted, executionCount.JobExecutionCounts.Count);
            output.WriteLine($"✓ TotalExecuted ({executionCount.TotalExecuted}) matches unique jobs ({executionCount.JobExecutionCounts.Count})");
            
            // ASSERTION 5: All job IDs from DB are in execution count dictionary
            var missingFromExecuted = allJobsFromDb.Where(id => !executionCount.JobExecutionCounts.ContainsKey(id)).ToList();
            Assert.Empty(missingFromExecuted);
            output.WriteLine($"✓ All {allJobsFromDb.Count} job IDs from DB are in execution count dictionary");

            // ASSERTION 6: No duplicate executions
            if (executionCount.TotalDuplicates > 0 || maxExecCount > 1)
            {
                var duplicated = executionCount.JobExecutionCounts
                    .Where(x => x.Value > 1)
                    .Select(x => new
                    {
                        JobId = x.Key,
                        Count = x.Value
                    })
                    .ToList();

                output.WriteLine("==== Duplicate Jobs Report ====");
                output.WriteLine($"TotalDuplicates={executionCount.TotalDuplicates}");
                output.WriteLine($"TotalDuplicatedJobs={duplicated.Count}");

                foreach (var item in duplicated.OrderByDescending(x => x.Count).ThenBy(x => x.JobId).Take(20))
                {
                    output.WriteLine($"  JobId={item.JobId} Executed={item.Count}");
                }

                output.WriteLine("==============================");
            }

            Assert.Equal(0, executionCount.TotalDuplicates);
            Assert.True(maxExecCount <= 1, $"Expected no dupes, but max execution count was {maxExecCount}.");
            output.WriteLine($"✓ No duplicate executions - all jobs executed exactly once");
            
            // Wait 2.5 minutes to ensure no additional jobs are being inserted (race condition check)
            output.WriteLine("");
            output.WriteLine("Waiting 2.5 minutes to verify no additional jobs are inserted...");
            await Task.Delay(TimeSpan.FromMinutes(2.5));
            
            // Re-validate totals after waiting
            output.WriteLine("==== Re-validation After 2.5 Minutes Wait ====");
            
            var revalidationResults = new Dictionary<string, (int succeeded, int other)>();
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();

                var allJobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });

                var succeeded = allJobs.Count(j => j.Status == JobMasterJobStatus.Succeeded);
                var other = allJobs.Count(j => j.Status != JobMasterJobStatus.Succeeded);

                revalidationResults[clusterId] = (succeeded, other);
            }

            var revalidatedTotalSucceeded = revalidationResults.Sum(x => x.Value.succeeded);
            var revalidatedTotalOther = revalidationResults.Sum(x => x.Value.other);
            var revalidatedTotalInDb = revalidatedTotalSucceeded + revalidatedTotalOther;
            
            output.WriteLine($"Initial: TotalInDb={totalInDb}, Succeeded={totalSucceededInDb}, Other={totalOtherInDb}");
            output.WriteLine($"After 2.5min: TotalInDb={revalidatedTotalInDb}, Succeeded={revalidatedTotalSucceeded}, Other={revalidatedTotalOther}");
            
            // Validate totals haven't changed
            Assert.Equal(totalInDb, revalidatedTotalInDb);
            output.WriteLine($"✓ Total in DB unchanged ({totalInDb})");
            
            Assert.Equal(totalSucceededInDb, revalidatedTotalSucceeded);
            output.WriteLine($"✓ Succeeded count unchanged ({totalSucceededInDb})");
            
            Assert.Equal(totalOtherInDb, revalidatedTotalOther);
            output.WriteLine($"✓ Other count unchanged ({totalOtherInDb})");
            
            // Validate per-cluster totals haven't changed
            foreach (var clusterId in fixture.ClusterIds)
            {
                var initial = dbValidationResults[clusterId];
                var revalidated = revalidationResults[clusterId];
                
                Assert.Equal(initial.succeeded, revalidated.succeeded);
                Assert.Equal(initial.other, revalidated.other);
                
                output.WriteLine($"✓ Cluster {clusterId}: counts unchanged (Succeeded={revalidated.succeeded}, Other={revalidated.other})");
            }
            
            output.WriteLine("==== Re-validation Passed - No Additional Jobs Inserted ====");
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            logRootDir =  DumpLogs();
            
            // Write performance report to file
            if (logRootDir != null)
            {
                var reportContent = perfMonitor.GetReportString();
                var filePath = Path.Combine(logRootDir, "performance-report.log");
                await File.WriteAllTextAsync(filePath, reportContent);
                output.WriteLine($"Performance report written: Path={filePath}");
            }
        }
    }

    /// <summary>
    /// Tests recurring schedule functionality with frequency validation.
    /// Schedules a recurring job, runs for specified duration, then cancels the schedule.
    /// Note: For short-period jobs (e.g., every second), some jobs may execute after cancellation
    /// because they were already scheduled before the cancel was processed.
    /// </summary>
    /// <param name="expressionTypeId">Type of recurrence expression (e.g., "TimeSpanInterval")</param>
    /// <param name="expression">The expression string (e.g., "00:00:01" for every second)</param>
    /// <param name="duration">How long to run the test before cancelling</param>
    /// <param name="qtyOfJobsExpected">Expected number of jobs to be created</param>
    /// <param name="discrepancyAllow">Tolerance for job count (±) - accounts for jobs that may execute after cancellation</param>
    /// <param name="frequencyExpected">Expected time interval between jobs</param>
    protected async Task RunRecurringScheduleTest(
        string expressionTypeId, 
        string expression, 
        TimeSpan duration, 
        int qtyOfJobsExpected, 
        int discrepancyAllow, 
        TimeSpan frequencyExpected)
    {
        var fromTimestamp = DateTime.UtcNow.AddMinutes(-1);
        var testExecutionId = Guid.NewGuid().ToString();
        fixture.CurrentTestExecutionId = testExecutionId;
        
        // Start performance monitoring (sample every 500ms)
        using var perfMonitor = new PerformanceMonitor(TimeSpan.FromMilliseconds(500));
        string? logRootDir = null;

        try
        {
            var scheduler = fixture.Services.GetRequiredService<IJobMasterScheduler>();
            
            await scheduler.OnceNowAsync<JobHandlerForTests>(
                metadata: WritableMetadata.New().SetStringValue("TestExecutionId", "WarmupJob"));
            // Nats consumer can take few seconds to initialize, if the gap is too small, some jobs may be missed
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // Create metadata with TestExecutionId
            var metadata = WritableMetadata.New();
            metadata.SetStringValue("TestExecutionId", testExecutionId);
            
            // Schedule recurring job
            var recurringContext = await scheduler.RecurringAsync<JobHandlerForTests>(
                expressionTypeId, 
                expression, 
                metadata: metadata);
            
            var recurringScheduleId = recurringContext.Id;
            
            output.WriteLine($"==== Recurring Schedule Test Started ====");
            output.WriteLine($"TestExecutionId={testExecutionId}");
            output.WriteLine($"RecurringScheduleId={recurringScheduleId}");
            output.WriteLine($"ExpressionType={expressionTypeId}");
            output.WriteLine($"Expression={expression}");
            output.WriteLine($"Duration={duration}");
            output.WriteLine($"ExpectedJobs={qtyOfJobsExpected} (±{discrepancyAllow})");
            output.WriteLine($"ExpectedFrequency={frequencyExpected}");
            output.WriteLine($"StartTime={DateTime.UtcNow:yyyy-MM-dd HH:mm:ss.fff}");
            output.WriteLine("=========================================");
            
            // Wait for NATS consumer to initialize before starting the test timer
            // This prevents missing jobs scheduled before the consumer is ready
            output.WriteLine("Waiting for consumer warmup (15 seconds)...");
            await Task.Delay(TimeSpan.FromSeconds(15));
            output.WriteLine("Consumer warmup complete, starting test timer");
            
            // Wait for the test duration
            var startTime = DateTime.UtcNow;
            await Task.Delay(duration);
            var endTime = DateTime.UtcNow;
            var actualDuration = endTime - startTime;
            
            output.WriteLine($"Test duration completed: {actualDuration}");
            
            // Cancel the recurring schedule
            output.WriteLine($"Cancelling recurring schedule: {recurringScheduleId}");
            var cancelled = await scheduler.TryCancelRecurringAsync(recurringScheduleId);
            output.WriteLine($"Recurring schedule cancelled: {cancelled}");
            
            // Wait for jobs to complete execution and cleanup to process
            // Poll until we have at least some jobs executed or timeout
            var minExpected = Math.Max(1, qtyOfJobsExpected - discrepancyAllow);
            var pollTimeout = DateTime.UtcNow.AddMinutes(2); // 2 minute timeout for jobs to execute
            var executionCount = JobHandlerForTests.GetExecutionCount(testExecutionId);
            
            output.WriteLine($"Waiting for jobs to execute (minimum expected: {minExpected})...");
            while (DateTime.UtcNow < pollTimeout && (executionCount == null || executionCount.TotalExecuted < minExpected))
            {
                await Task.Delay(1000);
                executionCount = JobHandlerForTests.GetExecutionCount(testExecutionId);
                
                if (executionCount != null && executionCount.TotalExecuted > 0)
                {
                    output.WriteLine($"  Progress: {executionCount.TotalExecuted} jobs executed so far...");
                }
            }
            
            // Wait a bit for cleanup runner to process jobs scheduled far in the future
            output.WriteLine("Waiting for cleanup runner to process (6 minutes)...");
            await Task.Delay(TimeSpan.FromMinutes(6));
            
            Assert.NotNull(executionCount);
            
            var executedJobs = executionCount.JobExecutionCounts.Keys.ToList();
            var totalExecuted = executionCount.TotalExecuted;
            
            output.WriteLine($"Total jobs executed: {totalExecuted}");
            output.WriteLine($"Unique jobs executed: {executedJobs.Count}");
            
            // Query database for jobs with this TestExecutionId
            var sessionMetadataFilters = new List<GenericRecordValueFilter>()
            {
                new GenericRecordValueFilter()
                {
                    Key = "TestExecutionId",
                    Value = testExecutionId,
                    Operation = GenericFilterOperation.Eq,
                }
            };
            
            var allJobsFromDb = new List<JobRawModel>();
            foreach (var clusterId in fixture.ClusterIds)
            {
                var factory = JobMasterClusterAwareComponentFactories.GetFactory(clusterId);
                var masterJobsService = factory.GetComponent<IMasterJobsService>();

                var jobs = await masterJobsService.QueryAsync(new JobQueryCriteria
                {
                    MetadataFilters = sessionMetadataFilters,
                    CountLimit = int.MaxValue
                });

                allJobsFromDb.AddRange(jobs);
            }
            
            var succeededJobs = allJobsFromDb.Where(j => j.Status == JobMasterJobStatus.Succeeded).ToList();
            var jobsWithRecurringId = allJobsFromDb.Where(j => j.RecurringScheduleId == recurringScheduleId).ToList();
            
            output.WriteLine($"Total jobs in DB: {allJobsFromDb.Count}");
            output.WriteLine($"Succeeded jobs in DB: {succeededJobs.Count}");
            output.WriteLine($"Jobs with RecurringScheduleId: {jobsWithRecurringId.Count}");
            
            // ASSERTION 1: Total executed matches expected (within discrepancy)
            minExpected = qtyOfJobsExpected - discrepancyAllow;
            var maxExpected = qtyOfJobsExpected + discrepancyAllow;
            Assert.InRange(totalExecuted, minExpected, maxExpected);
            output.WriteLine($"✓ Total executed ({totalExecuted}) is within expected range [{minExpected}, {maxExpected}]");
            
            // ASSERTION 2: Check job statuses
            // With many buckets (e.g., 180), some jobs may still be in Queued status waiting to be picked up
            // These will be cancelled when they reach onboarding or execution checks
            var cancelledJobs = allJobsFromDb.Where(j => j.Status == JobMasterJobStatus.Cancelled).ToList();
            var nonFinalStatusJobs = allJobsFromDb.Where(j => !j.Status.IsFinalStatus()).ToList();
            var queuedJobs = nonFinalStatusJobs.Where(j => j.Status == JobMasterJobStatus.Queued).ToList();
            
            output.WriteLine($"Job status breakdown:");
            output.WriteLine($"  Succeeded: {succeededJobs.Count}");
            output.WriteLine($"  Cancelled: {cancelledJobs.Count}");
            output.WriteLine($"  Non-final status: {nonFinalStatusJobs.Count}");
            output.WriteLine($"  Queued (waiting for pickup): {queuedJobs.Count}");
            
            if (nonFinalStatusJobs.Any())
            {
                var now = DateTime.UtcNow;
                var futureJobs = nonFinalStatusJobs.Where(j => j.ScheduledAt > now).ToList();
                var pastJobs = nonFinalStatusJobs.Where(j => j.ScheduledAt <= now).ToList();
                
                output.WriteLine($"  Non-final jobs scheduled in future: {futureJobs.Count}");
                output.WriteLine($"  Non-final jobs scheduled in past: {pastJobs.Count}");
                
                // Show status breakdown of non-final jobs
                var statusBreakdown = nonFinalStatusJobs.GroupBy(j => j.Status).Select(g => new { Status = g.Key, Count = g.Count() }).ToList();
                output.WriteLine($"  Non-final job status breakdown:");
                foreach (var status in statusBreakdown)
                {
                    output.WriteLine($"    {status.Status}: {status.Count}");
                }
                
                // Allow small number of Queued jobs (will be cancelled when picked up)
                // With 180 buckets, there may be processing delays
                var nonQueuedPastJobs = pastJobs.Where(j => j.Status != JobMasterJobStatus.Queued).ToList();
                
                if (nonQueuedPastJobs.Any())
                {
                    output.WriteLine($"  Sample of non-Queued past jobs (first 5):");
                    foreach (var job in nonQueuedPastJobs.Take(5))
                    {
                        var timeDiff = now - job.ScheduledAt;
                        output.WriteLine($"    Job {job.Id}: Status={job.Status}, ScheduledAt={job.ScheduledAt:HH:mm:ss}, Age={timeDiff.TotalMinutes:F1}min");
                    }
                }
                
                // Only non-Queued jobs scheduled in the past should be in final status
                // Queued jobs are waiting to be picked up and will be cancelled at onboarding/execution
                Assert.Empty(nonQueuedPastJobs);
                output.WriteLine($"✓ All past-scheduled non-Queued jobs are in final status");
                output.WriteLine($"✓ {queuedJobs.Count} Queued jobs waiting for pickup (will be cancelled)");
                output.WriteLine($"✓ {futureJobs.Count} future-scheduled jobs remain (will be cancelled by JobsExecutionEngine)");
            }
            else
            {
                output.WriteLine($"✓ All jobs in DB have final status (Succeeded: {succeededJobs.Count}, Cancelled: {cancelledJobs.Count})");
            }
            
            // ASSERTION 3: All jobs have the correct RecurringScheduleId
            Assert.Equal(allJobsFromDb.Count, jobsWithRecurringId.Count);
            output.WriteLine($"✓ All jobs have correct RecurringScheduleId ({recurringScheduleId})");
            
            // ASSERTION 4: Total executed matches succeeded jobs count
            Assert.Equal(totalExecuted, succeededJobs.Count);
            output.WriteLine($"✓ Total executed ({totalExecuted}) matches succeeded jobs count ({succeededJobs.Count})");
            
            // ASSERTION 5: No duplicate executions
            Assert.Equal(0, executionCount.TotalDuplicates);
            output.WriteLine($"✓ No duplicate executions");
            
            // ASSERTION 6: Validate job frequency (ScheduledAt intervals)
            if (succeededJobs.Count >= 2)
            {
                var sortedJobs = succeededJobs.OrderBy(j => j.ScheduledAt).ToList();
                var intervals = new List<TimeSpan>();
                
                for (int i = 1; i < sortedJobs.Count; i++)
                {
                    var interval = sortedJobs[i].ScheduledAt - sortedJobs[i - 1].ScheduledAt;
                    intervals.Add(interval);
                }
                
                var avgInterval = TimeSpan.FromMilliseconds(intervals.Average(i => i.TotalMilliseconds));
                var minInterval = intervals.Min();
                var maxInterval = intervals.Max();
                
                output.WriteLine($"Frequency Analysis:");
                output.WriteLine($"  Expected: {frequencyExpected}");
                output.WriteLine($"  Average: {avgInterval}");
                output.WriteLine($"  Min: {minInterval}");
                output.WriteLine($"  Max: {maxInterval}");
                
                // Allow 10% tolerance for frequency
                var toleranceLower = frequencyExpected.TotalSeconds * 0.9;
                var toleranceUpper = frequencyExpected.TotalSeconds * 1.1;
                
                Assert.InRange(avgInterval.TotalSeconds, toleranceLower, toleranceUpper);
                output.WriteLine($"✓ Average frequency ({avgInterval.TotalSeconds:F2}s) is within tolerance [{toleranceLower:F2}s, {toleranceUpper:F2}s]");
                
                // Show first 10 intervals for debugging
                output.WriteLine("First 10 intervals:");
                for (int i = 0; i < Math.Min(10, intervals.Count); i++)
                {
                    output.WriteLine($"  Interval {i + 1}: {intervals[i].TotalSeconds:F2}s (ScheduledAt: {sortedJobs[i + 1].ScheduledAt:HH:mm:ss.fff})");
                }
            }
            else
            {
                output.WriteLine("⚠ Not enough jobs to validate frequency");
            }
            
            // ASSERTION 7: All executed job IDs are in DB
            var missingFromDb = executedJobs.Where(id => !allJobsFromDb.Any(j => j.Id == id)).ToList();
            Assert.Empty(missingFromDb);
            output.WriteLine($"✓ All {executedJobs.Count} executed job IDs are in DB");
            
            output.WriteLine("==== All Assertions Passed ====");
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            logRootDir = DumpLogs();
            
            // Write performance report to file
            if (logRootDir != null)
            {
                var reportContent = perfMonitor.GetReportString();
                var filePath = Path.Combine(logRootDir, "recurring-performance-report.log");
                await File.WriteAllTextAsync(filePath, reportContent);
                output.WriteLine($"Performance report written: Path={filePath}");
            }
        }
    }

    private string? DumpLogs()
    {
        try
        {
            // Flush any remaining logs before reporting
            if (!string.IsNullOrEmpty(fixture.CurrentTestExecutionId))
            {
                foreach (var clusterId in fixture.ClusterIds)
                {
                    if (fixture.Dictionarylogs.TryGetValue(clusterId, out var list))
                    {
                        lock (list)
                        {
                            output.WriteLine($"Remaining logs in memory for {clusterId}: {list.Count}");
                        }
                    }
                }
            }
            
            var rootDir = Path.Combine(AppContext.BaseDirectory, "jobmaster-test-logs", fixture.CurrentTestExecutionId);
            
            if (Directory.Exists(rootDir))
            {
                output.WriteLine($"Logs directory: {rootDir}");
                
                var logFiles = Directory.GetFiles(rootDir, "*.log");
                foreach (var logFile in logFiles)
                {
                    var fileInfo = new FileInfo(logFile);
                    var lineCount = File.ReadLines(logFile).Count();
                    output.WriteLine($"  {Path.GetFileName(logFile)}: {lineCount} lines, {fileInfo.Length / 1024.0:F2} KB");
                }
            }
            else
            {
                output.WriteLine($"No logs directory found at: {rootDir}");
            }

            return rootDir;
        }
        catch (Exception e)
        {
            output.WriteLine($"Failed to dump logs: {e}");
            return null;
        }
    }

    public IList<QtyJobsTests> RandomQtys(int qtyTotal)
    {
        var dictionary = new ConcurrentDictionary<string, QtyJobsTests>();
        var eligibleClusterIds = fixture.ClusterIds;

        for (int i = qtyTotal - 1; i >= 0; i--)
        {
            var clusterId = eligibleClusterIds.Random() ?? fixture.DefaultClusterId;
            string? workerLane = null;
            if (JobMasterRandomUtil.GetBoolean(0.5))
            {
                workerLane = fixture.WorkerLanes.Random();
            }

            var priority = new []
            {
                JobMasterPriority.VeryLow,
                JobMasterPriority.Low,
                JobMasterPriority.Medium,
                JobMasterPriority.High,
                JobMasterPriority.Critical
            }.Random();

            var key = $"{clusterId}||{workerLane}||{priority}";
            var qtyJobsTests = dictionary.GetOrAdd(key, new QtyJobsTests());
            qtyJobsTests.QtyJobs++;
            qtyJobsTests.ClusterId = clusterId;
            qtyJobsTests.Priority = priority;
            qtyJobsTests.WorkerLane = workerLane;
        }

        return dictionary.Values.ToList();
    }
}