<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { components } from "$lib/api/schema";
    import { BucketStatus, JobStatus as ApiJobStatus } from "$lib/api/enums";
    import { DateTimeUtil } from "$lib/helper/datetime-util";
    import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
    import { SettingsStorage, type DashboardSettings } from "$lib/dashboard-settings-storage";

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    type UpcomingJobsBreakdown = {
        OnMaster: number;
        InBucket: number;
        Queued: number;
        Processing: number;
    };

    type Metrics = {
        upcomingJobs: {
            total: number;
            breakdown: UpcomingJobsBreakdown;
        };
        failures: {
            jobsFailedExceededRetries: number;
            failedExecutions: number;
        };
        workers: {
            onlineTotal: number;
            executionMode: number;
            drainingMode: number;
            fullMode: number;
            lastHeartbeatText: string;
        };
        hosts: {
            total: number;
            offline: number;
        };
        buckets: {
            total: number;
            active: number;
            completing: number;
            readyToDrain: number;
            lost: number;
            draining: number;
            readyToDelete: number;
        };
    };

    type RecentlyExecutedJob = {
        jobId: string;
        definitionId: string;
        status: JobStatusLabel;
        executedAt: string;
        durationText?: string;
    };

    type ApiClusterModel = components["schemas"]["ApiClusterModel"];
    type ApiJobModel = components["schemas"]["ApiJobModel"];

    function bestJobTimestampIso(j: ApiJobModel): string {
        return (
            j.succeedExecutedAt ??
            j.processingStartedAt ??
            j.scheduledAt ??
            j.createdAt ??
            new Date(0).toISOString()
        );
    }

    function zeroMetrics(): Metrics {
        return {
            upcomingJobs: {
                total: 0,
                breakdown: {
                    OnMaster: 0,
                    InBucket: 0,
                    Queued: 0,
                    Processing: 0
                }
            },
            failures: {
                jobsFailedExceededRetries: 0,
                failedExecutions: 0
            },
            workers: {
                onlineTotal: 0,
                executionMode: 0,
                drainingMode: 0,
                fullMode: 0,
                lastHeartbeatText: "—"
            },
            hosts: {
                total: 0,
                offline: 0
            },
            buckets: {
                total: 0,
                active: 0,
                completing: 0,
                readyToDrain: 0,
                lost: 0,
                draining: 0,
                readyToDelete: 0
            }
        };
    }

    let refreshIntervalSec = 20;
    let lastUpdatedAt = new Date();
    let isRefreshing = false;

    let metrics: Metrics = zeroMetrics();

    let recentlyExecutedJobs: RecentlyExecutedJob[] = [];

    let uiNow = new Date();
    let nowTicker: number | undefined;
    let poller: number | undefined;

    let settings: DashboardSettings = SettingsStorage.Dashboards.resolve(clusterId());
    let showSettings = false;
    let draftSettings: DashboardSettings = SettingsStorage.Dashboards.resolve(clusterId());

    function openSettings() {
        draftSettings = { ...settings };
        showSettings = true;
    }

    function closeSettings() {
        showSettings = false;
    }

    function saveSettings() {
        const cid = clusterId();
        if (!cid) {
            settings = { ...draftSettings };
            refreshIntervalSec = settings.refreshIntervalSec;
            restartPoller();
            closeSettings();
            return;
        }

        settings = { ...draftSettings };
        SettingsStorage.Dashboards.set(cid, settings);
        refreshIntervalSec = settings.refreshIntervalSec;
        restartPoller();
        closeSettings();
    }

    function executedAgo(iso: string): string {
        const ms = uiNow.getTime() - new Date(iso).getTime();
        return DateTimeUtil.formatAgeShort(ms);
    }

    function kpiBadgeClass(count: number, activeClass: string): string {
        return count > 0 ? activeClass : "badge-ghost";
    }

    async function refreshNow() {
        isRefreshing = true;
        try {
            const cid = clusterId();
            if (!cid) return;

            const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

            try {
                const [
                    onMasterCount,
                    inBucketCount,
                    queuedCount,
                    processingCount,
                    hostsCount,
                    bucketsCount,
                    bucketsActiveCount,
                    bucketsCompletingCount,
                    bucketsReadyToDrainCount,
                    bucketsDrainingCount,
                    bucketsLostCount,
                    bucketsReadyToDeleteCount,
                    succeededJobs,
                    failedJobs,
                    cancelledJobs
                ] = await Promise.all([
                    jmApi.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.HeldOnMaster } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.AssignedToBucket } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.Queued } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.Processing } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),

                    jmApi.GET("/{clusterId}/hosts/count", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.Active } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.Completing } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.ReadyToDrain } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.Draining } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.Lost } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    
                    jmApi.GET("/{clusterId}/buckets/count", {
                        params: { path: { clusterId: cid }, query: { Status: BucketStatus.ReadyToDelete } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),

                    jmApi.GET("/{clusterId}/jobs", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.Succeeded, CountLimit: 10, Offset: 0 } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    }),
                    
                    jmApi.GET("/{clusterId}/jobs", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.Failed, CountLimit: 10, Offset: 0 } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    }),
                    
                    jmApi.GET("/{clusterId}/jobs", {
                        params: { path: { clusterId: cid }, query: { Status: ApiJobStatus.Cancelled, CountLimit: 10, Offset: 0 } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    })
                ]);

                const upcomingTotal = onMasterCount + inBucketCount + queuedCount + processingCount;

                metrics = {
                    ...metrics,
                    upcomingJobs: {
                        total: upcomingTotal,
                        breakdown: {
                            OnMaster: onMasterCount,
                            InBucket: inBucketCount,
                            Queued: queuedCount,
                            Processing: processingCount
                        }
                    },
                    hosts: {
                        total: hostsCount,
                        offline: 0
                    },
                    buckets: {
                        total: bucketsCount,
                        active: bucketsActiveCount,
                        completing: bucketsCompletingCount,
                        readyToDrain: bucketsReadyToDrainCount,
                        draining: bucketsDrainingCount,
                        lost: bucketsLostCount,
                        readyToDelete: bucketsReadyToDeleteCount
                    }
                };

                const merged = [...succeededJobs, ...failedJobs, ...cancelledJobs]
                    .sort(
                        (a, b) =>
                            new Date(bestJobTimestampIso(b)).getTime() -
                            new Date(bestJobTimestampIso(a)).getTime()
                    )
                    .slice(0, 10);

                recentlyExecutedJobs = merged.map((j) => ({
                    jobId: j.id ?? "",
                    definitionId: j.jobDefinitionId ?? "",
                    status: JobStatusUtil.getLabel(j.status ?? ApiJobStatus.Succeeded),
                    executedAt: bestJobTimestampIso(j),
                    durationText: "—"
                }));
            } catch (e) {
                console.error("Dashboard refresh failed", e);
                metrics = zeroMetrics();
                recentlyExecutedJobs = [];
            }

            lastUpdatedAt = new Date();
        } finally {
            isRefreshing = false;
        }
    }

    function restartPoller() {
        if (poller) window.clearInterval(poller);
        poller = window.setInterval(() => {
            refreshNow();
        }, refreshIntervalSec * 1000);
    }

    onMount(() => {
        settings = SettingsStorage.Dashboards.resolve(clusterId());
        refreshIntervalSec = settings.refreshIntervalSec;

        nowTicker = window.setInterval(() => {
            uiNow = new Date();
        }, 1000);

        refreshNow();
        restartPoller();

        return () => {
            if (nowTicker) window.clearInterval(nowTicker);
            if (poller) window.clearInterval(poller);
        };
    });

    onDestroy(() => {
        if (nowTicker) window.clearInterval(nowTicker);
        if (poller) window.clearInterval(poller);
    });
</script>

<div class="min-h-[calc(100vh-theme(spacing.14))] bg-base-100">
    <div class="w-full px-2 py-2">
        <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div class="flex items-center gap-3">
                <h1 class="text-2xl font-semibold tracking-tight text-base-content">Overview</h1>

                <div class="badge badge-primary badge-lg font-semibold text-black">ACTIVE</div>
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
							<span>Last execution: {lastUpdatedAt.toLocaleString()}</span>
                <button
                        class="btn btn-ghost btn-sm btn-square"
                        aria-label="Refresh now"
                        on:click={refreshNow}
                        disabled={isRefreshing}
                >
                    <svg
                            xmlns="http://www.w3.org/2000/svg"
                            class={"h-4 w-4 " + (isRefreshing ? "animate-spin" : "")}
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            stroke-width="2"
                    >
                        <path d="M21 12a9 9 0 1 1-3-6.7" />
                        <path d="M21 3v6h-6" />
                    </svg>
                </button>

                <button class="btn btn-ghost btn-sm btn-square ml-1" aria-label="Settings" on:click={openSettings}>
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        class="h-4 w-4"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        stroke-width="2"
                    >
                        <path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z" />
                        <path d="M19.4 15a1.8 1.8 0 0 0 .4 2l.1.1a2.2 2.2 0 0 1 0 3.1 2.2 2.2 0 0 1-3.1 0l-.1-.1a1.8 1.8 0 0 0-2-.4 1.8 1.8 0 0 0-1 1.6V22a2.2 2.2 0 0 1-4.4 0v-.2a1.8 1.8 0 0 0-1-1.6 1.8 1.8 0 0 0-2 .4l-.1.1a2.2 2.2 0 0 1-3.1 0 2.2 2.2 0 0 1 0-3.1l.1-.1a1.8 1.8 0 0 0 .4-2 1.8 1.8 0 0 0-1.6-1H2a2.2 2.2 0 0 1 0-4.4h.2a1.8 1.8 0 0 0 1.6-1 1.8 1.8 0 0 0-.4-2l-.1-.1a2.2 2.2 0 0 1 0-3.1 2.2 2.2 0 0 1 3.1 0l.1.1a1.8 1.8 0 0 0 2 .4 1.8 1.8 0 0 0 1-1.6V2a2.2 2.2 0 0 1 4.4 0v.2a1.8 1.8 0 0 0 1 1.6 1.8 1.8 0 0 0 2-.4l.1-.1a2.2 2.2 0 0 1 3.1 0 2.2 2.2 0 0 1 0 3.1l-.1.1a1.8 1.8 0 0 0-.4 2 1.8 1.8 0 0 0 1.6 1H22a2.2 2.2 0 0 1 0 4.4h-.2a1.8 1.8 0 0 0-1.6 1Z" />
                    </svg>
                </button>
            </div>
        </div>

        {#if showSettings}
            <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" role="dialog" aria-modal="true">
                <div class="card w-full max-w-md bg-base-100 shadow-xl">
                    <div class="card-body gap-4">
                        <div class="flex items-start justify-between gap-4">
                            <h3 class="text-lg font-semibold">Dashboard Settings</h3>
                            <button class="btn btn-ghost btn-sm btn-square" aria-label="Close" on:click={closeSettings}>✕</button>
                        </div>

                        <div class="grid gap-3">
                            <div class="form-control">
                                <div class="text-sm opacity-70">Upcoming window (minutes)</div>
                                <input
                                    class="input input-bordered"
                                    type="number"
                                    min="1"
                                    step="1"
                                    bind:value={draftSettings.nextMinutes}
                                />
                            </div>

                            <div class="form-control">
                                <div class="text-sm opacity-70">Failed jobs window (hours)</div>
                                <input
                                    class="input input-bordered"
                                    type="number"
                                    min="1"
                                    step="1"
                                    bind:value={draftSettings.lastHours}
                                />
                            </div>

                            <div class="form-control">
                                <div class="text-sm opacity-70">Refresh interval (seconds)</div>
                                <input
                                    class="input input-bordered"
                                    type="number"
                                    min="5"
                                    step="1"
                                    bind:value={draftSettings.refreshIntervalSec}
                                />
                            </div>
                        </div>

                        <div class="card-actions justify-end">
                            <button class="btn btn-ghost" on:click={closeSettings}>Cancel</button>
                            <button class="btn btn-primary" on:click={saveSettings}>Save</button>
                        </div>
                    </div>
                </div>
            </div>
        {/if}

        <div class="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Upcoming Execution <span class="opacity-60">(next {settings.nextMinutes} min)</span></div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.upcomingJobs.total}</div>

                    <div class="mt-3 space-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between gap-2">
                            <span>On Master</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.OnMaster, "badge-primary")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.OnMaster}
                            </span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>In Bucket</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.InBucket, "badge-secondary")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.InBucket}
                            </span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Queued</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.Queued, "badge-warning")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.Queued}
                            </span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Processing</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.Processing, "badge-accent")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.Processing}
                            </span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Failed (Exceeded Retries) <span class="opacity-60">(last {settings.lastHours}h)</span></div>
                    <div class="mt-2 text-5xl font-semibold text-error">
                        {metrics.failures.jobsFailedExceededRetries}
                    </div>

                    <div class="mt-3 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>Failed Executions</span>
                            <span class="font-mono text-base font-semibold">{metrics.failures.failedExecutions}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Workers Online</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.workers.onlineTotal}</div>

                    <div class="mt-3 space-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between gap-2">
                            <span>Execution Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.executionMode}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Draining Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.drainingMode}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Full Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.fullMode}</span>
                        </div>

                        <div class="pt-2 text-[11px] opacity-60">
                            Last heartbeat: {metrics.workers.lastHeartbeatText}
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Hosts</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.hosts.total}</div>

                    <div class="mt-3 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>Offline</span>
                            <span class="font-mono text-base font-semibold text-error">{metrics.hosts.offline}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Buckets</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.buckets.total}</div>

                    <div class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between gap-2">
                            <span>Active</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.active}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Completing</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.completing}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Draining Soon</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.readyToDrain}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Draining</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.draining}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Lost</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.lost}</span>
                        </div>
                        <div class="flex items-center justify-between gap-2">
                            <span>Deleting Soon</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.readyToDelete}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        {#if recentlyExecutedJobs.length > 0}
            <div class="mt-6">
                <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                    <div class="card-body">
                        <div class="flex items-center justify-between">
                            <div class="text-lg font-semibold">Recently Executed Jobs</div>
                        </div>

                        <div class="mt-4 overflow-x-auto">
                            <table class="table">
                                <thead class="opacity-60">
                                <tr>
                                    <th>JobId</th>
                                    <th>Status</th>
                                    <th>Definition Id</th>
                                    <th>Executed</th>
                                    <th class="text-right">Duration</th>
                                </tr>
                                </thead>
                                <tbody>
                                {#each recentlyExecutedJobs as j (j.jobId)}
                                    <tr class="hover">
                                        <td class="font-medium">{j.jobId}</td>
                                        <td>
                                            <span class={`badge badge-sm ${JobStatusUtil.getBadgeClass(j.status)}`}>
                                                {j.status}
                                            </span>
                                        </td>
                                        <td>{j.definitionId}</td>
                                        <td class="opacity-80">{executedAgo(j.executedAt)} ago</td>
                                        <td class="text-right font-mono opacity-80">{j.durationText ?? "—"}</td>
                                    </tr>
                                {/each}
                                </tbody>
                            </table>
                        </div>

                    </div>
                </div>
            </div>
        {/if}
    </div>
</div>