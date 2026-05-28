<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { components } from "$lib/api/schema";
    import { BucketStatus, JobStatus as ApiJobStatus } from "$lib/api/enums";
    import { DateDisplayUtil } from "$lib/helper/date-display-util";
    import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
    import { SettingsStorage, type DashboardSettings } from "$lib/dashboard-settings-storage";
    import { createCopyFeedback } from "$lib/helper/clipboard-util";

    const copyFeedback = createCopyFeedback();
    const copiedId = copyFeedback.copiedId;

    const clusterId = () => $page.params.cluster;

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
        finalizedAt: string;
        durationText?: string;
    };

    type ApiJobModel = components["schemas"]["ApiJobModel"];

    interface ApiWorkerModel {
        isAlive?: boolean;
        mode?: number;
    }

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

    function formatFinalizedAt(iso: string): string {
        return DateDisplayUtil.formatRelativeOrDate(iso, uiNow);
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
                    cancelledJobs,
                    apiWorkers
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

                    (jmApi as any).GET("/{clusterId}/hosts/count", {
                        params: { path: { clusterId: cid } }
                    }).then((r: any) => {
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
                    }),

                    jmApi.GET("/{clusterId}/workers", {
                        params: { path: { clusterId: cid } },
                        parseAs: "json"
                    }).then((r) => {
                        return ((r.data ?? []) as ApiWorkerModel[]);
                    })
                ]);

                const upcomingTotal = onMasterCount + inBucketCount + queuedCount + processingCount;

                const onlineWorkers = apiWorkers.filter((w: any) => w.isAlive === true);
                const workerModes = onlineWorkers.map((w: any) => w.mode);
                const lastHb = onlineWorkers
                    .map((w: any) => w.lastHeartbeat ?? w.lastHeartbeatAt)
                    .filter(Boolean)
                    .sort()
                    .pop();

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
                    failures: {
                        jobsFailedExceededRetries: failedJobs.length,
                        failedExecutions: failedJobs.length
                    },
                    workers: {
                        onlineTotal: onlineWorkers.length,
                        executionMode: workerModes.filter((m) => m === 1).length,
                        drainingMode: workerModes.filter((m) => m === 3).length,
                        fullMode: workerModes.filter((m) => m === 2).length,
                        lastHeartbeatText: DateDisplayUtil.formatRelativeOrDate(lastHb, uiNow)
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
                    finalizedAt: bestJobTimestampIso(j),
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
        copyFeedback.destroy();
    });
</script>

<div class="min-h-screen bg-base-100">
    <div class="mx-auto max-w-full px-6 py-6">
        <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div class="flex items-center gap-3">
                <h1 class="text-3xl font-semibold tracking-tight">Overview</h1>

                <div class="badge badge-primary badge-lg font-semibold text-black">ACTIVE</div>
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
							<span>Last Refresh: {DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt, uiNow)}</span>
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
                        <a href="/{clusterId()}/jobs?statuses={ApiJobStatus.HeldOnMaster}" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>On Master</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.OnMaster, "badge-primary")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.OnMaster}
                            </span>
                        </a>
                        <a href="/{clusterId()}/jobs?statuses={ApiJobStatus.AssignedToBucket}" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>In Bucket</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.InBucket, "badge-secondary")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.InBucket}
                            </span>
                        </a>
                        <a href="/{clusterId()}/jobs?statuses={ApiJobStatus.Queued}" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Queued</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.Queued, "badge-warning")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.Queued}
                            </span>
                        </a>
                        <a href="/{clusterId()}/jobs?statuses={ApiJobStatus.Processing}" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Processing</span>
                            <span
                                class={`badge badge-sm ${kpiBadgeClass(metrics.upcomingJobs.breakdown.Processing, "badge-accent")} font-mono text-base font-semibold`}
                            >
                                {metrics.upcomingJobs.breakdown.Processing}
                            </span>
                        </a>
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
                        <a href="/{clusterId()}/workers?modes=Execution" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Execution Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.executionMode}</span>
                        </a>
                        <a href="/{clusterId()}/workers?modes=Draining" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Draining Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.drainingMode}</span>
                        </a>
                        <a href="/{clusterId()}/workers?modes=Full" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Full Mode</span>
                            <span class="font-mono text-base font-semibold">{metrics.workers.fullMode}</span>
                        </a>

                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Hosts</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.hosts.total}</div>

                    <div class="mt-3 text-xs opacity-70">
                        <a href="/{clusterId()}/hosts?statuses=Offline" class="flex items-center justify-between rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Offline</span>
                            <span class="font-mono text-base font-semibold text-error">{metrics.hosts.offline}</span>
                        </a>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Buckets</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.buckets.total}</div>

                    <div class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-xs opacity-70">
                        <a href="/{clusterId()}/buckets?statuses=Active" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Active</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.active}</span>
                        </a>
                        <a href="/{clusterId()}/buckets?statuses=Completing" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Completing</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.completing}</span>
                        </a>
                        <a href="/{clusterId()}/buckets?statuses=ReadyToDrain" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Draining Soon</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.readyToDrain}</span>
                        </a>
                        <a href="/{clusterId()}/buckets?statuses=Draining" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Draining</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.draining}</span>
                        </a>
                        <a href="/{clusterId()}/buckets?statuses=Lost" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Lost</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.lost}</span>
                        </a>
                        <a href="/{clusterId()}/buckets?statuses=ReadyToDelete" class="flex items-center justify-between gap-2 rounded px-1 -mx-1 hover:bg-base-300/50 transition-colors">
                            <span>Deleting Soon</span>
                            <span class="font-mono text-base font-semibold">{metrics.buckets.readyToDelete}</span>
                        </a>
                    </div>
                </div>
            </div>
        </div>

        {#if recentlyExecutedJobs.length > 0}
            <div class="mt-6">
                <div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
                    <div class="card-body">
                        <div class="flex items-center justify-between">
                            <div class="text-lg font-semibold">Recently Executed Jobs</div>
                        </div>

                        <div class="mt-4 overflow-x-auto">
                            <table class="table">
                                <thead>
                                <tr class="text-base-content/70">
                                    <th>JobId</th>
                                    <th>Status</th>
                                    <th>Definition Id</th>
                                    <th>Finalized At</th>
                                    <th class="text-right">Duration</th>
                                </tr>
                                </thead>
                                <tbody>
                                {#each recentlyExecutedJobs as j (j.jobId)}
                                    <tr class="hover cursor-pointer">
                                        <td class="font-medium">
                                            <span class="inline-flex items-center gap-1">
                                                <a href="/{clusterId()}/jobs/{j.jobId}" class="link link-hover link-primary">{j.jobId}</a>
                                                <button
                                                    class="btn btn-ghost btn-xs btn-square opacity-40 hover:opacity-100"
                                                    aria-label="Copy Job ID"
                                                    on:click={() => copyFeedback.copy(j.jobId)}
                                                >
                                                    {#if $copiedId === j.jobId}
                                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5 text-success" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                                            <path d="M20 6 9 17l-5-5"/>
                                                        </svg>
                                                    {:else}
                                                        <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                                                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
                                                        </svg>
                                                    {/if}
                                                </button>
                                            </span>
                                        </td>
                                        <td>
                                            <span class={`badge badge-sm ${JobStatusUtil.getBadgeClass(j.status)}`}>
                                                {j.status}
                                            </span>
                                        </td>
                                        <td>{j.definitionId}</td>
                                        <td class="opacity-80">{formatFinalizedAt(j.finalizedAt)}</td>
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
