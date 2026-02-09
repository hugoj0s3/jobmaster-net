<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { fetchJson } from "$lib/helper/fetch-json";
    import { lastUpdatedAgo } from "$lib/helper/time-ago";

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    function apiUrl(path: string) {
        const base = apiBaseUrl ?? "/jm-api";
        return `${base}${path.startsWith("/") ? path : `/${path}`}`;
    }

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
            lost: number;
            draining: number;
        };
    };

    type JobStatus = "Succeeded" | "Failed" | "Cancelled";

    type RecentlyExecutedJob = {
        jobId: string;
        definitionId: string;
        status: JobStatus;
        executedAt: string;
        durationText?: string;
    };

    type ApiCluster = {
        clusterId?: string;
        transientThreshold?: string;
        ClusterId?: string;
        TransientThreshold?: string;
    };

    type ApiJob = {
        id: string;
        jobDefinitionId: string;
        status: number;
        createdAt?: string;
        scheduledAt?: string;
        processingStartedAt?: string;
        succeedExecutedAt?: string;
    };

    function statusLabel(status: number): JobStatus {
        if (status === 5) return "Succeeded";
        if (status === 7) return "Failed";
        if (status === 8) return "Cancelled";
        return "Succeeded";
    }

    function bestJobTimestampIso(j: ApiJob): string {
        return (
            j.succeedExecutedAt ??
            j.processingStartedAt ??
            j.scheduledAt ??
            j.createdAt ??
            new Date(0).toISOString()
        );
    }

    function resolveTransientThreshold(cluster: ApiCluster): string | null {
        return cluster.transientThreshold ?? cluster.TransientThreshold ?? null;
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
                lost: 0,
                draining: 0
            }
        };
    }

    const refreshIntervalSec = 20;
    let lastUpdatedAt = new Date();
    let isRefreshing = false;

    let transientThreshold: string | null = null;

    let metrics: Metrics = zeroMetrics();

    let recentlyExecutedJobs: RecentlyExecutedJob[] = [];

    let uiNow = new Date();
    let nowTicker: number | undefined;
    let poller: number | undefined;

    function executedAgo(iso: string): string {
        const ms = uiNow.getTime() - new Date(iso).getTime();
        return formatAgeShort(ms);
    }

    function jobStatusBadgeClass(s: JobStatus): string {
        if (s === "Succeeded") return "badge-success";
        if (s === "Failed") return "badge-error";
        return "badge-ghost";
    }

    async function refreshNow() {
        isRefreshing = true;
        try {
            const cid = clusterId();
            if (!cid) return;

            if (!apiBaseUrl) {
                const cfg = await fetchJson<JobmasterConfig>("/jobmaster-config.json");
                apiBaseUrl = cfg.apiBaseUrl;
            }

            try {
                const [
                    cluster,
                    onMasterCount,
                    inBucketCount,
                    queuedCount,
                    processingCount,
                    hostsCount,
                    bucketsCount,
                    succeededJobs,
                    failedJobs,
                    cancelledJobs
                ] = await Promise.all([
                    fetchJson<ApiCluster>(apiUrl(`/clusters/${encodeURIComponent(cid)}`)),

                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/jobs/count?Status=2`)),
                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/jobs/count?Status=3`)),
                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/jobs/count?Status=6`)),
                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/jobs/count?Status=4`)),

                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/hosts/count`)),
                    fetchJson<number>(apiUrl(`/${encodeURIComponent(cid)}/buckets/count`)),

                    fetchJson<ApiJob[]>(apiUrl(`/${encodeURIComponent(cid)}/jobs?Status=5&CountLimit=10&Offset=0`)),
                    fetchJson<ApiJob[]>(apiUrl(`/${encodeURIComponent(cid)}/jobs?Status=7&CountLimit=10&Offset=0`)),
                    fetchJson<ApiJob[]>(apiUrl(`/${encodeURIComponent(cid)}/jobs?Status=8&CountLimit=10&Offset=0`))
                ]);

                transientThreshold = resolveTransientThreshold(cluster);

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
                        lost: 0,
                        draining: 0
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
                    jobId: j.id,
                    definitionId: j.jobDefinitionId,
                    status: statusLabel(j.status),
                    executedAt: bestJobTimestampIso(j),
                    durationText: "—"
                }));
            } catch {
                metrics = zeroMetrics();
                transientThreshold = null;
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

    $: sortedRecentlyExecutedJobs = [...recentlyExecutedJobs].sort(
        (a, b) => new Date(b.executedAt).getTime() - new Date(a.executedAt).getTime()
    );

    onMount(() => {
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

                {#if transientThreshold}
                    <div class="badge badge-ghost badge-lg font-mono">
                        TransientThreshold: {transientThreshold}
                    </div>
                {/if}
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
                <span>Last updated: {lastUpdatedAgo(uiNow, lastUpdatedAt)} ago</span>
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
            </div>
        </div>

        <div class="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-5">
            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Upcoming Jobs</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.upcomingJobs.total}</div>

                    <div class="mt-3 space-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>On Master</span>
                            <span class="font-mono">{metrics.upcomingJobs.breakdown.OnMaster}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>In Bucket</span>
                            <span class="font-mono">{metrics.upcomingJobs.breakdown.InBucket}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>Queued</span>
                            <span class="font-mono">{metrics.upcomingJobs.breakdown.Queued}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>Processing</span>
                            <span class="font-mono">{metrics.upcomingJobs.breakdown.Processing}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Jobs Failed (Exceeded Retries)</div>
                    <div class="mt-2 text-5xl font-semibold text-error">
                        {metrics.failures.jobsFailedExceededRetries}
                    </div>

                    <div class="mt-3 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>Failed Executions</span>
                            <span class="font-mono">{metrics.failures.failedExecutions}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Workers Online</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.workers.onlineTotal}</div>

                    <div class="mt-3 space-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>Execution Mode</span>
                            <span class="font-mono">{metrics.workers.executionMode}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>Draining Mode</span>
                            <span class="font-mono">{metrics.workers.drainingMode}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>Full Mode</span>
                            <span class="font-mono">{metrics.workers.fullMode}</span>
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
                            <span class="font-mono text-error">{metrics.hosts.offline}</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="card bg-base-200/70 shadow-xl backdrop-blur">
                <div class="card-body">
                    <div class="text-sm opacity-80">Buckets</div>
                    <div class="mt-2 text-5xl font-semibold">{metrics.buckets.total}</div>

                    <div class="mt-3 space-y-1 text-xs opacity-70">
                        <div class="flex items-center justify-between">
                            <span>Lost</span>
                            <span class="font-mono">{metrics.buckets.lost}</span>
                        </div>
                        <div class="flex items-center justify-between">
                            <span>Draining</span>
                            <span class="font-mono">{metrics.buckets.draining}</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        {#if sortedRecentlyExecutedJobs.length > 0}
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
                                    <th>Status</th>
                                    <th>JobId</th>
                                    <th>Definition</th>
                                    <th>Executed</th>
                                    <th class="text-right">Duration</th>
                                </tr>
                                </thead>
                                <tbody>
                                {#each sortedRecentlyExecutedJobs as j (j.jobId)}
                                    <tr class="hover">
                                        <td>
												<span class={`badge badge-sm ${jobStatusBadgeClass(j.status)}`}>
													{j.status}
												</span>
                                        </td>
                                        <td class="font-medium">{j.jobId}</td>
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