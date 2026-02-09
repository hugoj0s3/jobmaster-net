<script lang="ts">
    import { onDestroy, onMount } from "svelte";

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

    const refreshIntervalSec = 20;
    let lastUpdatedAt = new Date();
    let isRefreshing = false;

    let metrics: Metrics = {
        upcomingJobs: {
            total: 12,
            breakdown: {
                OnMaster: 3,
                InBucket: 2,
                Queued: 5,
                Processing: 2
            }
        },
        failures: {
            jobsFailedExceededRetries: 5,
            failedExecutions: 11
        },
        workers: {
            onlineTotal: 4,
            executionMode: 2,
            drainingMode: 1,
            fullMode: 1,
            lastHeartbeatText: "≈ 12s"
        },
        hosts: {
            total: 6,
            offline: 1
        },
        buckets: {
            total: 24,
            lost: 0,
            draining: 1
        }
    };

    let recentlyExecutedJobs: RecentlyExecutedJob[] = [
        {
            jobId: "9aa1...1cc2",
            definitionId: "CleanupHandler",
            status: "Succeeded",
            executedAt: new Date(Date.now() - 70_000).toISOString(),
            durationText: "121ms"
        },
        {
            jobId: "717e...7d7a",
            definitionId: "InvoicingHandler",
            status: "Failed",
            executedAt: new Date(Date.now() - 61 * 60_000).toISOString(),
            durationText: "2.3s"
        },
        {
            jobId: "c0f1...a91b",
            definitionId: "GenerateReportHandler",
            status: "Succeeded",
            executedAt: new Date(Date.now() - 5 * 60_000).toISOString(),
            durationText: "5.2s"
        },
        {
            jobId: "8b12...f9a7",
            definitionId: "FetchDataHandler",
            status: "Cancelled",
            executedAt: new Date(Date.now() - 16 * 60_000).toISOString(),
            durationText: "—"
        }
    ];

    let uiNow = new Date();
    let nowTicker: number | undefined;
    let poller: number | undefined;

    function formatAgeShort(ms: number): string {
        const s = Math.max(0, Math.floor(ms / 1000));
        if (s < 60) return `${s}s`;
        const m = Math.floor(s / 60);
        if (m < 60) return `${m}m`;
        const h = Math.floor(m / 60);
        return `${h}h`;
    }

    function lastUpdatedAgo(): string {
        return formatAgeShort(uiNow.getTime() - lastUpdatedAt.getTime());
    }

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
            // Placeholder: quando ligar no backend, substitui aqui.
            // Ex.: const res = await fetch(`/api/dashboard?...`);
            // const data = await res.json();
            // metrics = data.metrics;
            // recentlyExecutedJobs = data.recentlyExecutedJobs;

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
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
                <span>Last updated: {lastUpdatedAgo()} ago</span>

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
    </div>
</div>