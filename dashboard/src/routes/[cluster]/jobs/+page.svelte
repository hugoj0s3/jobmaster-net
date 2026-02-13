<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { createJobMasterClient } from "$lib/api/client";
    import type { components } from "$lib/api/schema";
    import { BucketStatus, JobStatus as ApiJobStatus, Priority as ApiPriority } from "$lib/api/enums";
    import { lastUpdatedAgo } from "$lib/helper/time-ago";

    const refreshIntervalSec = 20;

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    const pageSize = 12;
    let pageIndex = 0;
    let lastClusterId: string | null = null;

    let uiNow = new Date();
    let nowTicker: number | undefined;

    type JobStatus =
        | "Processing"
        | "Queued"
        | "SavePending"
        | "OnMaster"
        | "InBucket"
        | "Succeeded"
        | "Failed"
        | "Cancelled";

    type Priority = "Critical" | "High" | "Medium" | "Low";

    type Job = {
        jobId: string;
        definitionId: string;
        metadata: Record<string, string>;
        status: JobStatus;
        priority: Priority;

        numberOfFailures?: number;
        maxNumberOfRetries?: number;

        executedAt?: string;
        scheduledAt?: string;

        host?: string;
        workerLane?: string;
        worker?: string;
        bucket?: string;
    };

    type ApiClusterModel = components["schemas"]["ApiClusterModel"];
    type ApiJobModel = components["schemas"]["ApiJobModel"];
    type ApiHostModel = components["schemas"]["ApiHostModel"];
    type ApiBucketModel = components["schemas"]["ApiBucketModel"];

    function resolveTransientThreshold(cluster: ApiClusterModel): string | null {
        return cluster.transientThreshold ?? null;
    }

    function mapApiStatus(status: number): JobStatus {
        if (status === ApiJobStatus.SavePending) return "SavePending";
        if (status === ApiJobStatus.HeldOnMaster) return "OnMaster";
        if (status === ApiJobStatus.AssignedToBucket) return "InBucket";
        if (status === ApiJobStatus.Processing) return "Processing";
        if (status === ApiJobStatus.Succeeded) return "Succeeded";
        if (status === ApiJobStatus.Queued) return "Queued";
        if (status === ApiJobStatus.Failed) return "Failed";
        if (status === ApiJobStatus.Cancelled) return "Cancelled";
        return "Queued";
    }

    function mapApiPriority(priority: number): Priority {
        if (priority === ApiPriority.VeryLow || priority === ApiPriority.Low) return "Low";
        if (priority === ApiPriority.Medium) return "Medium";
        if (priority === ApiPriority.High) return "High";
        return "Critical";
    }

    function stringifyMetadata(meta: Record<string, unknown> | null | undefined): Record<string, string> {
        if (!meta) return {};
        const out: Record<string, string> = {};
        for (const [k, v] of Object.entries(meta)) {
            if (v === null || v === undefined) continue;
            if (typeof v === "string") out[k] = v;
            else if (typeof v === "number" || typeof v === "boolean") out[k] = String(v);
            else {
                try {
                    out[k] = JSON.stringify(v);
                } catch {
                    out[k] = String(v);
                }
            }
        }
        return out;
    }

    function bestExecutedAtIso(j: ApiJobModel): string | undefined {
        return j.succeedExecutedAt ?? j.processingStartedAt ?? undefined;
    }

    function scheduledIso(j: ApiJobModel): string | undefined {
        return j.scheduledAt ?? j.createdAt ?? undefined;
    }

    let refresh = true;

    let lastUpdatedAt = new Date();
    let isRefreshing = false;

    let transientThreshold: string | null = null;

    let jobs: Job[] = [];
    let jobsTotalCount = 0;

    let hostsById = new Map<string, ApiHostModel>();
    let bucketsById = new Map<string, ApiBucketModel>();

    let poller: number | undefined;

    const statusClasses: Record<JobStatus, string> = {
        Processing: "badge-info",
        Queued: "badge-warning",
        SavePending: "badge-neutral",
        OnMaster: "badge-ghost",
        InBucket: "badge-secondary",
        Succeeded: "badge-success",
        Failed: "badge-error",
        Cancelled: "badge-ghost"
    };

    const priorityClasses: Record<Priority, string> = {
        Critical: "badge-error",
        High: "badge-warning",
        Medium: "badge-info",
        Low: "badge-neutral"
    };

    let selected = new Set<string>();

    $: allSelected = jobs.length > 0 && selected.size === jobs.length;
    $: someSelected = selected.size > 0 && selected.size < jobs.length;

    $: maxPageIndex = Math.max(0, Math.ceil(jobsTotalCount / pageSize) - 1);
    $: pageIndex = Math.min(pageIndex, maxPageIndex);
    $: pageStart = jobsTotalCount === 0 ? 0 : pageIndex * pageSize + 1;
    $: pageEnd = jobsTotalCount === 0 ? 0 : Math.min(jobsTotalCount, pageIndex * pageSize + jobs.length);

    function toggleAll(checked: boolean) {
        selected = checked ? new Set(jobs.map((j) => j.jobId)) : new Set();
    }

    function toggleOne(jobId: string, checked: boolean) {
        const next = new Set(selected);
        if (checked) next.add(jobId);
        else next.delete(jobId);
        selected = next;
    }

    function formatIso(iso: string) {
        const d = new Date(iso);
        return d.toLocaleString();
    }

    function diffMinutes(fromMs: number, toMs: number) {
        return Math.round((toMs - fromMs) / 60_000);
    }

    function formatTimeCell(job: Job): { label: string; tooltip?: string } {
        const now = Date.now();

        if (job.status === "Succeeded" || job.status === "Failed" || job.status === "Cancelled") {
            if (!job.executedAt) return { label: "Executed", tooltip: undefined };
            
            const executedMs = new Date(job.executedAt).getTime();
            const minsAgo = Math.max(0, diffMinutes(executedMs, now));
            
            if (minsAgo <= 59) {
                return { label: `Executed ${minsAgo}m ago`, tooltip: formatIso(job.executedAt) };
            }
            
            return { label: `Executed at ${formatIso(job.executedAt)}`, tooltip: formatIso(job.executedAt) };
        }

        if (job.scheduledAt) {
            const scheduledMs = new Date(job.scheduledAt).getTime();
            if (scheduledMs > now) {
                const mins = diffMinutes(now, scheduledMs);
                if (mins <= 59) {
                    return { label: `Scheduled in ${mins}m`, tooltip: formatIso(job.scheduledAt) };
                }
                return { label: `Scheduled at ${formatIso(job.scheduledAt)}`, tooltip: formatIso(job.scheduledAt) };
            }

            const delayedMins = Math.max(0, diffMinutes(scheduledMs, now));
            return { label: `Delayed ${delayedMins}m`, tooltip: formatIso(job.scheduledAt) };
        }

        return { label: "—", tooltip: undefined };
    }

    function metadataPairs(job: Job): Array<[string, string]> {
        return Object.entries(job.metadata).filter(([k]) => k.startsWith("!"));
    }

    function canRunAgain(job: Job) {
        return job.status === "Succeeded" || job.status === "Failed" || job.status === "Cancelled";
    }

    function canRunNow(job: Job) {
        return job.status === "OnMaster" || job.status === "InBucket";
    }

    function canCancel(job: Job) {
        return job.status === "OnMaster";
    }

    function canAbort(_job: Job) {
        return true;
    }

    $: selectedJobs = jobs.filter((j) => selected.has(j.jobId));
    $: bulkRunAgainEnabled = selectedJobs.length > 0 && selectedJobs.every(canRunAgain);
    $: bulkRunNowEnabled = selectedJobs.length > 0 && selectedJobs.every(canRunNow);
    $: bulkCancelEnabled = selectedJobs.length > 0 && selectedJobs.every(canCancel);
    $: bulkAbortEnabled = selectedJobs.length > 0 && selectedJobs.every(canAbort);

    function bulkRunAgain() {
        console.log("Bulk Run Again", [...selected]);
    }

    function bulkRunNow() {
        console.log("Bulk Run Now", [...selected]);
    }

    function bulkCancel() {
        console.log("Bulk Cancel", [...selected]);
    }

    function bulkAbort() {
        console.log("Bulk Abort", [...selected]);
    }

    async function ensureConfigLoaded() {
        if (apiBaseUrl) return;
        const res = await fetch("/jobmaster-config.json");
        if (!res.ok) throw new Error(`${res.status} ${res.statusText} - /jobmaster-config.json`);
        const cfg = (await res.json()) as JobmasterConfig;
        apiBaseUrl = cfg.apiBaseUrl;
    }

    async function loadHost(hostId: string): Promise<ApiHostModel | null> {
        try {
            await ensureConfigLoaded();
            const cid = clusterId();
            if (!cid) return null;

            const jm = createJobMasterClient(apiBaseUrl, fetch);
            const r = await jm.GET("/jm-api/{clusterId}/hosts/{hostId}", {
                params: { path: { clusterId: cid, hostId } }
            });
            if (r.error) throw r.error;
            return (r.data ?? null) as ApiHostModel | null;
        } catch {
            return null;
        }
    }

    async function loadBucket(bucketId: string): Promise<ApiBucketModel | null> {
        try {
            await ensureConfigLoaded();
            const cid = clusterId();
            if (!cid) return null;

            const jm = createJobMasterClient(apiBaseUrl, fetch);
            const r = await jm.GET("/jm-api/{clusterId}/buckets/{bucketId}", {
                params: { path: { clusterId: cid, bucketId } }
            });
            if (r.error) throw r.error;
            return (r.data ?? null) as ApiBucketModel | null;
        } catch {
            return null;
        }
    }

    async function refreshNow() {
        if (!refresh) return;

        isRefreshing = true;
        try {
            const cid = clusterId();
            if (!cid) return;

            if (lastClusterId !== cid) {
                lastClusterId = cid;
                pageIndex = 0;
            }

            await ensureConfigLoaded();

            const jm = createJobMasterClient(apiBaseUrl, fetch);

            try {
                const safeOffset = Math.max(0, Math.min(pageIndex, maxPageIndex)) * pageSize;

                const [cluster, jobsCount, apiJobs, apiHosts, apiBuckets] = await Promise.all([
                    jm.GET("/jm-api/clusters/{clusterId}", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiClusterModel;
                    }),

                    jm.GET("/jm-api/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    jm.GET("/jm-api/{clusterId}/jobs", {
                        params: { path: { clusterId: cid }, query: { CountLimit: pageSize, Offset: safeOffset } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    }),

                    jm.GET("/jm-api/{clusterId}/hosts", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiHostModel[];
                    }),
                    jm.GET("/jm-api/{clusterId}/buckets", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiBucketModel[];
                    })
                ]);

                transientThreshold = resolveTransientThreshold(cluster);

                jobsTotalCount = jobsCount;

                const newMaxPageIndex = Math.max(0, Math.ceil(jobsCount / pageSize) - 1);
                if (pageIndex > newMaxPageIndex) {
                    pageIndex = newMaxPageIndex;
                }

                hostsById = new Map(apiHosts.filter((h) => !!h.id).map((h) => [h.id as string, h]));
                bucketsById = new Map(apiBuckets.filter((b) => !!b.id).map((b) => [b.id as string, b]));

                jobs = apiJobs.map((j) => {
                    const status = mapApiStatus(j.status ?? ApiJobStatus.Queued);
                    const priority = mapApiPriority(j.priority ?? ApiPriority.Medium);

                    const bucketId = j.bucketId ?? undefined;
                    const bucket = bucketId ? bucketsById.get(bucketId) : undefined;

                    const hostDisplayName = bucket?.hostDisplayName ?? undefined;

                    const lane = j.workerLane ?? bucket?.workerLane ?? undefined;

                    const worker = j.agentWorkerId ?? bucket?.agentWorkerId ?? undefined;

                    const meta = stringifyMetadata(j.metadata);
                    if (j.workerLane) meta["!lane"] = j.workerLane;

                    return {
                        jobId: j.id ?? "",
                        definitionId: j.jobDefinitionId ?? "",
                        metadata: meta,
                        status,
                        priority,
                        numberOfFailures: j.numberOfFailures,
                        maxNumberOfRetries: j.maxNumberOfRetries,
                        executedAt: bestExecutedAtIso(j),
                        scheduledAt: scheduledIso(j),
                        host: hostDisplayName,
                        workerLane: lane,
                        worker: worker ?? undefined,
                        bucket: bucketId
                    };
                });

                selected = new Set([...selected].filter((id) => jobs.some((j) => j.jobId === id)));

                lastUpdatedAt = new Date();
            } catch {
                transientThreshold = null;
                jobsTotalCount = 0;
                jobs = [];
                hostsById = new Map();
                bucketsById = new Map();
                selected = new Set();
            }
        } finally {
            isRefreshing = false;
            lastUpdatedAt = new Date();
        }
    }

    function restartPoller() {
        if (poller) window.clearInterval(poller);
        poller = window.setInterval(() => {
            if (refresh) refreshNow();
        }, refreshIntervalSec * 1000);
    }

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

    function goToPage(nextIndex: number) {
        pageIndex = Math.max(0, Math.min(nextIndex, maxPageIndex));
        selected = new Set();
        refreshNow();
    }
</script>

<div class="min-h-screen bg-base-100">
    <div
            class="pointer-events-none fixed inset-0 opacity-50"
            style="
      background:
        radial-gradient(1200px 600px at 30% 10%, rgba(45,212,191,0.10), transparent 60%),
        radial-gradient(900px 500px at 70% 20%, rgba(96,165,250,0.10), transparent 60%),
        radial-gradient(900px 500px at 80% 80%, rgba(167,139,250,0.10), transparent 60%);
    "
    />

    <div class="relative w-full px-6 py-6">
        <div class="flex items-center justify-between gap-4">
            <div class="flex items-center gap-4 min-w-0">
                <h1 class="text-4xl font-semibold">Jobs</h1>

                <div class="flex items-center gap-2 text-xs opacity-80">
                    <div class="hidden sm:block">
                        <span class="font-semibold">{pageStart}-{pageEnd}</span>
                        <span class="opacity-70">/</span>
                        <span class="font-semibold">{jobsTotalCount}</span>
                    </div>

                    <div class="flex items-center gap-1">
                        <button class="btn btn-ghost btn-xs" on:click={() => goToPage(pageIndex - 1)} disabled={pageIndex <= 0 || isRefreshing}>
                            ‹
                        </button>
                        <div class="hidden sm:block">
                            Page <span class="font-semibold">{pageIndex + 1}</span>/<span class="font-semibold">{maxPageIndex + 1}</span>
                        </div>
                        <button class="btn btn-ghost btn-xs" on:click={() => goToPage(pageIndex + 1)} disabled={pageIndex >= maxPageIndex || isRefreshing}>
                            ›
                        </button>
                    </div>
                </div>
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
                <span>Last updated: {lastUpdatedAgo(uiNow, lastUpdatedAt)} ago</span>

                <button class="btn btn-ghost btn-sm btn-square" aria-label="Refresh now" on:click={refreshNow} disabled={isRefreshing}>
                    <svg xmlns="http://www.w3.org/2000/svg" class={"h-5 w-5 " + (isRefreshing ? "animate-spin" : "")} fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                        <path d="M21 12a9 9 0 1 1-3-6.7" />
                        <path d="M21 3v6h-6" />
                    </svg>
                </button>
            </div>
        </div>

        {#if selected.size > 0}
            <div
                    class="mt-4 flex flex-wrap items-center justify-between gap-3 rounded-box border border-base-300 bg-base-200/60 px-4 py-3 backdrop-blur"
            >
                <div class="text-sm opacity-80">
                    <span class="font-semibold">{selected.size}</span>
                    selected
                </div>

                <div class="flex flex-wrap gap-2">
                    <button class="btn btn-sm" on:click={bulkCancel} disabled={!bulkCancelEnabled}>
                        Cancel ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkAbort} disabled={!bulkAbortEnabled}>
                        Abort ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkRunNow} disabled={!bulkRunNowEnabled}>
                        Run Now ({selected.size})
                    </button>
                    <button class="btn btn-sm" on:click={bulkRunAgain} disabled={!bulkRunAgainEnabled}>
                        Clone & Run ({selected.size})
                    </button>

                    <button class="btn btn-ghost btn-sm" on:click={() => (selected = new Set())}>
                        Clear
                    </button>
                </div>
            </div>
        {/if}

        <div class="mt-4 card bg-base-200/50 shadow-xl backdrop-blur">
            <div class="overflow-x-auto">
                <table class="table">
                    <thead class="opacity-70">
                    <tr>
                        <th class="w-10">
                            <input
                                    type="checkbox"
                                    class="checkbox checkbox-sm"
                                    checked={allSelected}
                                    indeterminate={someSelected}
                                    on:change={(e) => toggleAll((e.currentTarget as HTMLInputElement).checked)}
                                    aria-label="Select all"
                            />
                        </th>
                        <th>JobId</th>
                        <th>DefinitionId</th>
                        <th>Metadata</th>
                        <th>Status</th>
                        <th>Failure Attempts</th>
                        <th>Priority</th>
                        <th>Time</th>
                        <th>Worker Lane</th>
                    </tr>
                    </thead>

                    <tbody>
                    {#each jobs as j (j.jobId)}
                        <tr class="hover">
                            <td>
                                <input
                                        type="checkbox"
                                        class="checkbox checkbox-sm"
                                        checked={selected.has(j.jobId)}
                                        on:change={(e) => toggleOne(j.jobId, (e.currentTarget as HTMLInputElement).checked)}
                                        aria-label={`Select ${j.jobId}`}
                                />
                            </td>

                            <td class="font-medium">
                                <span class="tooltip tooltip-bottom" data-tip={j.jobId}>
                                    <a class="link link-hover" href={`/jobs/${j.jobId}`} aria-label={`Open job ${j.jobId}`}>
                                        {j.jobId}
                                    </a>
                                </span>
                            </td>

                            <td>{j.definitionId}</td>

                            <td>
                                {#if metadataPairs(j).length === 0}
                                    <span class="opacity-60">—</span>
                                {:else}
                                    <div class="flex flex-wrap gap-2">
                                        {#each metadataPairs(j) as [k, v] (k)}
                                            <span class="badge badge-ghost badge-sm">{k}={v}</span>
                                        {/each}
                                    </div>
                                {/if}
                            </td>

                            <td>
                            <span class={`badge badge-sm ${statusClasses[j.status] ?? "badge-ghost"}`}>
                                {j.status}
                            </span>
                            </td>

                            <td>
                                {#if typeof j.maxNumberOfRetries === "number" && j.maxNumberOfRetries > 0}
                                    <span class="font-mono">{j.numberOfFailures ?? 0}/{j.maxNumberOfRetries}</span>
                                {:else}
                                    <span class="opacity-60">—</span>
                                {/if}
                            </td>

                            <td>
                            <span class={`badge badge-sm ${priorityClasses[j.priority] ?? "badge-ghost"}`}>
                                {j.priority}
                            </span>
                            </td>

                            <td>
                                {#if formatTimeCell(j).tooltip}
                            <span class="tooltip tooltip-bottom" data-tip={formatTimeCell(j).tooltip}>
                                {formatTimeCell(j).label}
                            </span>
                                {:else}
                                    <span>{formatTimeCell(j).label}</span>
                                {/if}
                            </td>
                            
                            <td>
                                {#if j.workerLane}
                                    <span class="tooltip tooltip-bottom" data-tip={j.workerLane}>{j.workerLane}</span>
                                {:else}
                                    <span class="opacity-60">—</span>
                                {/if}
                            </td>
                        </tr>
                    {/each}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</div>