<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { components } from "$lib/api/schema";
    import { JobStatus as ApiJobStatus } from "$lib/api/enums";
    import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
    import FilterItem from "$lib/components/filters/FilterItem.svelte";
    import Pager from "$lib/components/Pager.svelte";
    import { DateTimeUtil } from "$lib/helper/datetime-util";
    import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
    import { PriorityUtil, type PriorityLabel } from "$lib/helper/priority-util";

    const refreshIntervalSec = 20;

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    let pageSize = 12;
    let pageIndex = 0;
    let lastClusterId: string | null = null;

    let uiNow = new Date();
    let nowTicker: number | undefined;

    type JobStatus = JobStatusLabel;
    type Priority = PriorityLabel;

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

        workerLane?: string;
        worker?: string;
    };

    type ApiJobModel = components["schemas"]["ApiJobModel"];

    function mapApiStatus(status: number): JobStatus {
        return JobStatusUtil.getLabel(status);
    }

    function mapApiPriority(priority: number | null | undefined): Priority {
        return PriorityUtil.getLabel(priority);
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

    let jobs: Job[] = [];
    let jobsTotalCount = 0;

    let poller: number | undefined;

    function statusBadgeClass(status: JobStatus): string {
        return JobStatusUtil.getBadgeClass(status);
    }

    let selected = new Set<string>();

    $: allSelected = jobs.length > 0 && selected.size === jobs.length;
    $: someSelected = selected.size > 0 && selected.size < jobs.length;

    let lastPageIndexForRefresh = pageIndex;
    $: if (pageIndex !== lastPageIndexForRefresh) {
        lastPageIndexForRefresh = pageIndex;
        selected = new Set();
        refreshNow();
    }

    let lastPageSizeForRefresh = pageSize;
    $: if (pageSize !== lastPageSizeForRefresh) {
        lastPageSizeForRefresh = pageSize;
        pageIndex = 0;
        selected = new Set();
        refreshNow();
    }

    type FilterValues = Record<string, unknown>;
    let filterValues: FilterValues = {};

    function buildJobsQuery() {
        const statuses = (filterValues.statuses ?? []) as components["schemas"]["JobMasterJobStatus"][];
        const scheduledAt = (filterValues.scheduledAt ?? {}) as { from?: string; to?: string };

        return {
            Statuses: statuses.length > 0 ? statuses : undefined,
            ScheduledFrom: scheduledAt.from,
            ScheduledTo: scheduledAt.to
        } as const;
    }

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

        if (
            job.status === JobStatusUtil.Label.Succeeded ||
            job.status === JobStatusUtil.Label.Failed ||
            job.status === JobStatusUtil.Label.Cancelled
        ) {
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
        return (
            job.status === JobStatusUtil.Label.Succeeded ||
            job.status === JobStatusUtil.Label.Failed ||
            job.status === JobStatusUtil.Label.Cancelled
        );
    }

    function canRunNow(job: Job) {
        return job.status === JobStatusUtil.Label.HeldOnMaster || job.status === JobStatusUtil.Label.AssignedToBucket;
    }

    function canCancel(job: Job) {
        return job.status === JobStatusUtil.Label.HeldOnMaster;
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
            const jm = ApiClientUtil.CreateApiClient(apiBaseUrl, fetch);

            try {
                const safeOffset = Math.max(0, pageIndex) * pageSize;

                const filters = buildJobsQuery();
                const [jobsCount, apiJobs] = await Promise.all([
                    jm.GET("/jm-api/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: filters }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    jm.GET("/jm-api/{clusterId}/jobs", {
                        params: {
                            path: { clusterId: cid },
                            query: { ...filters, CountLimit: pageSize, Offset: safeOffset }
                        }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    })
                ]);

                jobsTotalCount = jobsCount;

                const newMaxPageIndex = Math.max(0, Math.ceil(jobsCount / pageSize) - 1);
                if (pageIndex > newMaxPageIndex) {
                    pageIndex = newMaxPageIndex;
                }

                jobs = apiJobs.map((j) => {
                    const status = mapApiStatus(j.status ?? ApiJobStatus.Queued);
                    const priority = mapApiPriority(j.priority);

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
                        workerLane: j.workerLane,
                        worker: j.agentWorkerId
                    };
                });

                selected = new Set([...selected].filter((id) => jobs.some((j) => j.jobId === id)));

                lastUpdatedAt = new Date();
            } catch {
                jobsTotalCount = 0;
                jobs = [];
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

                <Pager
                    bind:pageIndex
                    bind:pageSize
                    totalCount={jobsTotalCount}
                    currentCount={jobs.length}
                    disabled={isRefreshing}
                    showPageSize={true}
                />
            </div>

            <div class="flex items-center gap-3 text-sm opacity-80">
                <span>Last updated: {DateTimeUtil.lastUpdatedAgo(uiNow, lastUpdatedAt)} ago</span>

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

        <FilterContainer
            title="Filters"
            on:change={(e) => {
                filterValues = e.detail;
                pageIndex = 0;
                selected = new Set();
                refreshNow();
            }}
        >
            <FilterItem
                id="scheduledAt"
                label="Scheduled at"
                type="datetime"
                presets={[
                    { type: "LAST_MINUTES", minutes: 15, label: "Last 15 minutes" },
                    { type: "LAST_MINUTES", minutes: 60, label: "Last 60 minutes" },
                    { type: "NEXT_HOURS", hours: 10, label: "Next 10 hours" }
                ]}
            />

            <FilterItem
                id="statuses"
                label="Statuses"
                type="multiselect"
                options={[
                    { value: ApiJobStatus.SavePending, label: JobStatusUtil.Label.SavePending },
                    { value: ApiJobStatus.HeldOnMaster, label: JobStatusUtil.Label.HeldOnMaster },
                    { value: ApiJobStatus.AssignedToBucket, label: JobStatusUtil.Label.AssignedToBucket },
                    { value: ApiJobStatus.Processing, label: JobStatusUtil.Label.Processing },
                    { value: ApiJobStatus.Succeeded, label: JobStatusUtil.Label.Succeeded },
                    { value: ApiJobStatus.Queued, label: JobStatusUtil.Label.Queued },
                    { value: ApiJobStatus.Failed, label: JobStatusUtil.Label.Failed },
                    { value: ApiJobStatus.Cancelled, label: JobStatusUtil.Label.Cancelled }
                ]}
            />
        </FilterContainer>

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
                                <span class={`badge badge-sm ${statusBadgeClass(j.status)}`}>
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
                                <span class={`badge badge-sm ${PriorityUtil.getBadgeClass(j.priority)}`}>
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