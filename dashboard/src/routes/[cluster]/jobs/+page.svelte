<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { components } from "$lib/api/schema";
    import { JobStatus as ApiJobStatus } from "$lib/api/enums";
    import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
    import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
    import FilterItem from "$lib/components/filters/FilterItem.svelte";
    import Pager from "$lib/components/Pager.svelte";
    import { DateTimeUtil } from "$lib/helper/datetime-util";
    import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
    import { PriorityUtil, type PriorityLabel } from "$lib/helper/priority-util";
		import { resolve } from '$app/paths';
		import { copyText, createCopyFeedback } from '$lib/helper/clipboard-util';
		import { readUrlParams, writeUrlParams, Serializers } from '$lib/helper/url-filters';
		import { parseDatetimeParam, datetimeToParam, computeDateRange, type DatetimeFilterValue } from '$lib/helper/datetime-filter-url';

    const refreshIntervalSec = 20;

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    const urlParamDefs = {
        statuses: { defaultValue: [] as number[], ...Serializers.numberArray },
        scheduledAt: { defaultValue: "" as string },
        bucketId: { defaultValue: "" },
        page: { defaultValue: 0, ...Serializers.number },
        size: { defaultValue: 12, ...Serializers.number }
    };

    let _initParams = readUrlParams(urlParamDefs);
    let pageSize = _initParams.size;
    let pageIndex = _initParams.page;
    let lastClusterId: string | null = null;

    let filterKey = $page.url.search;
    let lastSearch = $page.url.search;
    $: if ($page.url.search !== lastSearch) {
        lastSearch = $page.url.search;
        filterKey = $page.url.search;
        _initParams = readUrlParams(urlParamDefs);
        pageSize = _initParams.size;
        pageIndex = _initParams.page;
        selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
        selectedBucketId = _initParams.bucketId;
        filterValues = parseDatetimeParam(_initParams.scheduledAt, "scheduledAt");
        refreshNow();
    }

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
        bucketId?: string;
        bucketName?: string;
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

    let lastPageIndexForRefresh = pageIndex;
    $: if (pageIndex !== lastPageIndexForRefresh) {
        lastPageIndexForRefresh = pageIndex;
        refreshNow();
    }

    let lastPageSizeForRefresh = pageSize;
    $: if (pageSize !== lastPageSizeForRefresh) {
        lastPageSizeForRefresh = pageSize;
        pageIndex = 0;
        refreshNow();
    }

    let selectedStatuses: number[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
    let selectedBucketId: string = _initParams.bucketId;

    type ApiBucketModel = components["schemas"]["ApiBucketModel"];
    let bucketOptions: { value: string; label: string }[] = [];
    let bucketNameMap: Record<string, string> = {};

    type FilterValues = Record<string, unknown>;
    let filterValues: FilterValues = parseDatetimeParam(_initParams.scheduledAt, "scheduledAt");

    function syncToUrl() {
        writeUrlParams(urlParamDefs, {
            statuses: selectedStatuses,
            scheduledAt: datetimeToParam(filterValues, "scheduledAt"),
            bucketId: selectedBucketId,
            page: pageIndex,
            size: pageSize
        });
    }

    $: filterValues, selectedStatuses, selectedBucketId, pageIndex, pageSize, syncToUrl();

    function buildJobsQuery() {
        const hb = (filterValues.scheduledAt ?? {}) as DatetimeFilterValue;
        const range = computeDateRange(hb);

        return {
            Statuses: selectedStatuses.length > 0 ? selectedStatuses as components["schemas"]["JobMasterJobStatus"][] : undefined,
            ScheduledFrom: range.from?.toISOString(),
            ScheduledTo: range.to?.toISOString(),
            BucketId: selectedBucketId || undefined
        } as const;
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

            const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

            try {
                const safeOffset = Math.max(0, pageIndex) * pageSize;

                const filters = buildJobsQuery();
                const [jobsCount, apiJobs, apiBuckets] = await Promise.all([
                    jm.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: filters }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    jm.GET("/{clusterId}/jobs", {
                        params: {
                            path: { clusterId: cid },
                            query: { ...filters, CountLimit: pageSize, Offset: safeOffset }
                        }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    }),
                    jm.GET("/{clusterId}/buckets", {
                        params: { path: { clusterId: cid } }
                    }).then((r) => {
                        if (r.error) throw r.error;
                        return r.data as ApiBucketModel[];
                    })
                ]);

                const newBucketMap: Record<string, string> = {};
                bucketOptions = (apiBuckets ?? []).map((b) => {
                    const id = b.id ?? "";
                    const name = b.name ?? b.id ?? "—";
                    newBucketMap[id] = name;
                    return { value: id, label: name };
                });
                bucketNameMap = newBucketMap;

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
                        worker: j.agentWorkerId,
                        bucketId: j.bucketId,
                        bucketName: j.bucketId ? (bucketNameMap[j.bucketId] ?? j.bucketId) : undefined
                    };
                });


                lastUpdatedAt = new Date();
            } catch {
                jobsTotalCount = 0;
                jobs = [];
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

		const copyFeedback = createCopyFeedback({ resetAfterMs: 1200 });
		const copiedId = copyFeedback.copiedId;

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
			copyFeedback.destroy();
			if (nowTicker) window.clearInterval(nowTicker);
			if (poller) window.clearInterval(poller);
    });
</script>

<div class="min-h-screen bg-base-100">
    <div class="mx-auto max-w-full px-6 py-6">
        <div class="flex items-center justify-between gap-4">
            <h1 class="text-3xl font-semibold tracking-tight">Jobs</h1>

            <div class="flex items-center gap-3 text-sm opacity-80">
								<span>Last Refresh: {lastUpdatedAt.toLocaleString()}</span>

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

        <div class="flex items-center justify-between gap-4 mt-4">
            {#key filterKey}
            <div class="flex flex-wrap items-center gap-2">
                <FilterDropdownMulti
                    label="Statuses"
                    options={[
                        { value: String(ApiJobStatus.SavePending), label: JobStatusUtil.Label.SavePending },
                        { value: String(ApiJobStatus.HeldOnMaster), label: JobStatusUtil.Label.HeldOnMaster },
                        { value: String(ApiJobStatus.AssignedToBucket), label: JobStatusUtil.Label.AssignedToBucket },
                        { value: String(ApiJobStatus.Processing), label: JobStatusUtil.Label.Processing },
                        { value: String(ApiJobStatus.Succeeded), label: JobStatusUtil.Label.Succeeded },
                        { value: String(ApiJobStatus.Queued), label: JobStatusUtil.Label.Queued },
                        { value: String(ApiJobStatus.Failed), label: JobStatusUtil.Label.Failed },
                        { value: String(ApiJobStatus.Cancelled), label: JobStatusUtil.Label.Cancelled }
                    ]}
                    values={selectedStatuses.map(String)}
                    on:change={(e) => {
                        selectedStatuses = e.detail.map(Number);
                        pageIndex = 0;
                        refreshNow();
                    }}
                />

                <FilterDropdownMulti
                    label="Bucket"
                    options={bucketOptions}
                    values={selectedBucketId ? [selectedBucketId] : []}
                    on:change={(e) => {
                        selectedBucketId = e.detail.length > 0 ? e.detail[e.detail.length - 1] : "";
                        pageIndex = 0;
                        refreshNow();
                    }}
                />

                <FilterContainer
                    initialValues={filterValues}
                    onChange={(v) => {
                        filterValues = v;
                        pageIndex = 0;
                        refreshNow();
                    }}
                >
                    <FilterItem
                        id="scheduledAt"
                        label="Scheduled at"
                        type="datetime"
                        presets={[
                            { type: "LAST_MINUTES", minutes: 15, label: "Last 15 min" },
                            { type: "LAST_MINUTES", minutes: 30, label: "Last 30 min" },
                            { type: "LAST_MINUTES", minutes: 60, label: "Last 1 hour" }
                        ]}
                    />
                </FilterContainer>
            </div>
            {/key}

            <Pager
                bind:pageIndex
                bind:pageSize
                totalCount={jobsTotalCount}
                currentCount={jobs.length}
                disabled={isRefreshing}
                showPageSize={true}
            />
        </div>

        <div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
            <div class="overflow-x-auto">
                <table class="table">
                    <thead>
                    <tr class="text-base-content/70">
                        <th>JobId</th>
                        <th>DefinitionId</th>
                        <th>Metadata</th>
                        <th>Status</th>
                        <th>Failure Attempts</th>
                        <th>Priority</th>
                        <th>Time</th>
                        <th>Worker Lane</th>
                        <th>Bucket</th>
                    </tr>
                    </thead>

                    <tbody>
                    {#each jobs as j (j.jobId)}
                        <tr class="hover cursor-pointer">
													<td class="font-medium">
														<div class="flex items-center gap-2">
															<span class="tooltip tooltip-bottom" data-tip={j.jobId}>
																	<a
																		class="link link-hover"
																		href={resolve(`/${clusterId()}/jobs/${j.jobId}`)}
																		aria-label={`Open job ${j.jobId}`}
																	>
																			{j.jobId}
																	</a>
															</span>

															<button
																class="btn btn-ghost btn-xs btn-square opacity-40 hover:opacity-100"
																aria-label="Copy Job ID"
																on:click|preventDefault|stopPropagation={() => copyFeedback.copy(j.jobId)}
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
														</div>
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

                            <td>
                                {#if j.bucketId}
                                    <a
                                        class="link link-hover link-primary"
                                        href={resolve(`/${clusterId()}/buckets/${j.bucketId}`)}
                                        title={j.bucketId}
                                    >{j.bucketName ?? j.bucketId}</a>
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