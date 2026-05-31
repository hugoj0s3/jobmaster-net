<script lang="ts">
    import { onDestroy, onMount } from "svelte";
    import { page } from "$app/stores";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { components } from "$lib/api/schema";
    import { JobStatus as ApiJobStatus } from "$lib/api/enums";
    import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
    import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
    import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
    import FilterItem from "$lib/components/filters/FilterItem.svelte";
    import Pager from "$lib/components/Pager.svelte";
    import { DateDisplayUtil } from "$lib/helper/date-display-util";
    import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
    import { PriorityUtil, type PriorityLabel } from "$lib/helper/priority-util";
		import { resolve } from '$app/paths';
		import { copyText, createCopyFeedback } from '$lib/helper/clipboard-util';
		import { readUrlParams, writeUrlParams, Serializers } from '$lib/helper/url-filters';
		import { parseDatetimeParam, datetimeToParam, computeDateRange, type DatetimeFilterValue } from '$lib/helper/datetime-filter-url';
    import { readSavedFilter, writeSavedFilter, setupFilterPersistOnUnload } from "$lib/helper/filter-persistence";
    import { bucketNameCache } from "$lib/helper/bucket-name-cache";

    const refreshIntervalSec = 20;

    const clusterId = () => $page.params.cluster;

    type JobmasterConfig = {
        apiBaseUrl: string;
    };

    let apiBaseUrl: string | null = null;

    const urlParamDefs = {
        statuses: { defaultValue: [] as number[], ...Serializers.numberArray },
        scheduledAt: { defaultValue: "" as string },
        jobDefinitionId: { defaultValue: "" as string },
        sortDirection: { defaultValue: "" as string },
        page: { defaultValue: 0, ...Serializers.number },
        size: { defaultValue: 10, ...Serializers.number }
    };

    const LS_KEY_JOBS_FILTERS = `jobs-filters-${$page.params.cluster}`;
    const DEFAULT_SCHEDULED_PARAM = "rel:min:-60:60"; // -1h to +1h

    let _initParams = readUrlParams(urlParamDefs);
    let pageSize = _initParams.size;
    let pageIndex = _initParams.page;

    // Load saved filters from localStorage
    function loadSavedFilters() {
        const saved = readSavedFilter(LS_KEY_JOBS_FILTERS, "");
        if (!saved) return null;
        try {
            return JSON.parse(saved);
        } catch {
            return null;
        }
    }

    let filterKey = $page.url.search;
    let lastSearch = $page.url.search;
    $: if ($page.url.search !== lastSearch) {
        lastSearch = $page.url.search;
        filterKey = $page.url.search;
        _initParams = readUrlParams(urlParamDefs);
        pageSize = _initParams.size;
        pageIndex = _initParams.page;
        selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
        selectedJobDefinitionId = _initParams.jobDefinitionId;
        selectedSortDirection = _initParams.sortDirection;
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
        nextPlanExecutionAt?: string;
        completedAt?: string;

        workerLane?: string;
        worker?: string;
        bucketId?: string;
        bucketName?: string;
    };

    type ApiJobModel = components["schemas"]["ApiJobModel"];
    type ApiBucketModel = components["schemas"]["ApiBucketModel"];

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
        return j.finalizedAt ?? j.processStartedAt ?? undefined;
    }

    function scheduledIso(j: ApiJobModel): string | undefined {
        return j.scheduledAt ?? j.createdAt ?? undefined;
    }

    let refresh = true;

    let lastUpdatedAt = new Date();
    let isRefreshing = false;

    let jobs: Job[] = [];
    let jobsTotalCount = 0;
    let lastClusterId: string | null = null;

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

    // Initialize with URL values if present
    let selectedStatuses: number[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
    let selectedJobDefinitionId: string = _initParams.jobDefinitionId;
    let selectedSortDirection: string = _initParams.sortDirection;
    let searchTimeout: ReturnType<typeof setTimeout> | undefined;

    type FilterValues = Record<string, unknown>;
    let filterValues: FilterValues = _initParams.scheduledAt ? parseDatetimeParam(_initParams.scheduledAt, "scheduledAt") : {};

    function debouncedSearch(value: string) {
        if (searchTimeout) clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => {
            selectedJobDefinitionId = value;
            pageIndex = 0;
            refreshNow();
        }, 300);
    }

    function resetFilters() {
        selectedStatuses = [];
        selectedJobDefinitionId = "";
        selectedSortDirection = "";
        filterValues = {};
        pageIndex = 0;
        refreshNow();
    }

    function syncToUrl() {
        writeUrlParams(urlParamDefs, {
            statuses: selectedStatuses,
            scheduledAt: datetimeToParam(filterValues, "scheduledAt"),
            jobDefinitionId: selectedJobDefinitionId,
            sortDirection: selectedSortDirection,
            page: pageIndex,
            size: pageSize
        });
    }

    $: filterValues, selectedStatuses, selectedJobDefinitionId, selectedSortDirection, pageIndex, pageSize, syncToUrl();

    onMount(() => {
        // If no URL params, try to load from localStorage
        const hasUrlParams = $page.url.search.length > 0;
        if (!hasUrlParams) {
            const savedFilters = loadSavedFilters();
            if (savedFilters) {
                // Load saved filters (respects even empty values)
                selectedStatuses = savedFilters.statuses !== undefined ? savedFilters.statuses : [];
                selectedJobDefinitionId = savedFilters.jobDefinitionId !== undefined ? savedFilters.jobDefinitionId : "";
                selectedSortDirection = savedFilters.sortDirection !== undefined ? savedFilters.sortDirection : "";
                filterValues = savedFilters.scheduledAt ? parseDatetimeParam(savedFilters.scheduledAt, "scheduledAt") : {};
            } else {
                // Apply default when no localStorage entry exists (first visit)
                filterValues = parseDatetimeParam(DEFAULT_SCHEDULED_PARAM, "scheduledAt");
            }
            syncToUrl();
        }
    });

    onDestroy(() => {
        // Save current filters to localStorage on unmount
        writeSavedFilter(
            LS_KEY_JOBS_FILTERS,
            JSON.stringify({
                statuses: selectedStatuses,
                jobDefinitionId: selectedJobDefinitionId,
                sortDirection: selectedSortDirection,
                scheduledAt: datetimeToParam(filterValues, "scheduledAt")
            })
        );
    });

    function buildJobsQuery() {
        const hb = (filterValues.scheduledAt ?? {}) as DatetimeFilterValue;
        const range = computeDateRange(hb);

        return {
            Statuses: selectedStatuses.length > 0 ? selectedStatuses as components["schemas"]["JobMasterJobStatus"][] : undefined,
            ScheduledFrom: range.from?.toISOString(),
            ScheduledTo: range.to?.toISOString(),
            JobDefinitionId: selectedJobDefinitionId || undefined
        };
    }


    function formatIso(iso: string) {
        return DateDisplayUtil.formatDateTime(iso);
    }

    function diffMinutes(fromMs: number, toMs: number) {
        return Math.round((toMs - fromMs) / 60_000);
    }

    function formatDateCell(dateIso: string | undefined, jobStatus?: JobStatus, showOnlyFuture: boolean = false): { label: string; tooltip?: string } {
        // For completed jobs (success, failure, cancelled), don't show next planned execution
        if (jobStatus && (
            jobStatus === JobStatusUtil.Label.Succeeded ||
            jobStatus === JobStatusUtil.Label.Failed ||
            jobStatus === JobStatusUtil.Label.Cancelled
        )) {
            return { label: "—", tooltip: undefined };
        }
        
        if (!dateIso) return { label: "—", tooltip: undefined };
        
        const date = new Date(dateIso);
        const now = Date.now();
        const dateMs = date.getTime();
        
        // For next planned execution, only show future dates
        if (showOnlyFuture && dateMs <= now) {
            return { label: "—", tooltip: undefined };
        }
        
        if (dateMs > now) {
            const mins = diffMinutes(now, dateMs);
            if (mins <= 59) {
                return { label: `In ${mins}m`, tooltip: formatIso(dateIso) };
            }
            return { label: formatIso(dateIso), tooltip: formatIso(dateIso) };
        } else {
            const minsAgo = Math.max(0, diffMinutes(dateMs, now));
            if (minsAgo <= 59) {
                return { label: `${minsAgo}m ago`, tooltip: formatIso(dateIso) };
            }
            return { label: formatIso(dateIso), tooltip: formatIso(dateIso) };
        }
    }

    function metadataPairs(job: Job): Array<[string, string]> {
        return Object.entries(job.metadata).filter(([k]) => k.startsWith("!"));
    }


    async function refreshNow() {
        if (!refresh) return;

        isRefreshing = true;
        try {
            const cid = clusterId();
            if (!cid) {
                console.log('[Jobs] No cluster ID found');
                return;
            }

            if (lastClusterId !== cid) {
                lastClusterId = cid;
                pageIndex = 0;
            }

            const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

            try {
                const safeOffset = Math.max(0, pageIndex) * pageSize;

                const filters = buildJobsQuery();
                console.log('[Jobs] Filters:', filters);
                console.log('[Jobs] ClusterID:', cid);
                console.log('[Jobs] Offset:', safeOffset, 'PageSize:', pageSize);

                const [jobsCount, apiJobs] = await Promise.all([
                    jm.GET("/{clusterId}/jobs/count", {
                        params: { path: { clusterId: cid }, query: filters }
                    }).then((r) => {
                        console.log('[Jobs] Count response:', r);
                        if (r.error) throw r.error;
                        return r.data as number;
                    }),
                    jm.GET("/{clusterId}/jobs", {
                        params: {
                            path: { clusterId: cid },
                            query: { ...filters, CountLimit: pageSize, Offset: safeOffset }
                        }
                    }).then((r) => {
                        console.log('[Jobs] Jobs response:', r);
                        if (r.error) throw r.error;
                        return r.data as ApiJobModel[];
                    })
                ]);

                console.log('[Jobs] Jobs count:', jobsCount);
                console.log('[Jobs] Jobs data:', apiJobs);
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
                        nextPlanExecutionAt: j.nextPlanExecutionAt ?? undefined,
                        completedAt: j.finalizedAt ?? undefined,
                        workerLane: j.workerLane ?? undefined,
                        worker: j.agentWorkerId ?? undefined,
                        bucketId: j.bucketId ?? undefined,
                        bucketName: undefined
                    };
                });

                // Post-load: resolve bucket names (fetch only IDs not yet in cache)
                const distinctBucketIds = [...new Set(apiJobs.map((j) => j.bucketId).filter((id): id is string => !!id))];
                const missingIds = bucketNameCache.getMissing(cid, distinctBucketIds);
                if (missingIds.length > 0) {
                    try {
                        const apiBuckets = await jm.GET("/{clusterId}/buckets", {
                            params: { path: { clusterId: cid }, query: { BucketIds: missingIds, CountLimit: missingIds.length } }
                        }).then((r) => {
                            if (r.error) throw r.error;
                            return r.data as ApiBucketModel[];
                        });
                        bucketNameCache.populate(cid, apiBuckets);
                    } catch {
                        // cache stays as-is; bucket names may be partially resolved
                    }
                }
                jobs = jobs.map((j) => j.bucketId ? { ...j, bucketName: bucketNameCache.get(cid, j.bucketId) } : j);

                console.log('[Jobs] Mapped jobs:', jobs);
                lastUpdatedAt = new Date();
            } catch (error) {
                console.error('[Jobs] Error fetching jobs:', error);
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

    $: activeFiltersCount =
      (selectedStatuses.length > 0 ? 1 : 0) +
      (selectedJobDefinitionId ? 1 : 0) +
      (selectedSortDirection ? 1 : 0) +
      (filterValues?.scheduledAt ? 1 : 0);
</script>

<div class="min-h-screen bg-base-100">
    <div class="mx-auto max-w-full px-6 py-6">
                <h1 class="text-2xl font-semibold tracking-tight">Jobs</h1>

        <div class="flex items-center justify-between gap-4 mt-4">
            {#key filterKey}
            <div class="flex flex-wrap items-center gap-2">
                <FilterDropdownMulti
                    label="Statuses"
                    options={[
                        { value: String(ApiJobStatus.PendingSave), label: JobStatusUtil.Label.PendingSave },
                        { value: String(ApiJobStatus.OnMaster), label: JobStatusUtil.Label.OnMaster },
                        { value: String(ApiJobStatus.InBucket), label: JobStatusUtil.Label.InBucket },
                        { value: String(ApiJobStatus.Onboarded), label: JobStatusUtil.Label.Onboarded },
                        { value: String(ApiJobStatus.Processing), label: JobStatusUtil.Label.Processing },
                        { value: String(ApiJobStatus.Succeeded), label: JobStatusUtil.Label.Succeeded },
                        { value: String(ApiJobStatus.Queued), label: JobStatusUtil.Label.Queued },
                        { value: String(ApiJobStatus.Failed), label: JobStatusUtil.Label.Failed },
                        { value: String(ApiJobStatus.Cancelled), label: JobStatusUtil.Label.Cancelled },
                        { value: String(ApiJobStatus.Aborted), label: JobStatusUtil.Label.Aborted }
                    ]}
                    values={selectedStatuses.map(String)}
                    on:change={(e) => {
                        selectedStatuses = e.detail.map(Number);
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

                {#if activeFiltersCount > 0}
                    <button
                        type="button"
                        class="btn btn-sm bg-red-200 text-red-900 border border-red-300 hover:bg-red-300"
                        on:click={resetFilters}
                    >
                        Clear filters
                    </button>
                {/if}
            </div>
            {/key}

            <div class="flex items-center gap-2">
                <FilterDropdown
                    label="Sort By"
                    options={[
                        { value: "desc", label: "Recents" },
                        { value: "asc", label: "Olders" }
                    ]}
                    bind:value={selectedSortDirection}
                    on:change={() => {
                        pageIndex = 0;
                        refreshNow();
                    }}
                />
                <Pager
                    bind:pageIndex
                    bind:pageSize
                    totalCount={jobsTotalCount}
                    currentCount={jobs.length}
                    disabled={isRefreshing}
                    showPageSize={true}
                />
                <button class="btn btn-xs" on:click={refreshNow} disabled={isRefreshing}>Refresh</button>
            </div>
        </div>
        <div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
            <div class="overflow-x-auto">
                <table class="table">
                    <thead>
                    <tr class="text-base-content/70">
                        <th>Id</th>
                        <th>Definition Id</th>
                        <th>Metadata</th>
                        <th>Status</th>
                        <th>Failure Attempts</th>
                        <th>Priority</th>
                        <th>Schedule Date</th>
                        <th>Next Planned Execution</th>
                        <th>Finish</th>
                        <th>Worker Lane</th>
                        <th>Bucket</th>
                    </tr>
                    </thead>

                    <tbody>
                    {#if isRefreshing && jobs.length === 0}
                        <tr>
                            <td colspan="11" class="text-center py-8">
                                <span class="loading loading-spinner loading-md"></span>
                                <p class="mt-2 opacity-60">Loading jobs...</p>
                            </td>
                        </tr>
                    {:else if jobs.length === 0}
                        <tr>
                            <td colspan="11" class="text-center py-8">
                                <p class="opacity-60">No jobs found</p>
                            </td>
                        </tr>
                    {:else}
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
                                <span class={`badge badge-sm whitespace-nowrap ${statusBadgeClass(j.status)}`}>
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
                                <span class={`badge badge-sm whitespace-nowrap ${PriorityUtil.getBadgeClass(j.priority)}`}>
                                    {j.priority}
                                </span>
                            </td>

                            <td>
                                {#if formatDateCell(j.scheduledAt).tooltip}
                                    <span class="tooltip tooltip-bottom" data-tip={formatDateCell(j.scheduledAt).tooltip}>
                                        {formatDateCell(j.scheduledAt).label}
                                    </span>
                                {:else}
                                    <span>{formatDateCell(j.scheduledAt).label}</span>
                                {/if}
                            </td>

                            <td>
                                {#if formatDateCell(j.nextPlanExecutionAt, j.status, true).tooltip}
                                    <span class="tooltip tooltip-bottom" data-tip={formatDateCell(j.nextPlanExecutionAt, j.status, true).tooltip}>
                                        {formatDateCell(j.nextPlanExecutionAt, j.status, true).label}
                                    </span>
                                {:else}
                                    <span>{formatDateCell(j.nextPlanExecutionAt, j.status, true).label}</span>
                                {/if}
                            </td>

                            <td>
                                {#if formatDateCell(j.completedAt).tooltip}
                                    <span class="tooltip tooltip-bottom" data-tip={formatDateCell(j.completedAt).tooltip}>
                                        {formatDateCell(j.completedAt).label}
                                    </span>
                                {:else}
                                    <span>{formatDateCell(j.completedAt).label}</span>
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
                    {/if}
                    </tbody>
                </table>

            </div>
        </div>
    </div>
</div>
