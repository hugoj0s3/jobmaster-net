<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
	import { RecurringSchedulesStatusUtil, type RecurringScheduleStatusLabel } from "$lib/helper/recurring-schedules-status-util";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
	import FilterItem from "$lib/components/filters/FilterItem.svelte";
	import { RecurrenceExpressionTypeId } from "$lib/api/enums";
	import { RecurrenceExpressionUtil } from '$lib/helper/recurrence-expression-util';
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { goto } from '$app/navigation';
	import { readUrlParams, writeUrlParams, Serializers } from '$lib/helper/url-filters';
	import { parseDatetimeParam, datetimeToParam, passesDatetimeFilter, type DatetimeFilterValue } from '$lib/helper/datetime-filter-url';

	type RecurringScheduleStatus = components["schemas"]["RecurringScheduleStatus"];
	type RecurringScheduleType = components["schemas"]["RecurringScheduleType"];

	type RecurringScheduleRow = {
		id: string;
		jobType: string;
		handler: string;
		description: string;
		expressionTypeId?: string;
		frequency: string;
		tz?: string;
		nextRun: string;
		nextScheduledAtRaw?: string;
		createdAtRaw?: string;
		metadata: Record<string, string>;
		scheduleStatus: RecurringScheduleStatusLabel;
		scheduleStatusAgo: string;
		status: RecurringScheduleStatus;
		scheduleType: RecurringScheduleType;
		lastJobStatus?: JobStatusLabel;
	};

	let rows: RecurringScheduleRow[] = [];

	const urlParamDefs = {
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number },
		sortDirection: { defaultValue: "desc" as "asc" | "desc" },
		createdAt: { defaultValue: "" as string }
	};

	let _initParams = readUrlParams(urlParamDefs);

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		sortDirection = _initParams.sortDirection;
		selectedStatuses = [];
		filterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");
		refreshNow();
	}

	let refreshIntervalSec = 20;
	let lastUpdatedAt = new Date();
	let isRefreshing = false;
	let poller: number | undefined;

	const clusterId = () => $page.params.cluster;

	let selectedStatuses: string[] = [];
	let sortDirection: "asc" | "desc" = _initParams.sortDirection;

	type FilterValues = Record<string, unknown>;
	let filterValues: FilterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");

	$: createdAtFilter = (filterValues.createdAt ?? {}) as DatetimeFilterValue;

	$: filtered = rows.filter((r) => {
		if (selectedStatuses.length > 0 && !selectedStatuses.includes(r.scheduleStatus)) return false;
		if (!passesDatetimeFilter(createdAtFilter, r.createdAtRaw)) return false;
		return true;
	});

	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			page: pageIndex,
			size: pageSize,
			sortDirection,
			createdAt: datetimeToParam(filterValues, "createdAt")
		});
	}

	$: filterValues, sortDirection, pageIndex, pageSize, syncToUrl();

	function resetFilters() {
		selectedStatuses = [];
		filterValues = {};
		sortDirection = "desc";
		pageIndex = 0;
	}

	$: totalCount = filtered.length;

	$: {
		const maxPageIndex = Math.max(0, Math.ceil(totalCount / pageSize) - 1);
		if (pageIndex > maxPageIndex) pageIndex = maxPageIndex;
		if (pageIndex < 0) pageIndex = 0;
	}

	$: sorted = filtered.slice().sort((a, b) => {
		const dir = sortDirection === "asc" ? 1 : -1;
		const safeTime = (iso: string | undefined) => {
			if (!iso) return Number.MIN_SAFE_INTEGER;
			const t = new Date(iso).getTime();
			return Number.isFinite(t) ? t : Number.MIN_SAFE_INTEGER;
		};
		return (safeTime(a.createdAtRaw) - safeTime(b.createdAtRaw)) * dir;
	});

	$: paged = sorted.slice(pageIndex * pageSize, pageIndex * pageSize + pageSize);
	$: activeFiltersCount =
		(selectedStatuses.length > 0 ? 1 : 0) +
		(filterValues?.createdAt ? 1 : 0) +
		(sortDirection !== "desc" ? 1 : 0);

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

	function metadataPairs(r: RecurringScheduleRow): Array<[string, string]> {
		return Object.entries(r.metadata).filter(([k]) => k.startsWith("!"));
	}

	function scheduleBadge(r: RecurringScheduleRow): string {
		return `badge ${RecurringSchedulesStatusUtil.getBadgeClassByStatus(r.status)}`;
	}

	function lastJobBadge(status: JobStatusLabel): string {
		return `badge ${JobStatusUtil.getBadgeClass(status)}`;
	}

	function mapScheduleStatus(status?: number | string): RecurringScheduleStatusLabel {
		if (status == null) return RecurringSchedulesStatusUtil.Label.Inactive;
		try {
			return RecurringSchedulesStatusUtil.getLabel(status);
		} catch {
			return RecurringSchedulesStatusUtil.Label.Inactive;
		}
	}

	function mapLastJobStatus(status?: number): JobStatusLabel | undefined {
		if (!status) return undefined;
		try {
			return JobStatusUtil.getLabel(status);
		} catch {
			return undefined;
		}
	}

	function formatCronExpression(cron?: string): string {
		if (!cron) return "Unknown";
		return cron;
	}

	function formatNextRun(nextRun?: string): string {
		if (!nextRun) return "—";
		return DateDisplayUtil.formatRelativeOrDate(nextRun);
	}

	function formatTimeAgo(timestamp?: string): string {
		if (!timestamp) return "Never";
		return DateDisplayUtil.formatRelativeOrDate(timestamp);
	}

	function navigateToDetail(scheduleId: string) {
		goto(`/${clusterId()}/recurring-schedules/${scheduleId}`);
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jm.GET("/{clusterId}/recurring-schedules", {
				params: {
					path: { clusterId: cid },
					query: {
						CountLimit: 1000
					}
				}
			});

			if (response.error) {
				console.error("API error:", response.error);
				return;
			}

			const apiSchedules = (response.data as any) || [];

			rows = apiSchedules.map((schedule: any) => {
				const expressionTypeId =
					schedule.expressionTypeId ??
					schedule.expressionType ??
					schedule.recurrenceExpressionTypeId ??
					schedule.recurrenceTypeId ??
					undefined;

				const expression =
					schedule.cronExpression ??
					schedule.expression ??
					schedule.recurrenceExpression ??
					schedule.scheduleExpression ??
					undefined;

				const meta = stringifyMetadata(schedule.metadata);

				return {
					id: schedule.id ?? "",
					jobType: schedule.jobDefinitionId ?? "Unknown",
					handler: schedule.profileId ?? "Handler",
					description: meta["description"] ?? schedule.description ?? "",
					metadata: meta,
					expressionTypeId: expressionTypeId,
					frequency: RecurrenceExpressionUtil.formatExpression(expressionTypeId, expression) ?? formatCronExpression(schedule.cronExpression),
					tz: schedule.timeZoneId,
					nextRun: formatNextRun(schedule.nextScheduledAt),
					nextScheduledAtRaw: schedule.nextScheduledAt ?? undefined,
					createdAtRaw: schedule.createdAt ?? undefined,
					scheduleStatus: mapScheduleStatus(schedule.status),
					scheduleStatusAgo: formatTimeAgo(schedule.lastJobExecutedAt),
					status: schedule.status ?? 3,
					scheduleType: schedule.scheduleType ?? 2,
					lastJobStatus: mapLastJobStatus(schedule.lastJobStatus)
				};
			});

			lastUpdatedAt = new Date();
		} catch (e) {
			console.error("Refresh failed", e);
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
		refreshNow();
		restartPoller();

		return () => {
			if (poller) window.clearInterval(poller);
		};
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
			<div>
				<h1 class="text-3xl font-semibold tracking-tight">Recurring Schedules</h1>
			</div>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last Refresh: {DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt)}</span>
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

		<div class="mt-6 flex items-center justify-between gap-4">
			{#key filterKey}
			<div class="flex flex-wrap items-center gap-2">
				<FilterDropdownMulti
					label="Status"
					options={[
						{ value: "Active", label: "Active" },
						{ value: "Inactive", label: "Inactive" },
						{ value: "Completed", label: "Completed" },
						{ value: "Failed", label: "Failed" }
					]}
					bind:values={selectedStatuses}
					on:change={() => { pageIndex = 0; }}
				/>

				<FilterDropdown
					label="Sort By"
					options={[
						{ value: "desc", label: "Recents" },
						{ value: "asc", label: "Olders" }
					]}
					value={sortDirection}
					on:change={(e) => {
						sortDirection = (e.detail as "asc" | "desc") ?? "desc";
						pageIndex = 0;
					}}
				/>


				<FilterContainer
					initialValues={filterValues}
					onChange={(v) => {
						filterValues = v;
						pageIndex = 0;
					}}
				>
					<FilterItem
						id="createdAt"
						label="Created At"
						type="datetime"
						presets={[
							{ type: "LAST_MINUTES", minutes: 60, label: "Last 1 hour" },
							{ type: "LAST_MINUTES", minutes: 1440, label: "Last 24 hours" }
						]}
					/>
				</FilterContainer>

				{#if activeFiltersCount > 0}
					<button
						type="button"
						class="btn btn-sm btn-ghost"
						on:click={resetFilters}
					>
						Reset filters
					</button>
				{/if}
			</div>
			{/key}

			<Pager
				bind:pageIndex
				bind:pageSize
				totalCount={filtered.length}
				currentCount={paged.length}
				disabled={isRefreshing}
				showPageSize={true}
			/>
		</div>

		<div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="overflow-x-auto">
				<table class="table table-zebra">
					<thead>
					<tr class="text-base-content/70">
						<th class="w-[24%]">Job Definition Id</th>
						<th class="w-[20%]">Type</th>
						<th>Metadata</th>
						<th class="w-[13%]">Status</th>
					</tr>
					</thead>

					<tbody>
					{#each paged as r (r.id)}
						<tr class="hover cursor-pointer" on:click={() => navigateToDetail(r.id)}>
							<td>
								<div class="flex items-center gap-3">
									<div class="h-10 w-10 rounded-xl bg-base-300/60 grid place-items-center">
										<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 opacity-80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
											<path d="M8 2v4M16 2v4M3 10h18M5 4h14a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2z" />
											<rect x="8" y="14" width="2" height="2" rx="1" />
											<rect x="14" y="14" width="2" height="2" rx="1" />
										</svg>
									</div>
									<div class="leading-tight">
										<div class="font-medium">{r.jobType}</div>
									</div>
								</div>
							</td>

							<td>
								<div class="font-medium opacity-90">{r.expressionTypeId ?? "Unknown"}</div>
							</td>

							<td>
								{#if metadataPairs(r).length === 0}
									<span class="opacity-60">—</span>
								{:else}
									<div class="flex flex-wrap gap-2">
										{#each metadataPairs(r) as [k, v] (k)}
											<span class="badge badge-ghost badge-sm">{k}={v}</span>
										{/each}
									</div>
								{/if}
							</td>

							<td>
								<div class="flex flex-col gap-1">
									<div class="flex items-center gap-2">
										<span class={scheduleBadge(r)}>{r.scheduleStatus}</span>
									</div>
									{#if r.lastJobStatus}
										<div class="flex items-center gap-2">
											<span class={lastJobBadge(r.lastJobStatus)} style="font-size: 0.7rem;">{r.lastJobStatus}</span>
											<span class="text-xs opacity-60">{r.scheduleStatusAgo}</span>
										</div>
									{/if}
								</div>
							</td>
						</tr>
					{/each}

					{#if filtered.length === 0}
						<tr>
							<td colspan="6">
								<div class="py-8 text-center opacity-70">No schedules found.</div>
							</td>
						</tr>
					{/if}
					</tbody>
				</table>
			</div>

			</div>
	</div>

</div>
