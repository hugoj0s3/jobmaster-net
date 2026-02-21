<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
	import { RecurringSchedulesStatusUtil, type RecurringScheduleStatusLabel } from "$lib/helper/recurring-schedules-status-util";
	import Pager from "$lib/components/Pager.svelte";
	import { RecurrenceExpressionTypeId } from "$lib/api/enums";
	import { RecurrenceExpressionUtil } from '$lib/helper/recurrence-expression-util';
	import { goto } from '$app/navigation';
	import { readUrlParams, writeUrlParams, Serializers } from '$lib/helper/url-filters';

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
		scheduleStatus: RecurringScheduleStatusLabel;
		scheduleStatusAgo: string;
		status: RecurringScheduleStatus;
		scheduleType: RecurringScheduleType;
		lastJobStatus?: JobStatusLabel;
	};

	let rows: RecurringScheduleRow[] = [];

	const urlParamDefs = {
		q: { defaultValue: "" },
		status: { defaultValue: "All Statuses" as "All Statuses" | RecurringScheduleStatusLabel },
		type: { defaultValue: "All Job Types" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 12, ...Serializers.number }
	};

	const _initParams = readUrlParams(urlParamDefs);
	let query = _initParams.q;
	let statusFilter: "All Statuses" | RecurringScheduleStatusLabel = _initParams.status;
	let typeFilter = _initParams.type;

	let refreshIntervalSec = 20;
	let lastUpdatedAt = new Date();
	let isRefreshing = false;
	let poller: number | undefined;

	const clusterId = () => $page.params.cluster;

	$: filtered = rows.filter((r) => {
		const q = query.trim().toLowerCase();
		const matchesQuery =
			!q ||
			`${r.jobType} ${r.handler} ${r.description} ${r.frequency} ${r.tz ?? ""}`
				.toLowerCase()
				.includes(q);

		const matchesStatus = statusFilter === "All Statuses" ? true : r.scheduleStatus === statusFilter;

		const matchesType = typeFilter === "All Job Types" ? true : r.jobType === typeFilter;

		return matchesQuery && matchesStatus && matchesType;
	});

	$: jobTypes = Array.from(new Set(rows.map((r) => r.jobType)));

	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			q: query,
			status: statusFilter,
			type: typeFilter,
			page: pageIndex,
			size: pageSize
		});
	}

	$: query, statusFilter, typeFilter, pageIndex, pageSize, syncToUrl();

	let lastFilterKey = "";
	$: {
		const nextKey = `${query}|${statusFilter}|${typeFilter}`;
		if (nextKey !== lastFilterKey) {
			lastFilterKey = nextKey;
			pageIndex = 0;
		}
	}

	$: totalCount = filtered.length;

	$: {
		const maxPageIndex = Math.max(0, Math.ceil(totalCount / pageSize) - 1);
		if (pageIndex > maxPageIndex) pageIndex = maxPageIndex;
		if (pageIndex < 0) pageIndex = 0;
	}

	$: paged = filtered.slice(pageIndex * pageSize, pageIndex * pageSize + pageSize);

	function clearFilters() {
		query = "";
		statusFilter = "All Statuses";
		typeFilter = "All Job Types";
	}

	function scheduleBadge(r: RecurringScheduleRow): string {
		return `badge ${RecurringSchedulesStatusUtil.getBadgeClassByStatus(r.status)}`;
	}

	function lastJobBadge(status: JobStatusLabel): string {
		return `badge ${JobStatusUtil.getBadgeClass(status)}`;
	}

	function mapScheduleStatus(status?: number): RecurringScheduleStatusLabel {
		if (!status) return RecurringSchedulesStatusUtil.Label.Inactive;
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
		const diff = new Date(nextRun).getTime() - Date.now();
		if (diff < 0) return "Overdue";

		const minutes = Math.floor(diff / 60000);
		const hours = Math.floor(minutes / 60);
		const days = Math.floor(hours / 24);

		if (days > 0) return `In ${days} day${days > 1 ? "s" : ""}`;
		if (hours > 0) return `In ${hours} hour${hours > 1 ? "s" : ""}`;
		if (minutes > 0) return `In ${minutes} min`;
		return "In < 1 min";
	}

	function formatTimeAgo(timestamp?: string): string {
		if (!timestamp) return "Never";
		const diff = Date.now() - new Date(timestamp).getTime();

		const minutes = Math.floor(diff / 60000);
		const hours = Math.floor(minutes / 60);
		const days = Math.floor(hours / 24);

		if (days > 0) return `${days} day${days > 1 ? "s" : ""} ago`;
		if (hours > 0) return `${hours} hour${hours > 1 ? "s" : ""} ago`;
		if (minutes > 0) return `${minutes} min ago`;
		return "Just now";
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

				return {
					id: schedule.id ?? "",
					jobType: schedule.jobDefinitionId ?? "Unknown",
					handler: schedule.profileId ?? "Handler",
					description: schedule.metadata?.description ?? schedule.description ?? "",
					expressionTypeId: expressionTypeId,
					frequency: RecurrenceExpressionUtil.formatExpression(expressionTypeId, expression) ?? formatCronExpression(schedule.cronExpression),
					tz: schedule.timeZoneId,
					nextRun: formatNextRun(schedule.nextScheduledAt),
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
	<div class="mx-auto max-w-6xl px-6 py-6">
		<div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
			<div>
				<h1 class="text-3xl font-semibold tracking-tight">Recurring Schedules</h1>
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


			</div>
		</div>

		<div class="mt-6 flex flex-col gap-3">
			<div class="flex flex-wrap items-center gap-3">
				<select class="select select-sm select-bordered" bind:value={statusFilter}>
					<option>All Statuses</option>
					<option>Active</option>
					<option>Inactive</option>
					<option>Completed</option>
					<option>Failed</option>
				</select>

				<select class="select select-sm select-bordered" bind:value={typeFilter}>
					<option>All Job Types</option>
					{#each jobTypes as jt}
						<option value={jt}>{jt}</option>
					{/each}
				</select>
			</div>

			<div class="flex flex-wrap items-center justify-between gap-3">
				<label class="input input-bordered input-sm flex items-center gap-2 w-full sm:w-80">
					<span class="opacity-60">🔎</span>
					<input class="grow" type="text" placeholder="Search schedules..." bind:value={query} />
				</label>

				<Pager
					bind:pageIndex
					bind:pageSize
					totalCount={filtered.length}
					currentCount={paged.length}
					disabled={isRefreshing}
					showPageSize={true}
				/>
			</div>
		</div>

		<div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="overflow-x-auto">
				<table class="table table-zebra">
					<thead>
					<tr class="text-base-content/70">
						<th class="w-[28%]">Job Definition Id</th>
						<th class="w-[28%]">Type</th>
						<th class="w-[18%]">Next Run</th>
						<th class="w-[13%]">Status</th>
						<th class="w-[1%]"></th>
					</tr>
					</thead>

					<tbody>
					{#each paged as r}
						<tr class="hover cursor-pointer" on:click={() => navigateToDetail(r.id)}>
							<td>
								<div class="flex items-center gap-3">
									<div class="h-10 w-10 rounded-xl bg-base-300/60 grid place-items-center">
										<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 opacity-80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
											<path d="M21 8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v1Z" />
											<path d="M21 16a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-1a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v1Z" />
										</svg>
									</div>

									<div class="leading-tight">
										<div class="font-medium">{r.jobType}</div>
										<div class="text-xs opacity-60">{r.handler}</div>
									</div>
								</div>
							</td>

							<td>
								<div class="font-medium opacity-90">{r.expressionTypeId ?? "Unknown"}</div>
							</td>

							<td>
								<div class="leading-tight">
									<div class="font-medium opacity-90">{r.nextRun}</div>
								</div>
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