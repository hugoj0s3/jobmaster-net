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

	let query = "";
	let statusFilter: "All Statuses" | RecurringScheduleStatusLabel = "All Statuses";
	let typeFilter = "All Job Types";

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

	let pageIndex = 0;
	let pageSize = 12;

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
		return `badge ${RecurringSchedulesStatusUtil.getBadgeClass(r.scheduleStatus)}`;
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

				console.log(schedule);

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

	let createOpen = false;
	let settingsOpen = false;

	let jobName = "";
	let description = "";
	let handler = "";
	let frequency = "";
	let startPaused = false;

	const handlers = [
		"RenewalJobHandler",
		"CleanupOldReportsHandler",
		"BackupDatabaseHandler",
		"HelloJobHandler",
		"InvoiceProcessingHandler"
	];

	const frequencies = [
		"Every minute",
		"Every 5 minutes",
		"Every hour",
		"Every 2 hours",
		"Every 6 hours",
		"Daily at 03:00 AM",
		"Every Monday at 12:00 PM"
	];

	function openCreate() {
		createOpen = true;
	}

	function closeCreate() {
		createOpen = false;
	}

	function openSettings() {
		settingsOpen = true;
	}

	function closeSettings() {
		settingsOpen = false;
	}

	function submitCreate() {
		console.log({ jobName, description, handler, frequency, startPaused });

		jobName = "";
		description = "";
		handler = "";
		frequency = "";
		startPaused = false;

		closeCreate();
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

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl p-6">
		<div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
			<div>
				<h1 class="text-2xl font-semibold">Recurring Schedules</h1>
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

		<div class="mt-6 rounded-2xl bg-base-100/60 shadow-xl backdrop-blur">
			<div class="p-4">
				<label class="input input-bordered flex items-center gap-2 w-full">
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-70" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<circle cx="11" cy="11" r="7"></circle>
						<path d="M21 21l-4.3-4.3"></path>
					</svg>
					<input class="grow" type="text" placeholder="Search schedules..." bind:value={query} />
				</label>
			</div>

			<div class="flex flex-col gap-3 border-t border-base-300/60 px-4 py-3 md:flex-row md:items-center md:justify-between">
				<div class="flex flex-wrap items-center gap-3">
					<select class="select select-bordered select-sm" bind:value={statusFilter}>
						<option>All Statuses</option>
						<option>Active</option>
						<option>Paused</option>
						<option>Inactive</option>
						<option>Completed</option>
						<option>Failed</option>
					</select>

					<select class="select select-bordered select-sm" bind:value={typeFilter}>
						<option>All Job Types</option>
						{#each jobTypes as jt}
							<option value={jt}>{jt}</option>
						{/each}
					</select>

					<button class="btn btn-ghost btn-sm" on:click={clearFilters}>
						Clear filters
						<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-70" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
							<path d="M21 12a9 9 0 1 1-2.64-6.36" />
							<path d="M21 3v6h-6" />
						</svg>
					</button>
				</div>

				<div class="flex items-center gap-2 opacity-70">
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

			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-sm">
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

			<div class="flex items-center justify-between border-t border-base-300/60 px-4 py-3">
				<div class="text-sm opacity-70">{filtered.length} of {rows.length}</div>
				<div />
			</div>
		</div>
	</div>

	{#if createOpen}
		<div class="fixed inset-0 z-50">
			<div class="absolute inset-0 bg-black/60 backdrop-blur-sm" on:click={closeCreate} />

			<div class="absolute inset-0 flex items-center justify-center p-4">
				<div
					class="w-full max-w-3xl rounded-2xl border border-white/10 bg-[#2b2f43]/80 shadow-2xl backdrop-blur"
					on:click|stopPropagation
					role="dialog"
					aria-modal="true"
					aria-label="New Recurring Schedule"
				>
					<div class="flex items-center justify-between px-8 pt-7">
						<h2 class="text-2xl font-semibold text-white/90">New Recurring Schedule</h2>

						<button
							class="btn btn-ghost btn-sm h-9 min-h-9 w-9 rounded-xl bg-white/10 hover:bg-white/15"
							aria-label="Close"
							on:click={closeCreate}
						>
							<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-white/80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M18 6 6 18" />
								<path d="M6 6l12 12" />
							</svg>
						</button>
					</div>

					<div class="px-8 pb-6 pt-6 space-y-6">
						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Job Name <span class="text-pink-400">*</span>
								</span>
							</label>
							<input
								class="input input-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white placeholder:text-white/35"
								placeholder="Enter job name"
								bind:value={jobName}
							/>
						</div>

						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">Description</span>
							</label>
							<textarea
								class="textarea textarea-bordered w-full min-h-[92px] bg-white/5 border-white/15 focus:border-white/30 text-white placeholder:text-white/35"
								placeholder="Enter description (optional)"
								bind:value={description}
							/>
						</div>

						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Select Handler <span class="text-pink-400">*</span>
								</span>
							</label>
							<div class="relative">
								<select
									class="select select-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white"
									bind:value={handler}
								>
									<option value="" disabled selected>Select handler type</option>
									{#each handlers as h}
										<option value={h}>{h}</option>
									{/each}
								</select>

								<div class="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white/60">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</div>
							</div>
						</div>

						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Frequency <span class="text-pink-400">*</span>
								</span>
							</label>

							<div class="relative">
								<select
									class="select select-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white"
									bind:value={frequency}
								>
									<option value="" disabled selected>Select frequency</option>
									{#each frequencies as f}
										<option value={f}>{f}</option>
									{/each}
								</select>

								<div class="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white/60">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</div>
							</div>
						</div>

						<div class="flex items-center gap-3 pt-1">
							<input class="toggle toggle-lg" type="checkbox" bind:checked={startPaused} />
							<span class="text-white/70">Start Paused</span>
						</div>
					</div>

					<div class="flex items-center justify-end gap-3 px-8 pb-7">
						<button class="btn btn-ghost bg-white/10 hover:bg-white/15 text-white/80" on:click={closeCreate}>
							Cancel
						</button>

						<button
							class="btn border-0 bg-fuchsia-500 hover:bg-fuchsia-400 text-white"
							on:click={submitCreate}
							disabled={!jobName || !handler || !frequency}
							class:opacity-60={!jobName || !handler || !frequency}
						>
							Create Schedule
						</button>
					</div>
				</div>
			</div>
		</div>
	{/if}
</div>