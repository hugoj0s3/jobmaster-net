<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
	import {
		RecurringSchedulesStatusUtil,
		type RecurringScheduleStatusLabel
	} from '$lib/helper/recurring-schedules-status-util';
	import { RecurrenceExpressionUtil } from '$lib/helper/recurrence-expression-util';
	import Pager from "$lib/components/Pager.svelte";
	import { copyText, createCopyFeedback } from "$lib/helper/clipboard-util";

	type RecurringSchedule = components["schemas"]["RecurringSchedule"];

	const clusterId = () => $page.params.cluster;
	const scheduleId = () => $page.params.id;

	let schedule: RecurringSchedule | null = null;
	let upcomingRuns: any[] = [];
	let isLoading = true;
	let isLoadingUpcoming = false;
	let error: string | null = null;
	let upcomingRunsPage = 0;
	let upcomingRunsPageSize = 10;

	let refreshIntervalSec = 20;
	let poller: number | undefined;

	const copyFeedback = createCopyFeedback({ resetAfterMs: 1200 });
	const copiedId = copyFeedback.copiedId;

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

	function getScheduleStatus(): RecurringScheduleStatusLabel {
		if (schedule?.status == null) return RecurringSchedulesStatusUtil.Label.Inactive;
		try {
			return RecurringSchedulesStatusUtil.getLabel(schedule.status);
		} catch {
			return RecurringSchedulesStatusUtil.Label.Inactive;
		}
	}

	function getLastJobStatus(): JobStatusLabel | null {
		if (!schedule?.lastJobStatus) return null;
		try {
			return JobStatusUtil.getLabel(schedule.lastJobStatus);
		} catch {
			return null;
		}
	}

	function getScheduleBadgeClass(): string {
		return `badge ${RecurringSchedulesStatusUtil.getBadgeClass(statusLabel)}`;
	}

	function getLastJobBadgeClass(status: JobStatusLabel): string {
		return `badge ${JobStatusUtil.getBadgeClass(status)}`;
	}

	async function fetchUpcomingRuns() {
		if (!schedule?.id) return;
		
		isLoadingUpcoming = true;
		try {
			const cid = clusterId();
			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jm.GET("/{clusterId}/jobs", {
				params: {
					path: { clusterId: cid },
					query: { 
						SourceId: schedule.id,
						CountLimit: 10,
						SortByProperty: "ScheduledAt",
						SortByAscending: true
					}
				}
			});

			if (response.error) {
				console.error('Error fetching upcoming runs:', response.error);
				upcomingRuns = [];
			} else {
				upcomingRuns = response.data || [];
			}
		} catch (err) {
			console.error('Error fetching upcoming runs:', err);
			upcomingRuns = [];
		} finally {
			isLoadingUpcoming = false;
		}
	}

	async function refreshSchedule() {
		isLoading = true;
		error = null;

		try {
			const cid = clusterId();
			const sid = scheduleId();
			if (!cid || !sid) {
				error = "Missing cluster ID or schedule ID";
				return;
			}

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jm.GET("/{clusterId}/recurring-schedules/{id}", {
				params: {
					path: { clusterId: cid, id: sid }
				}
			});

			if (response.error) {
				console.error("API error:", response.error);
				error = "Failed to load schedule details";
				return;
			}

			schedule = response.data as any;
			// Fetch upcoming runs after schedule is loaded
			await fetchUpcomingRuns();
		} catch (e) {
			console.error("Refresh failed", e);
			error = "An error occurred while loading the schedule";
		} finally {
			isLoading = false;
		}
	}

	function restartPoller() {
		if (poller) window.clearInterval(poller);
		poller = window.setInterval(() => {
			refreshSchedule();
			fetchUpcomingRuns();
		}, refreshIntervalSec * 1000);
	}

	function goBack() {
		goto(`/${clusterId()}/recurring-schedules`);
	}

	function formatJobDateTime(timestamp?: string): string {
		if (!timestamp) return "—";
		return new Date(timestamp).toLocaleString();
	}

	function formatJobTimeAgo(timestamp?: string): string {
		if (!timestamp) return "Never";
		return formatTimeAgo(timestamp);
	}

	function getJobStatusBadgeClass(status?: number): string {
		if (!status) return "badge-ghost";
		try {
			const statusLabel = JobStatusUtil.getLabel(status);
			return `badge ${JobStatusUtil.getBadgeClass(statusLabel)}`;
		} catch {
			return "badge-ghost";
		}
	}

	function getJobStatusLabel(status?: number): string {
		if (!status) return "—";
		try {
			return JobStatusUtil.getLabel(status);
		} catch {
			return "Unknown";
		}
	}

	async function copyJobId(jobId: string) {
		await copyFeedback.copy(jobId);
	}

	onMount(() => {
		refreshSchedule();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
		copyFeedback.destroy();
	});

	$: scheduleName = schedule?.profileId ?? schedule?.jobDefinitionId ?? "Loading...";
	$: expressionTypeId = schedule?.expressionTypeId ??
		schedule?.expressionType ??
		schedule?.recurrenceExpressionTypeId ??
		schedule?.recurrenceTypeId ??
		undefined;
	$: expression = schedule?.cronExpression ?? schedule?.expression ?? schedule?.recurrenceExpression;
	$: scheduleExpression = RecurrenceExpressionUtil.formatExpression(expressionTypeId, expression);
	$: description = schedule?.metadata?.description ?? schedule?.description ?? "—";
	$: nextRunFormatted = formatNextRun(schedule?.nextScheduledAt);
	$: lastRunAgo = formatTimeAgo(schedule?.lastJobExecutedAt);
	$: statusLabel = schedule ? getScheduleStatus() : RecurringSchedulesStatusUtil.Label.Inactive;
	$: lastJobStatusLabel = schedule?.lastJobStatus ? JobStatusUtil.getLabel(schedule.lastJobStatus) : null;

	// Pagination for upcoming runs
	$: paginatedUpcomingRuns = upcomingRuns.slice(
		upcomingRunsPage * upcomingRunsPageSize, 
		(upcomingRunsPage + 1) * upcomingRunsPageSize
	);
	$: upcomingRunsTotalCount = upcomingRuns.length;
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		{#if isLoading && !schedule}
			<div class="flex items-center justify-center py-20">
				<span class="loading loading-spinner loading-lg"></span>
			</div>
		{:else if error && !schedule}
			<div class="alert alert-error">
				<svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
					<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" />
				</svg>
				<span>{error}</span>
			</div>
		{:else if schedule}
			<!-- Top: back + title + actions -->
			<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
				<div class="space-y-2">
					<div class="text-sm breadcrumbs">
						<ul>
							<li><a href="/{clusterId()}/recurring-schedules" class="link link-hover">Recurring Schedules</a></li>
							<li>{scheduleName}</li>
						</ul>
					</div>

					<h1 class="text-3xl font-semibold tracking-tight">{scheduleName}</h1>
				</div>


			</div>

			<!-- Content grid -->
			<div class="mt-6 grid grid-cols-1 gap-6">
				<!-- Full width content -->
				<div class="space-y-6">
					<!-- Details card -->
					<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
						<div class="flex items-center justify-between px-6 pt-5 pb-2">
							<h2 class="text-lg font-semibold">Details</h2>
							<span class={getScheduleBadgeClass()}>{statusLabel}</span>
						</div>

						<div class="px-6 pb-5">
							<div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
								<div class="space-y-3">
									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Description</div>
										<div class="font-medium">{description}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Expression</div>
										<div class="font-medium text-primary">{scheduleExpression}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Timezone</div>
										<div class="font-medium">{schedule.timeZoneId ?? "—"}</div>
									</div>
								</div>

								<div class="space-y-3">
									{#if lastJobStatusLabel}
										<div class="flex items-center justify-between gap-4">
											<div class="text-sm opacity-70">Last Job Status</div>
											<div class="flex items-center gap-2">
												<span class={getLastJobBadgeClass()}>{lastJobStatusLabel}</span>
												<span class="text-sm opacity-70">{lastRunAgo}</span>
											</div>
										</div>
									{/if}

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Job Definition</div>
										<div class="font-medium">{schedule.jobDefinitionId ?? "—"}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Status</div>
										<div class="flex items-center gap-2">
											<span class="h-2 w-2 rounded-full {statusLabel === 'Active' ? 'bg-success' : statusLabel === 'Inactive' ? 'bg-warning' : 'bg-ghost'}"></span>
											<span class="font-medium">{statusLabel}</span>
										</div>
									</div>
								</div>
							</div>
						</div>
					</div>

					<!-- Message Data and Metadata cards in 2-column layout -->
					<div class="grid grid-cols-1 gap-6 lg:grid-cols-2">
						<!-- Message Data card -->
						<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
							<div class="flex items-center justify-between px-6 pt-5 pb-2">
								<h2 class="text-lg font-semibold">Message Data</h2>
							</div>

							<div class="px-6 pb-5">
								{#if schedule?.messageData}
									<div class="space-y-2">
										{#each Object.entries(schedule.messageData) as [key, value]}
											<div class="flex items-center justify-between gap-4">
												<div class="text-sm opacity-70 font-mono">{key}</div>
												<div class="font-medium text-right break-all">{String(value)}</div>
											</div>
										{/each}
									</div>
								{:else}
									<div class="text-center opacity-70">
										<p>No message data available</p>
									</div>
								{/if}
							</div>
						</div>

						<!-- Metadata card -->
						<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
							<div class="flex items-center justify-between px-6 pt-5 pb-2">
								<h2 class="text-lg font-semibold">Metadata</h2>
							</div>

							<div class="px-6 pb-5">
								{#if schedule?.metadata}
									<div class="space-y-2">
										{#each Object.entries(schedule.metadata) as [key, value]}
											<div class="flex items-center justify-between gap-4">
												<div class="text-sm opacity-70 font-mono">{key}</div>
												<div class="font-medium text-right break-all">{String(value)}</div>
											</div>
										{/each}
									</div>
								{:else}
									<div class="text-center opacity-70">
										<p>No metadata available</p>
									</div>
								{/if}
							</div>
						</div>
					</div>

					<!-- Upcoming runs -->
					<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
						<div class="card-body">
							<div class="flex items-center justify-between gap-4">
								<h2 class="card-title text-base">Upcoming Runs</h2>
								<div class="flex items-center gap-4">
									{#if isLoadingUpcoming}
										<span class="loading loading-spinner loading-sm"></span>
									{/if}
									<Pager
										bind:pageIndex={upcomingRunsPage}
										bind:pageSize={upcomingRunsPageSize}
										totalCount={upcomingRunsTotalCount}
										currentCount={paginatedUpcomingRuns.length}
										disabled={isLoadingUpcoming}
										showPageSize={true}
									/>
								</div>
							</div>

							{#if isLoadingUpcoming}
								<div class="text-center opacity-70 py-8">
									<p>Loading upcoming runs...</p>
								</div>
							{:else if upcomingRuns.length === 0}
								<div class="text-center opacity-70 py-8">
									<p>No upcoming runs found</p>
								</div>
							{:else}
								<div class="overflow-x-auto">
									<table class="table table-zebra table-sm">
										<thead>
											<tr class="text-base-content/70">
												<th>Job ID</th>
												<th>Status</th>
												<th>Scheduled At</th>
												<th>Priority</th>
											</tr>
										</thead>
										<tbody>
											{#each paginatedUpcomingRuns as job (job.id)}
												<tr>
													<td>
														<div class="flex items-center gap-2">
															<span class="tooltip tooltip-bottom" data-tip={job.id}>
																<a
																	class="link link-hover"
																	href={`/${clusterId()}/jobs/${job.id}`}
																	aria-label={`Open job ${job.id}`}
																>
																	{job.id}
																</a>
															</span>

															<button
																class="btn btn-ghost btn-xs btn-square opacity-40 hover:opacity-100"
																aria-label="Copy Job ID"
																on:click|preventDefault|stopPropagation={() => copyFeedback.copy(job.id)}
															>
																{#if $copiedId === job.id}
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
													<td>
														<span class={getJobStatusBadgeClass(job.status)}>
															{getJobStatusLabel(job.status)}
														</span>
													</td>
													<td>
														<div class="font-medium">{formatJobDateTime(job.scheduledAt)}</div>
														<div class="text-xs opacity-60">{formatJobTimeAgo(job.scheduledAt)}</div>
													</td>
													<td class="font-mono text-sm">{job.priority ?? "—"}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>