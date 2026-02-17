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

	type RecurringSchedule = components["schemas"]["RecurringSchedule"];

	const clusterId = () => $page.params.cluster;
	const scheduleId = () => $page.params.id;

	let schedule: RecurringSchedule | null = null;
	let isLoading = true;
	let error: string | null = null;

	let refreshIntervalSec = 20;
	let poller: number | undefined;

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
		if (!schedule?.status) return RecurringSchedulesStatusUtil.Label.Inactive;
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
		}, refreshIntervalSec * 1000);
	}

	function goBack() {
		goto(`/${clusterId()}/recurring-schedules`);
	}

	onMount(() => {
		refreshSchedule();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
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
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl p-6">
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

				<div class="flex flex-wrap items-center gap-3">
					<button class="btn btn-primary btn-sm">
						<span class="inline-flex items-center gap-2">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
								<path d="M8 5v14l11-7z" />
							</svg>
							Run Now
						</span>
					</button>

					<button class="btn btn-warning btn-sm">
						<span class="inline-flex items-center gap-2">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
								<path d="M6 5h4v14H6zM14 5h4v14h-4z" />
							</svg>
							Pause
						</span>
					</button>

					<button class="btn btn-error btn-outline btn-sm">
						<span class="inline-flex items-center gap-2">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M3 6h18" />
								<path d="M8 6V4h8v2" />
								<path d="M19 6l-1 14H6L5 6" />
								<path d="M10 11v6M14 11v6" />
							</svg>
							Delete
						</span>
					</button>
				</div>
			</div>

			<!-- Content grid -->
			<div class="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
				<!-- Left column -->
				<div class="lg:col-span-2 space-y-6">
					<!-- Details card -->
					<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
						<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
							<h2 class="text-lg font-semibold">Details</h2>
							<span class={getScheduleBadgeClass()}>{statusLabel}</span>
						</div>

						<div class="p-6">
							<div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
								<div class="space-y-3">
									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Handler</div>
										<div class="font-medium">{schedule.profileId ?? "—"}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Description</div>
										<div class="font-medium">{description}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Schedule</div>
										<div class="font-medium text-primary">{scheduleExpression}</div>
									</div>

									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Timezone</div>
										<div class="font-medium">{schedule.timeZoneId ?? "—"}</div>
									</div>
								</div>

								<div class="space-y-3">
									<div class="flex items-center justify-between gap-4">
										<div class="text-sm opacity-70">Next Run</div>
										<div class="font-medium text-warning">{nextRunFormatted}</div>
									</div>

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

					<!-- Upcoming runs placeholder -->
					<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
						<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
							<h2 class="text-lg font-semibold">Upcoming Runs</h2>
						</div>

						<div class="p-6 text-center opacity-70">
							<p>Upcoming runs information not available in current API response</p>
						</div>
					</div>
				</div>

				<!-- Right column: Recent activity placeholder -->
				<div class="space-y-6">
					<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
						<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
							<h2 class="text-lg font-semibold">Recent Activity</h2>
						</div>

						<div class="p-6 text-center opacity-70">
							<p>Recent activity information not available in current API response</p>
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>