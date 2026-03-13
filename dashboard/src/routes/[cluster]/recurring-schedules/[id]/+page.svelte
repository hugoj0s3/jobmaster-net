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

					<!-- Upcoming runs placeholder -->
					<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
						<div class="flex items-center justify-between px-6 pt-5 pb-2">
							<h2 class="text-lg font-semibold">Upcoming Runs</h2>
						</div>

						<div class="px-6 pb-5 text-center opacity-70">
							<p>Upcoming runs information not available in current API response</p>
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>