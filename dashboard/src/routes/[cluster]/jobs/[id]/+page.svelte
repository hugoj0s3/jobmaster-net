<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
	import { PriorityUtil, type PriorityLabel } from "$lib/helper/priority-util";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";

	type ApiJobModel = components["schemas"]["ApiJobModel"];

	const clusterId = () => $page.params.cluster;
	const jobId = () => $page.params.id;

	type LogEntry = {
		id?: string;
		timestamp?: string;
		level?: number;
		message?: string;
		subjectType?: number;
		subjectId?: string;
		exceptionMessage?: string;
		exceptionStackTrace?: string;
	};

	let job: ApiJobModel | null = null;
	let recentLogs: LogEntry[] = [];
	let filteredLogs: LogEntry[] = [];
	let selectedLogLevel: string = "";
	let isLoading = true;
	let refreshError: string | null = null;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 15;

	$: statusLabel = safeStatusLabel(job?.status);
	$: priorityLabel = safePriorityLabel(job?.priority);

	$: filteredLogs = selectedLogLevel 
		? recentLogs.filter(log => log.level === parseInt(selectedLogLevel))
		: recentLogs;

	function safeStatusLabel(status: number | null | undefined): JobStatusLabel | "—" {
		try {
			return JobStatusUtil.getLabel(status ?? null);
		} catch {
			return "—";
		}
	}

	function safePriorityLabel(priority: number | null | undefined): PriorityLabel | "—" {
		try {
			return PriorityUtil.getLabel(priority ?? null);
		} catch {
			return "—";
		}
	}

	function statusBadgeClass(label: JobStatusLabel | "—"): string {
		if (label === "—") return "badge-ghost";
		return JobStatusUtil.getBadgeClass(label);
	}

	function priorityBadgeClass(label: PriorityLabel | "—"): string {
		if (label === "—") return "badge-ghost";
		return PriorityUtil.getBadgeClass(label);
	}

	function formatDateTime(iso: string | null | undefined): string {
		return DateDisplayUtil.formatRelativeOrDate(iso);
	}

	function formatDuration(startIso: string | null | undefined, endIso: string | null | undefined): string {
		if (!startIso || !endIso) return "—";
		const ms = new Date(endIso).getTime() - new Date(startIso).getTime();
		if (!Number.isFinite(ms) || ms < 0) return "—";
		if (ms < 1000) return `${Math.round(ms)}ms`;
		if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
	}

	function metadataEntries(meta: Record<string, unknown> | null | undefined): Array<[string, string]> {
		if (!meta) return [];
		return Object.entries(meta).map(([k, v]) => {
			if (typeof v === "string") return [k, v];
			if (v === null || v === undefined) return [k, "—"];
			try { return [k, JSON.stringify(v)]; } catch { return [k, String(v)]; }
		});
	}

	function msgDataEntries(data: Record<string, unknown> | null | undefined): Array<[string, string]> {
		if (!data) return [];
		return Object.entries(data).map(([k, v]) => {
			if (typeof v === "string") return [k, v];
			if (v === null || v === undefined) return [k, "—"];
			try { return [k, JSON.stringify(v)]; } catch { return [k, String(v)]; }
		});
	}

	$: lastUpdated = DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt);

	async function refreshNow() {
		isLoading = true;
		refreshError = null;
		try {
			const cid = clusterId();
			const jid = jobId();
			if (!cid || !jid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jm.GET("/{clusterId}/jobs/{id}", {
				params: { path: { clusterId: cid, id: jid } }
			});

			if (response.error) {
				console.error("API error (job detail):", response.error);
				refreshError = "Failed to load job details.";
				return;
			}

			job = (response.data ?? null) as ApiJobModel | null;

			try {
				const logsResp = await jm.GET("/{clusterId}/logs", {
					params: {
						path: { clusterId: cid },
						query: { SubjectId: jid, CountLimit: 20 }
					}
				});
				if (!logsResp.error) {
					const allLogs = (logsResp.data ?? []) as LogEntry[];
					recentLogs = allLogs.filter((l) => l.level != null && l.level >= 3);
				} else {
					recentLogs = [];
				}
			} catch {
				recentLogs = [];
			}

			lastUpdatedAt = new Date();
		} catch (e) {
			console.error("Failed to fetch job:", e);
			refreshError = e instanceof Error ? e.message : String(e);
		} finally {
			isLoading = false;
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
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		{#if isLoading && !job}
			<div class="flex items-center justify-center py-20">
				<span class="loading loading-spinner loading-lg"></span>
			</div>
		{:else if refreshError && !job}
			<div class="alert alert-error">
				<span>{refreshError}</span>
			</div>
		{:else if job}
			<!-- Header -->
			<div class="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
				<div class="space-y-2">
					<div class="text-sm breadcrumbs opacity-70">
						<ul>
							<li><a href="/{clusterId()}/jobs" class="link link-hover">Jobs</a></li>
							<li>{job.id ?? "—"}</li>
						</ul>
					</div>

					<h1 class="text-3xl font-semibold tracking-tight">Job Detail</h1>

					<div class="flex flex-wrap items-center gap-2">
						<span class={"badge " + statusBadgeClass(statusLabel)}>{statusLabel}</span>
						<span class={"badge " + priorityBadgeClass(priorityLabel)}>{priorityLabel}</span>
						{#if job.workerLane}
							<span class="badge badge-ghost">{job.workerLane}</span>
						{/if}
					</div>
				</div>

				<div class="flex items-center gap-3 text-sm opacity-80">
					<span>Last updated: {lastUpdated}</span>
					<button
						class="btn btn-ghost btn-sm btn-square"
						aria-label="Refresh now"
						on:click={refreshNow}
						disabled={isLoading}
					>
						<svg
							xmlns="http://www.w3.org/2000/svg"
							class={"h-4 w-4 " + (isLoading ? "animate-spin" : "")}
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

			{#if refreshError}
				<div class="alert alert-warning mt-4 text-sm">
					<span>{refreshError}</span>
				</div>
			{/if}

			<!-- Main content grid -->
			<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
				<!-- Identity -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Identity</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Job ID</span>
								<span class="font-mono font-medium">{job.id ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Definition ID</span>
								<span class="font-medium">{job.jobDefinitionId ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Source ID</span>
								<span class="font-medium">{job.sourceId ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Cluster</span>
								<span class="font-medium">{job.clusterId ?? clusterId()}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Status & Scheduling -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Status & Scheduling</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Status</span>
								<span class={"badge " + statusBadgeClass(statusLabel)}>{statusLabel}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Priority</span>
								<span class={"badge " + priorityBadgeClass(priorityLabel)}>{priorityLabel}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Scheduled At</span>
								<span class="font-medium">{formatDateTime(job.originalScheduledAt ?? job.scheduledAt)}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Created At</span>
								<span class="font-medium">{formatDateTime(job.createdAt)}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Message Data (full width) -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
					<div class="card-body">
						<h2 class="card-title text-base">Message Data</h2>
						<div class="divider my-2"></div>
						{#if msgDataEntries(job.msgData).length === 0}
							<div class="text-sm opacity-60">No message data.</div>
						{:else}
							<div class="overflow-x-auto">
								<table class="table table-zebra">
									<thead>
									<tr class="text-base-content/70">
										<th>Key</th>
										<th>Value</th>
									</tr>
									</thead>
									<tbody>
									{#each msgDataEntries(job.msgData) as [k, v] (k)}
										<tr>
											<td class="font-medium opacity-80">{k}</td>
											<td class="font-mono break-all">{v}</td>
										</tr>
									{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				</div>

				<!-- Metadata -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Metadata</h2>
						<div class="divider my-2"></div>
						{#if metadataEntries(job.metadata).length === 0}
							<div class="text-sm opacity-60">No metadata.</div>
						{:else}
							<div class="space-y-2 text-sm">
								{#each metadataEntries(job.metadata) as [k, v] (k)}
									<div class="flex items-start justify-between gap-4">
										<span class="opacity-70 shrink-0">{k}</span>
										<span class="font-mono text-right break-all">{v}</span>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				</div>

				<!-- Infrastructure -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Infrastructure</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Bucket ID</span>
								<span class="font-mono font-medium">
									{#if job.bucketId}
										<a href="/{clusterId()}/buckets/{job.bucketId}" class="link link-hover link-primary">{job.bucketId}</a>
									{:else}
										—
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Agent Connection</span>
								<span class="font-mono font-medium">
									{#if job.agentConnectionId}
										<a href="/{clusterId()}/agent-connections/{job.agentConnectionId}" class="link link-hover link-primary">{job.agentConnectionId}</a>
									{:else}
										—
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Agent Worker ID</span>
								<span class="font-mono font-medium">{job.agentWorkerId ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Host</span>
								<span class="font-medium">
									{#if job.hostId}
										<a href="/{clusterId()}/hosts/{job.hostId}" class="link link-hover link-primary">{job.hostDisplayName ?? "—"}</a>
									{:else}
										{job.hostDisplayName ?? "—"}
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Worker Lane</span>
								<span class="font-medium">{job.workerLane ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Trigger Source</span>
								<span class="font-medium">{job.triggerSourceType ?? "—"}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Failure & Retries -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Failure & Retries</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Number of Failures</span>
								<span class="font-semibold">{job.numberOfFailures ?? 0}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Max Retries</span>
								<span class="font-semibold">{job.maxNumberOfRetries ?? 0}</span>
							</div>
							{#if typeof job.numberOfFailures === "number" && typeof job.maxNumberOfRetries === "number" && job.maxNumberOfRetries > 0}
								<progress
									class="progress progress-error w-full"
									value={job.numberOfFailures}
									max={job.maxNumberOfRetries}
								></progress>
							{/if}
							{#if job.scheduledAt && typeof job.numberOfFailures === "number" && job.numberOfFailures > 0 && typeof job.maxNumberOfRetries === "number" && job.numberOfFailures < job.maxNumberOfRetries}
								<div class="flex items-center justify-between gap-4">
									<span class="opacity-70">Retrying at</span>
									<span class="font-medium text-warning">{formatDateTime(job.scheduledAt)}</span>
								</div>
							{/if}
						</div>
					</div>
				</div>

				<!-- Execution -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
					<div class="card-body">
						<h2 class="card-title text-base">Execution</h2>
						<div class="divider my-2"></div>
						<div class="overflow-x-auto">
							<table class="table table-zebra">
								<thead>
								<tr class="text-base-content/70">
									<th>Processing Started</th>
									<th>Succeeded / Executed At</th>
									<th>Process Deadline</th>
									<th>Run Duration</th>
									<th>Timeout</th>
								</tr>
								</thead>
								<tbody>
								<tr>
									<td class="font-medium">{formatDateTime(job.processingStartedAt)}</td>
									<td class="font-medium">{formatDateTime(job.succeedExecutedAt)}</td>
									<td class="font-medium">{formatDateTime(job.processDeadline)}</td>
									<td class="font-medium">{formatDuration(job.processingStartedAt, job.succeedExecutedAt)}</td>
									<td class="font-mono font-medium">{job.timeout ?? "—"}</td>
								</tr>
								</tbody>
							</table>
						</div>
					</div>
				</div>

				<!-- Recents Logs -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
					<div class="card-body">
						<div class="flex items-center justify-between gap-4">
							<h2 class="card-title text-base">Recents Logs</h2>
							<FilterDropdown
								label="Log Level"
								options={[
									{ value: "4", label: "Error" },
									{ value: "3", label: "Warning" },
									{ value: "2", label: "Info" },
									{ value: "1", label: "Debug" }
								]}
								bind:value={selectedLogLevel}
							/>
						</div>
						<div class="divider my-2"></div>
						{#if filteredLogs.length === 0}
							<div class="text-sm opacity-60">
								{#if selectedLogLevel}
									No logs match the selected log level.
								{:else}
									No recent failure or error logs.
								{/if}
							</div>
						{:else}
							<div class="overflow-x-auto max-h-80 overflow-y-auto">
								<table class="table table-zebra">
									<thead class="sticky top-0 bg-base-200 z-10">
									<tr class="text-base-content/70">
										<th>Level</th>
										<th>Timestamp</th>
										<th>Message</th>
									</tr>
									</thead>
									<tbody>
									{#each filteredLogs as log (log.id ?? log.timestamp ?? Math.random())}
										<tr>
											<td>
												<span class={"badge badge-sm " + (log.level === 4 ? "badge-error" : log.level === 3 ? "badge-warning" : "badge-ghost")}>
													{log.level === 4 ? "Error" : log.level === 3 ? "Warning" : "Level " + (log.level ?? "?")}
												</span>
											</td>
											<td class="font-medium whitespace-nowrap">{formatDateTime(log.timestamp)}</td>
											<td class="font-mono text-xs break-all">
												{log.message ?? "—"}
												{#if log.exceptionMessage}
													<div class="mt-1 text-error/80">{log.exceptionMessage}</div>
												{/if}
											</td>
										</tr>
									{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				</div>

			</div>
		{/if}
	</div>
</div>
