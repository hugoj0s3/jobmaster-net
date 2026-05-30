<script lang="ts">
	import { onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { JobStatusUtil, type JobStatusLabel } from "$lib/helper/job-status-util";
	import { PriorityUtil, type PriorityLabel } from "$lib/helper/priority-util";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import { bucketNameCache } from "$lib/helper/bucket-name-cache";
	import { workerCache, type WorkerCacheEntry } from "$lib/helper/worker-cache";
	import { LogCategory, LogLevel, logLevelLabel, logLevelBadgeClass, LogLevelFilterOptions, workerModeLabel } from "$lib/api/enums";

	type ApiJobModel = components["schemas"]["ApiJobModel"];
	type ApiBucketModel = components["schemas"]["ApiBucketModel"];
	type ApiAgentWorker = components["schemas"]["ApiAgentWorker"];
	type ApiJobExecution = components["schemas"]["ApiJobExecution"];

	function triggerSourceTypeLabel(value: number | null | undefined): string {
		if (value === 1) return "Once";
		if (value === 2) return "Static Recurring";
		if (value === 3) return "Dynamic Recurring";
		return "—";
	}

	const clusterId = () => $page.params.cluster;
	const jobId = () => $page.params.id;

	type LogEntry = {
		id?: string;
		timestampUtc?: string;
		level?: number;
		message?: string;
		subjectType?: number;
		subjectId?: string;
		exceptionMessage?: string;
		exceptionStackTrace?: string;
	};

	let job: ApiJobModel | null = null;
	let executions: ApiJobExecution[] = [];
	let recentLogs: LogEntry[] = [];
	let filteredLogs: LogEntry[] = [];
	let selectedLogLevel: string = "";
	let logsPageSize = 10;
	let logsSortDirection: "asc" | "desc" = "desc";
	let isLoading = true;
	let refreshError: string | null = null;
	let lastClusterId: string | null = null;
	let notFound = false;
	let selectedExecDetail: { message: string; isError: boolean } | null = null;
	let selectedLogDetail: LogEntry | null = null;
	let isDarkTheme = false;

	$: statusLabel = safeStatusLabel(job?.status);
	$: priorityLabel = safePriorityLabel(job?.priority);
	$: sortedExecutions = [...executions].sort((a, b) =>
		new Date(b.startedAt ?? 0).getTime() - new Date(a.startedAt ?? 0).getTime()
	);
	let bucketName: string | null = null;
	let workerEntry: WorkerCacheEntry | undefined = undefined;

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

	function outcomeLabel(value: unknown): string {
		if (value == null) return "—";
		const s = String(value).toLowerCase();
		if (s === "1" || s === "succeeded" || s === "success") return "Succeeded";
		if (s === "2" || s === "failed" || s === "failure") return "Failed";
		return String(value);
	}

	function outcomeBadgeClass(value: unknown): string {
		if (value == null) return "badge-ghost";
		const s = String(value).toLowerCase();
		if (s === "1" || s === "succeeded" || s === "success") return "badge-success";
		if (s === "2" || s === "failed" || s === "failure") return "badge-error";
		return "badge-ghost";
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

async function refreshNow() {
		isLoading = true;
		refreshError = null;
		notFound = false;
		try {
			const cid = clusterId();
			const jid = jobId();
			if (!cid || !jid) return;

			if (lastClusterId && lastClusterId !== cid) {
				goto(`/${cid}/jobs`);
				return;
			}
			lastClusterId = cid;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jm.GET("/{clusterId}/jobs/{id}", {
				params: { path: { clusterId: cid, id: jid } }
			});

			if (response.error) {
				console.error("API error (job detail):", response.error);
				if (response.response?.status === 404) {
					notFound = true;
					refreshError = null;
				} else {
					refreshError = "Failed to load job details.";
				}
				return;
			}

			job = (response.data ?? null) as ApiJobModel | null;

			if (!job) {
				notFound = true;
				return;
			}

			if (job.bucketId && bucketNameCache.getMissing(cid, [job.bucketId]).length > 0) {
				try {
					const br = await jm.GET("/{clusterId}/buckets", {
						params: { path: { clusterId: cid }, query: { BucketIds: [job.bucketId], CountLimit: 1 } }
					});
					if (!br.error) bucketNameCache.populate(cid, (br.data ?? []) as ApiBucketModel[]);
				} catch { /* name stays as ID */ }
			}
			bucketName = job.bucketId ? (bucketNameCache.get(cid, job.bucketId) ?? job.bucketId) : null;

			if (job.agentWorkerId && workerCache.getMissing(cid, [job.agentWorkerId]).length > 0) {
				try {
					const wr = await jm.GET("/{clusterId}/workers/{workerId}", {
						params: { path: { clusterId: cid, workerId: job.agentWorkerId } }
					});
					if (!wr.error && wr.data) workerCache.populate(cid, [wr.data as ApiAgentWorker]);
				} catch { /* name stays as ID */ }
			}
			workerEntry = job.agentWorkerId ? workerCache.get(cid, job.agentWorkerId) : undefined;

			try {
				const execResp = await jm.GET("/{clusterId}/jobs/{id}/executions", {
					params: { path: { clusterId: cid, id: jid } }
				});
				executions = execResp.error ? [] : (execResp.data ?? []) as ApiJobExecution[];
			} catch {
				executions = [];
			}

			try {
				const logsResp = await jm.GET("/{clusterId}/logs", {
					params: {
						path: { clusterId: cid },
						query: { ReferenceGuid: jid, Category: LogCategory.JobExecution, CountLimit: logsPageSize }
					}
				});
				recentLogs = logsResp.error ? [] : (logsResp.data ?? []) as LogEntry[];
			} catch {
				recentLogs = [];
			}

		} catch (e) {
			console.error("Failed to fetch job:", e);
			refreshError = e instanceof Error ? e.message : String(e);
		} finally {
			isLoading = false;
		}
	}

	onMount(() => {
		const checkTheme = () => {
			isDarkTheme = document.documentElement.style.colorScheme === "dark";
		};
		checkTheme();
		const observer = new MutationObserver(checkTheme);
		observer.observe(document.documentElement, { attributes: true, attributeFilter: ["style", "data-theme"] });

		refreshNow();

		return () => observer.disconnect();
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		{#if notFound}
			<div class="flex items-center justify-center py-20">
				<div class="text-center max-w-2xl">
					<div class="mb-8">
						<h1 class="text-9xl font-bold text-primary opacity-20">404</h1>
					</div>
					<div class="space-y-4">
						<h2 class="text-3xl font-semibold">Job Not Found</h2>
						<p class="text-base-content/70 text-lg">
							The job you're looking for doesn't exist in this cluster or has been deleted.
						</p>
					</div>
					<div class="mt-8 flex gap-4 justify-center">
						<button class="btn btn-primary" on:click={() => goto(`/${clusterId()}/jobs`)}>
							Go to Jobs List
						</button>
						<button class="btn btn-ghost" on:click={() => window.history.back()}>
							Go Back
						</button>
					</div>
				</div>
			</div>
		{:else if isLoading && !job}
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
				</div>

			</div>

			{#if refreshError}
				<div class="alert alert-warning mt-4 text-sm">
					<span>{refreshError}</span>
				</div>
			{/if}

			<!-- Main content grid -->
			<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
				<!-- Summary -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Summary</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Id</span>
								<span class="font-mono font-medium">{job.id ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Job Definition Id</span>
								<span class="font-medium">{job.jobDefinitionId ?? "—"}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Status</span>
								<span class={"badge whitespace-nowrap " + statusBadgeClass(statusLabel)}>{statusLabel}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Priority</span>
								<span class={"badge whitespace-nowrap " + priorityBadgeClass(priorityLabel)}>{priorityLabel}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Scheduled Date</span>
								<span class="font-medium">{formatDateTime(job.scheduledAt)}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Created At</span>
								<span class="font-medium">{formatDateTime(job.createdAt)}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Infrastructure -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Infrastructure</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Bucket</span>
								<span class="font-mono font-medium">
									{#if job.bucketId}
										<a href="/{clusterId()}/buckets/{job.bucketId}" class="link link-hover link-primary">{bucketName ?? job.bucketId}</a>
									{:else}
										—
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Agent Connection</span>
								<span class="font-medium">
									{#if job.agentConnectionId}
										<a href="/{clusterId()}/agent-connections/{job.agentConnectionId}" class="link link-hover link-primary">{job.agentConnectionName ?? job.agentConnectionId}</a>
									{:else}
										—
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Worker</span>
								<span class="font-medium flex items-center gap-1">
									{#if job.agentWorkerId}
										<a href="/{clusterId()}/workers/{job.agentWorkerId}" class="link link-hover link-primary">{workerEntry?.name ?? job.agentWorkerId}</a>
										{#if workerEntry?.mode != null}
											<span class="badge badge-ghost badge-sm">{workerModeLabel(workerEntry.mode)}</span>
										{/if}
									{:else}
										—
									{/if}
								</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Host</span>
								<span class="font-medium">
									{#if job.hostId}
										<a href="/{clusterId()}/hosts/{job.hostId}" class="link link-hover link-primary">{job.hostDisplayName ?? job.hostId}</a>
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
								<span class="font-medium">{triggerSourceTypeLabel(job.triggerSourceType)}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Message Data (full width) -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
					<div class="card-body">
						<h2 class="card-title text-base">Message Data</h2>
						<div class="divider my-2"></div>
						{#if !job.msgData || Object.keys(job.msgData).length === 0}
							<div class="text-sm opacity-60">No message data.</div>
						{:else}
							<pre class="bg-base-300/60 rounded-lg p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-all">{JSON.stringify(job.msgData, null, 2)}</pre>
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

				<!-- Failure & Retries -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Failure & Retries</h2>
						<div class="divider my-2"></div>
						<div class="space-y-3 text-sm">
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Number Of Failures</span>
								<span class="font-semibold">{job.numberOfFailures ?? 0}</span>
							</div>
							<div class="flex items-center justify-between gap-4">
								<span class="opacity-70">Max Number Of Retries</span>
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
									<span class="opacity-70">Scheduled At</span>
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
						{#if sortedExecutions.length === 0}
							<div class="text-sm opacity-60">No execution records.</div>
						{:else}
							<div class="overflow-x-auto">
								<table class="table table-zebra">
									<thead>
									<tr class="text-base-content/70">
										<th>Started At</th>
										<th>Completed At</th>
										<th>Run Duration</th>
										<th>Outcome</th>
									</tr>
									</thead>
									<tbody>
									{#each sortedExecutions as exec (exec.id)}
										<tr>
											<td class="font-medium whitespace-nowrap">{formatDateTime(exec.startedAt)}</td>
											<td class="font-medium whitespace-nowrap">{formatDateTime(exec.finalizedAt)}</td>
											<td class="font-medium">{formatDuration(exec.startedAt, exec.finalizedAt)}</td>
											<td>
												<div class="flex items-center gap-2">
													<span class={"badge whitespace-nowrap " + outcomeBadgeClass(exec.outcome)}>{outcomeLabel(exec.outcome)}</span>
													{#if exec.outcomeMessage}
														<button
															class="btn btn-ghost btn-xs opacity-70 hover:opacity-100"
															on:click={() => selectedExecDetail = { message: exec.outcomeMessage ?? "", isError: outcomeLabel(exec.outcome) === "Failed" }}
														>
															Details
														</button>
													{/if}
												</div>
											</td>
										</tr>
									{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				</div>

				<!-- Logs -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
					<div class="card-body">
						<div class="flex items-center justify-between gap-4">
							<h2 class="card-title text-base">Logs</h2>
							<div class="flex items-center gap-2">
								<FilterDropdown
									label="Log Level"
									options={LogLevelFilterOptions}
									bind:value={selectedLogLevel}
								/>
								<select class="select select-bordered select-xs" bind:value={logsSortDirection}>
									<option value="desc">Newest first</option>
									<option value="asc">Oldest first</option>
								</select>
								<select class="select select-bordered select-xs" bind:value={logsPageSize}>
									<option value={10}>10</option>
									<option value={25}>25</option>
									<option value={50}>50</option>
									<option value={100}>100</option>
								</select>
							</div>
						</div>
						<div class="divider my-2"></div>
						{#if filteredLogs.length === 0}
							<div class="text-sm opacity-60">
								{#if selectedLogLevel}
									No logs match the selected log level.
								{:else}
									No logs found.
								{/if}
							</div>
						{:else}
							<div class="overflow-x-auto">
								<table class="table table-zebra">
									<thead>
									<tr class="text-base-content/70">
										<th>Level</th>
										<th>Timestamp</th>
										<th>Message</th>
										<th></th>
									</tr>
									</thead>
									<tbody>
									{#each filteredLogs as log (log.id ?? log.timestampUtc ?? Math.random())}
										<tr>
											<td>
												<span class={"badge badge-sm whitespace-nowrap " + logLevelBadgeClass(log.level)}>
													{logLevelLabel(log.level)}
												</span>
											</td>
											<td class="font-medium whitespace-nowrap">{formatDateTime(log.timestampUtc)}</td>
											<td class="font-mono text-xs max-w-xs truncate">{log.message ?? "—"}</td>
											<td>
												<button
													class="btn btn-ghost btn-xs opacity-70 hover:opacity-100"
													on:click={() => selectedLogDetail = log}
												>
													Details
												</button>
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

{#if selectedExecDetail !== null}
	<dialog class="modal modal-open" on:click|self={() => selectedExecDetail = null}>
		<div class="modal-box max-w-2xl w-full">
			<div class="flex items-center gap-2 mb-4">
				{#if selectedExecDetail.isError}
					<span class="badge badge-error badge-sm">Failed</span>
				{:else}
					<span class="badge badge-success badge-sm">Succeeded</span>
				{/if}
				<h3 class="font-bold text-base">Execution Detail</h3>
			</div>
			<pre class={"rounded-lg p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-words max-h-96 overflow-y-auto border-l-4 " + (selectedExecDetail.isError ? "border-error" : "border-success") + (isDarkTheme ? " bg-neutral text-neutral-content" : " bg-base-200 text-base-content")}>{selectedExecDetail.message}</pre>
			<div class="modal-action">
				<button class="btn btn-sm" on:click={() => selectedExecDetail = null}>Close</button>
			</div>
		</div>
	</dialog>
{/if}

{#if selectedLogDetail !== null}
	<dialog class="modal modal-open" on:click|self={() => selectedLogDetail = null}>
		<div class="modal-box max-w-2xl w-full">
			<div class="flex items-center gap-2 mb-4">
				<span class={"badge badge-sm " + logLevelBadgeClass(selectedLogDetail.level)}>
					{logLevelLabel(selectedLogDetail.level)}
				</span>
				<h3 class="font-bold text-base">Log Detail</h3>
				<span class="text-xs opacity-50 ml-auto">{formatDateTime(selectedLogDetail.timestampUtc)}</span>
			</div>
			<pre class={"rounded-lg p-4 text-xs font-mono overflow-x-auto whitespace-pre-wrap break-words max-h-96 overflow-y-auto border-l-4 " + (selectedLogDetail.level === LogLevel.Error || selectedLogDetail.level === LogLevel.Critical ? "border-error" : selectedLogDetail.level === LogLevel.Warning ? "border-warning" : selectedLogDetail.level === LogLevel.Info ? "border-info" : "border-base-300") + (isDarkTheme ? " bg-neutral text-neutral-content" : " bg-base-200 text-base-content")}>{selectedLogDetail.message ?? "—"}{selectedLogDetail.exceptionMessage ? "\n\n" + selectedLogDetail.exceptionMessage : ""}{selectedLogDetail.exceptionStackTrace ? "\n\n" + selectedLogDetail.exceptionStackTrace : ""}</pre>
			<div class="modal-action">
				<button class="btn btn-sm" on:click={() => selectedLogDetail = null}>Close</button>
			</div>
		</div>
	</dialog>
{/if}
