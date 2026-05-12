<!-- src/routes/buckets/[id]/+page.svelte -->
<script lang="ts">
	import { onMount } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus, JobStatus } from "$lib/api/enums";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import Pager from "$lib/components/Pager.svelte";
	import { resolve } from '$app/paths';
	import { readUrlParams, writeUrlParams, Serializers } from '$lib/helper/url-filters';

	type ActiveJob = {
		name: string;
		id: string;
		since: string;
		state: "Queued" | "Processing" | "Succeeded" | "Failed";
		agent: string;
		assigned: string;
		queueTime: string;
		runDuration: string;
	};


	type ApiBucketModel = components["schemas"]["ApiBucketModel"];
	type ApiJobModel = components["schemas"]["ApiJobModel"];

	const clusterId = () => $page.params.cluster;
	const bucketId = () => $page.params.id;

	let bucket: ApiBucketModel | null = null;
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let refreshError: string | null = null;

	let activeJobs: ActiveJob[] = [];
	let activeApiJobs: ApiJobModel[] = [];
	let historyApiJobs: ApiJobModel[] = [];

	// Pagination state
	let activeJobsTotalCount = 0;
	let pageIndex = 0;
	let pageSize = 10;

	const urlParamDefs = {
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	let _initParams = readUrlParams(urlParamDefs);
	pageIndex = _initParams.page;
	pageSize = _initParams.size;

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageIndex = _initParams.page;
		pageSize = _initParams.size;
		refreshNow();
	}

	function safeMsBetween(startIso: string | null | undefined, endIso: string | null | undefined): number | null {
		if (!startIso || !endIso) return null;
		const s = Date.parse(startIso);
		const e = Date.parse(endIso);
		if (!Number.isFinite(s) || !Number.isFinite(e)) return null;
		const diff = e - s;
		return diff >= 0 ? diff : null;
	}

	function avg(nums: number[]): number | null {
		if (nums.length === 0) return null;
		return nums.reduce((a, b) => a + b, 0) / nums.length;
	}

	function formatDuration(ms: number | null): string {
		if (ms == null || Number.isNaN(ms)) return "—";
		if (ms < 1000) return `${Math.round(ms)}ms`;
		return `${(ms / 1000).toFixed(1)}s`;
	}

	function safeFormatDateTime(d: string | null | undefined): string {
		return DateDisplayUtil.formatRelativeOrDate(d);
	}

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			page: pageIndex,
			size: pageSize
		});
	}

	$: pageIndex, pageSize, syncToUrl();

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

	async function refreshNow() {
		isRefreshing = true;
		refreshError = null;
		try {
			const cid = clusterId();
			const bid = bucketId();
			if (!cid || !bid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const [bucketResp, activeJobsResp, activeJobsCountResp, historyJobsResp] = await Promise.all([
				jm.GET("/{clusterId}/buckets/{bucketId}", {
					params: { path: { clusterId: cid, bucketId: bid } }
				}),
				jm.GET("/{clusterId}/jobs", {
					params: {
						path: { clusterId: cid },
						query: {
							BucketId: bid,
							Statuses: [JobStatus.Queued, JobStatus.Processing, JobStatus.AssignedToBucket],
							CountLimit: pageSize,
							Offset: Math.max(0, pageIndex) * pageSize,
							OrderByProperty: "createdAt",
							OrderByAsc: false
						}
					}
				}),
				jm.GET("/{clusterId}/jobs/count", {
					params: {
						path: { clusterId: cid },
						query: {
							BucketId: bid,
							Statuses: [JobStatus.Queued, JobStatus.Processing, JobStatus.AssignedToBucket]
						}
					}
				}).then((r) => {
					if (r.error) throw r.error;
					return r.data as number;
				}),
				jm.GET("/{clusterId}/jobs", {
					params: {
						path: { clusterId: cid },
						query: {
							BucketId: bid,
							Statuses: [JobStatus.Succeeded, JobStatus.Failed, JobStatus.Cancelled],
							CountLimit: 50,
							OrderByProperty: "succeedExecutedAt",
							OrderByAsc: false
						}
					}
				})
			]);

			if (bucketResp.error) {
				console.error("API error (bucket detail):", bucketResp.error);
				refreshError = "Failed to load bucket.";
				return;
			}
			bucket = (bucketResp.data ?? null) as ApiBucketModel | null;

			if (activeJobsResp.error) {
				console.error("API error (bucket active jobs):", activeJobsResp.error);
				refreshError = "Failed to load bucket jobs.";
				activeJobs = [];
				activeApiJobs = [];
			} else {
				const jobs = ((activeJobsResp.data ?? []) as ApiJobModel[]).filter(Boolean);
				activeApiJobs = jobs;
				activeJobsTotalCount = activeJobsCountResp as number;

				const newMaxPageIndex = Math.max(0, Math.ceil(activeJobsTotalCount / pageSize) - 1);
				if (pageIndex > newMaxPageIndex) {
					pageIndex = newMaxPageIndex;
					return refreshNow();
				}

				activeJobs = jobs.map((j) => {
					const since = DateDisplayUtil.formatRelativeOrDate(j.createdAt);
					const queueMs = safeMsBetween(j.createdAt ?? null, j.processingStartedAt ?? null);
					const runMs =
						j.status === JobStatus.Processing && j.processingStartedAt
							? safeMsBetween(j.processingStartedAt ?? null, new Date().toISOString())
							: null;
					const state =
						j.status === JobStatus.Queued
							? "Queued"
							: j.status === JobStatus.Processing
								? "Processing"
								: "Queued";
					return {
						name: j.jobDefinitionId ?? j.id ?? "—",
						id: j.id ?? "",
						since,
						state,
						agent: j.agentConnectionId ?? "—",
						assigned: j.agentWorkerId ?? "",
						queueTime: formatDuration(queueMs),
						runDuration: formatDuration(runMs)
					};
				});
			}

			if (historyJobsResp.error) {
				console.error("API error (bucket history jobs):", historyJobsResp.error);
				historyApiJobs = [];
			} else {
				historyApiJobs = ((historyJobsResp.data ?? []) as ApiJobModel[]).filter(Boolean);
			}

			lastUpdatedAt = new Date();
		} catch (e) {
			console.error("Failed to refresh bucket detail:", e);
			refreshError = "Failed to refresh.";
		} finally {
			isRefreshing = false;
		}
	}

	onMount(() => {
		refreshNow();
	});

	$: bucketName = bucket?.name ?? bucket?.id ?? "Bucket";
	$: bucketHost = bucket?.hostDisplayName ?? "—";
	$: bucketAgent = bucket?.agentConnectionName ?? bucket?.agentConnectionId ?? "—";
	$: bucketWorker = bucket?.workerLane ?? bucket?.agentWorkerId ?? "—";
	$: bucketCreatedAt = bucket?.createdAt;
	$: bucketLastExecutionAt = bucket?.lastStatusChangeAt;
	$: lastUpdatedLabel = DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt);
	$: bucketStatus = bucket?.status;
	$: bucketStatusBadge =
		bucketStatus === BucketStatus.Active
			? "badge-success"
			: bucketStatus === BucketStatus.Lost
				? "badge-error"
				: bucketStatus === BucketStatus.Draining || bucketStatus === BucketStatus.ReadyToDrain || bucketStatus === BucketStatus.Completing
					? "badge-warning"
					: bucketStatus === BucketStatus.ReadyToDelete
						? "badge-ghost"
						: "badge-ghost";
	$: bucketStatusDot =
		bucketStatus === BucketStatus.Active
			? "bg-success"
			: bucketStatus === BucketStatus.Lost
				? "bg-error"
				: bucketStatus === BucketStatus.Draining || bucketStatus === BucketStatus.ReadyToDrain || bucketStatus === BucketStatus.Completing
					? "bg-warning"
					: "bg-base-content/30";

	$: queuedJobsCount = activeApiJobs.filter((j) => j.status === JobStatus.Queued).length;
	$: totalActiveJobsCount = activeJobsTotalCount;
	$: avgQueueMs = avg(
		activeApiJobs
			.map((j) => safeMsBetween(j.createdAt ?? null, j.processingStartedAt ?? null))
			.filter((v): v is number => v != null)
	);
	$: avgRunMs = avg(
		historyApiJobs
			.map((j) => safeMsBetween(j.processingStartedAt ?? null, j.succeedExecutedAt ?? null))
			.filter((v): v is number => v != null)
	);
	$: avgQueueLabel = formatDuration(avgQueueMs);
	$: avgRunLabel = formatDuration(avgRunMs);
	$: deltaMs = (avgQueueMs != null && avgRunMs != null) ? avgQueueMs - avgRunMs : null;
	$: deltaLabel = formatDuration(deltaMs);
	$: deltaIsPositive = deltaMs != null && deltaMs > 0;


	const stateBadge: Record<ActiveJob["state"], string> = {
		Queued: "badge-ghost",
		Processing: "badge-warning",
		Succeeded: "badge-success",
		Failed: "badge-error",
	};

</script>

<div class="min-h-screen bg-base-100">
	<div class="w-full px-6 py-6">
		<div class="flex flex-wrap items-center justify-between gap-3">
			<a class="link link-hover text-sm opacity-70" href={`/${clusterId()}/buckets`}>← Back to Buckets</a>

			<div class="flex items-center gap-3">
				<div class="text-sm opacity-70">
					<span class="hidden sm:inline">Last Refresh:</span>
					<span class="font-semibold">{safeFormatDateTime(bucketLastExecutionAt)}</span>
				</div>
				<button
					class="btn btn-sm btn-ghost btn-square"
					aria-label="Refresh"
					title={isRefreshing ? "Refreshing..." : "Refresh"}
					disabled={isRefreshing}
					on:click={refreshNow}
				>
					<span class={"i text-2xl leading-none " + (isRefreshing ? "animate-spin" : "")}>⟳</span>
				</button>
			</div>
		</div>

		{#if refreshError}
			<div class="alert alert-error mt-4">
				<span>{refreshError}</span>
			</div>
		{/if}

		<!-- Title -->
		<div class="mt-3">
			<h1 class="text-2xl font-semibold">Bucket Detail</h1>
		</div>

		<!-- Summary card -->
		<div class="card mt-4 bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body gap-4">
				<div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
					<div class="flex items-center gap-4">
						<div>
							<div class="flex items-center gap-2">
								<div class="text-xl font-semibold">{bucketName}</div>
								<span class={"badge badge-outline gap-2 " + bucketStatusBadge}>
									<span class={"inline-block h-2 w-2 rounded-full " + bucketStatusDot}></span>
								</span>
							</div>
							<div class="text-sm opacity-70">{bucket?.status ?? "—"}</div>

							<div class="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm opacity-80">
								<span>Host: <span class="font-semibold">{bucketHost}</span></span>
								<span class="opacity-40">|</span>
								<span>Agent: <span class="font-semibold">{bucketAgent}</span></span>
								<span class="opacity-40">|</span>
								<span>Worker: <span class="font-semibold">🏷 {bucketWorker}</span></span>
								<span class="opacity-40">|</span>
								<span>{safeFormatDateTime(bucketCreatedAt)}</span>
								<span class="opacity-40">|</span>
								<span>‹ {bucket?.status ?? "—"}</span>
								<span class="badge badge-ghost badge-sm">{lastUpdatedLabel}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Stats row -->
				<div class="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">

					<!-- Amount Jobs -->
					<div class="card bg-base-100 border border-base-300/60">
						<div class="card-body">
							<div class="text-sm opacity-70">Amount Jobs</div>
							<div class="mt-1 text-3xl font-semibold">{totalActiveJobsCount}</div>
							<div class="mt-2 text-xs opacity-60">{queuedJobsCount} queued</div>
						</div>
					</div>

					<!-- Avg queue time -->
					<div class="card bg-base-100 border border-base-300/60">
						<div class="card-body">
							<div class="text-sm opacity-70">Avg Queue Time</div>
							<div class="mt-1 text-3xl font-semibold">{avgQueueLabel}</div>
							<div class="mt-2 text-xs opacity-60">Time in queue</div>
						</div>
					</div>

					<!-- Avg run duration -->
					<div class="card bg-base-100 border border-base-300/60">
						<div class="card-body">
							<div class="text-sm opacity-70">Avg Run Duration</div>
							<div class="mt-1 text-3xl font-semibold">{avgRunLabel}</div>
							<div class="mt-2 text-xs opacity-60">Execution time</div>
						</div>
					</div>

					<!-- Delta -->
					<div class="card bg-base-100 border border-base-300/60">
						<div class="card-body">
							<div class="text-sm opacity-70">Delta (Queue - Run)</div>
							<div class="mt-1 text-3xl font-semibold flex items-center gap-2">
								{#if deltaMs === null}
									<span>—</span>
								{:else if deltaIsPositive}
									<span class="text-warning">+{deltaLabel}</span>
									<span class="text-warning text-lg">↑</span>
								{:else}
									<span class="text-success">{deltaLabel}</span>
									<span class="text-success text-lg">↓</span>
								{/if}
							</div>
							<div class="mt-2 text-xs opacity-60">
								{#if deltaMs === null}
									No data
								{:else if deltaIsPositive}
									Queue slower than run
								{:else}
									Queue faster than run
								{/if}
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Active Jobs -->
		<div class="mt-6">
			<div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
				<div class="flex items-center gap-3">
					<h2 class="text-xl font-semibold">Active Jobs</h2>
					<span class="badge badge-ghost">{activeJobs.length} / {activeJobsTotalCount}</span>
				</div>

				<Pager
					bind:pageIndex
					bind:pageSize
					totalCount={activeJobsTotalCount}
					currentCount={activeJobs.length}
					disabled={isRefreshing}
					showPageSize={true}
				/>
			</div>

			<div class="card mt-3 bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-0">
					<div class="overflow-x-auto">
						<table class="table table-zebra">
							<thead>
							<tr class="text-base-content/70">
								<th>Job</th>
								<th class="hidden md:table-cell">ID</th>
								<th>Since</th>
								<th>State</th>
								<th class="hidden lg:table-cell">Agent</th>
								<th class="hidden lg:table-cell">Assigned</th>
								<th>Queue Time</th>
								<th class="text-right">Run Duration</th>
							</tr>
							</thead>
							<tbody>
							{#each activeJobs as j (j.name + j.id)}
								<tr class="hover cursor-pointer">
									<td>
										<div class="flex items-center gap-2">
											<span class="text-success">♥</span>
											<a
												class="link link-hover font-medium"
												href={resolve(`/${clusterId()}/jobs/${j.id}`)}
												aria-label={`Open job ${j.name}`}
												>{j.name}</a
											>
										</div>
									</td>
									<td class="hidden md:table-cell opacity-80">
										<span class="tooltip tooltip-bottom" data-tip={j.id || "--"}>
											{j.id ? j.id.substring(0, 8) + "..." : "--"}
										</span>
									</td>
									<td class="opacity-80">{j.since}</td>
									<td>
										<span class={"badge badge-sm " + stateBadge[j.state]}>{j.state}</span>
									</td>
									<td class="hidden lg:table-cell opacity-80">{j.agent}</td>
									<td class="hidden lg:table-cell opacity-80">{j.assigned || "--"}</td>
									<td class="opacity-80">{j.queueTime}</td>
									<td class="text-right opacity-80">{j.runDuration}</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>

	</div>
</div>
