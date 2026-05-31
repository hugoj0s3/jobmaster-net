<script lang="ts">
	import { onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus, JobStatus } from "$lib/api/enums";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import Pager from "$lib/components/Pager.svelte";
	import { workerCache } from "$lib/helper/worker-cache";

	type ApiBucketModel = components["schemas"]["ApiBucketModel"];
	type ApiJobModel = components["schemas"]["ApiJobModel"];
	type ApiAgentWorker = components["schemas"]["ApiAgentWorker"];

	const clusterId = () => $page.params.cluster;
	const bucketId = () => $page.params.id;

	let bucket: ApiBucketModel | null = null;
	let isRefreshing = false;
	let refreshError: string | null = null;
	let notFound = false;
	let workerName: string | null = null;

	let activeApiJobs: ApiJobModel[] = [];
	let activeJobsTotalCount = 0;
	let pageIndex = 0;
	let pageSize = 10;

	$: bucketName = bucket?.name ?? bucket?.id ?? "Bucket";
	$: bucketStatusBadgeClass =
		bucket?.status === BucketStatus.Active   ? "badge-success" :
		bucket?.status === BucketStatus.Lost     ? "badge-error" :
		(bucket?.status === BucketStatus.Draining || bucket?.status === BucketStatus.ReadyToDrain || bucket?.status === BucketStatus.Completing) ? "badge-warning" :
		"badge-ghost";

	$: bucketStatusLabel =
		bucket?.status === BucketStatus.Active        ? "Active" :
		bucket?.status === BucketStatus.Completing    ? "Completing" :
		bucket?.status === BucketStatus.ReadyToDrain  ? "Ready to Drain" :
		bucket?.status === BucketStatus.Draining      ? "Draining" :
		bucket?.status === BucketStatus.Lost          ? "Lost" :
		bucket?.status === BucketStatus.ReadyToDelete ? "Ready to Delete" :
		"—";

	function formatDuration(startIso: string | null | undefined, endIso: string | null | undefined): string {
		if (!startIso || !endIso) return "—";
		const ms = new Date(endIso).getTime() - new Date(startIso).getTime();
		if (!Number.isFinite(ms) || ms < 0) return "—";
		if (ms < 1000) return `${Math.round(ms)}ms`;
		if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
		return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
	}

	function jobStatusBadgeClass(status: number | undefined): string {
		if (status === JobStatus.Queued)      return "badge-ghost";
		if (status === JobStatus.Processing)  return "badge-warning";
		if (status === JobStatus.InBucket)    return "badge-info";
		return "badge-ghost";
	}

	function jobStatusLabel(status: number | undefined): string {
		if (status === JobStatus.Queued)      return "Queued";
		if (status === JobStatus.Processing)  return "Processing";
		if (status === JobStatus.InBucket)    return "In Bucket";
		return "—";
	}

	async function refreshNow() {
		isRefreshing = true;
		refreshError = null;
		try {
			const cid = clusterId();
			const bid = bucketId();
			if (!cid || !bid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const [bucketResp, activeJobsResp, activeJobsCountResp] = await Promise.all([
				jm.GET("/{clusterId}/buckets/{bucketId}", {
					params: { path: { clusterId: cid, bucketId: bid } }
				}),
				jm.GET("/{clusterId}/jobs", {
					params: {
						path: { clusterId: cid },
						query: {
							BucketId: bid,
							Statuses: [JobStatus.Queued, JobStatus.Processing, JobStatus.InBucket],
							CountLimit: pageSize,
							Offset: Math.max(0, pageIndex) * pageSize
						}
					}
				}),
				jm.GET("/{clusterId}/jobs/count", {
					params: {
						path: { clusterId: cid },
						query: { BucketId: bid, Statuses: [JobStatus.Queued, JobStatus.Processing, JobStatus.InBucket] }
					}
				}).then((r) => (r.error ? 0 : (r.data as number) ?? 0))
			]);

			if (bucketResp.error) {
				if (bucketResp.response?.status === 404) { notFound = true; return; }
				refreshError = "Failed to load bucket.";
				return;
			}
			bucket = (bucketResp.data ?? null) as ApiBucketModel | null;
			if (!bucket) { notFound = true; return; }

			if (bucket.agentWorkerId && workerCache.getMissing(cid, [bucket.agentWorkerId]).length > 0) {
				try {
					const wr = await jm.GET("/{clusterId}/workers/{workerId}", {
						params: { path: { clusterId: cid, workerId: bucket.agentWorkerId } }
					});
					if (!wr.error && wr.data) workerCache.populate(cid, [wr.data as ApiAgentWorker]);
				} catch { /* keep ID */ }
			}
			workerName = bucket.agentWorkerId
				? (workerCache.get(cid, bucket.agentWorkerId)?.name ?? bucket.agentWorkerId)
				: null;

			if (activeJobsResp.error) {
				activeApiJobs = [];
				activeJobsTotalCount = 0;
			} else {
				activeApiJobs = ((activeJobsResp.data ?? []) as ApiJobModel[]).filter(Boolean);
				activeJobsTotalCount = activeJobsCountResp as number;
				const maxPage = Math.max(0, Math.ceil(activeJobsTotalCount / pageSize) - 1);
				if (pageIndex > maxPage) { pageIndex = maxPage; return refreshNow(); }
			}
		} catch (e) {
			console.error("Failed to refresh bucket detail:", e);
			refreshError = "Failed to refresh.";
		} finally {
			isRefreshing = false;
		}
	}

	let _lastPage = pageIndex;
	let _lastSize = pageSize;
	$: if (pageIndex !== _lastPage || pageSize !== _lastSize) {
		_lastPage = pageIndex;
		_lastSize = pageSize;
		refreshNow();
	}

	onMount(() => { refreshNow(); });
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">

		<div class="space-y-2">
			<div class="text-sm breadcrumbs opacity-70">
				<ul>
					<li><a href="/{clusterId()}/buckets" class="link link-hover">Buckets</a></li>
					<li>{bucketName}</li>
				</ul>
			</div>
			<h1 class="text-2xl font-semibold tracking-tight">{bucketName}</h1>
			<div class="flex items-center gap-2">
				<span class={"badge badge-sm badge-outline " + bucketStatusBadgeClass}>{bucketStatusLabel}</span>
			</div>
		</div>

		{#if refreshError}
			<div class="alert alert-error mt-4"><span>{refreshError}</span></div>
		{/if}

		{#if notFound}
			<div class="alert alert-error text-sm mt-4"><span>Bucket not found.</span></div>
		{:else if bucket}

		<!-- Info card -->
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body">
				<h2 class="card-title text-base">Details</h2>
				<div class="divider my-2"></div>
				<div class="grid grid-cols-1 gap-3 sm:grid-cols-2 text-sm">
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Host</span>
						<span class="font-medium">{bucket.hostDisplayName ?? "—"}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Agent Connection</span>
						<span class="font-medium">{bucket.agentConnectionName ?? bucket.agentConnectionId ?? "—"}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Worker</span>
						<a href="/{clusterId()}/workers/{bucket.agentWorkerId}" class="link link-hover link-primary font-medium">{workerName ?? "—"}</a>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Worker Lane</span>
						<span class="font-medium">{bucket.workerLane ?? "—"}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Created At</span>
						<span class="font-medium">{DateDisplayUtil.formatRelativeOrDate(bucket.createdAt)}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Last Status Change</span>
						<span class="font-medium">{DateDisplayUtil.formatRelativeOrDate(bucket.lastStatusChangeAt)}</span>
					</div>
				</div>
			</div>
		</div>

		<!-- Active Jobs -->
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body">
				<div class="flex items-center justify-between gap-3">
					<h2 class="card-title text-base">Active Jobs</h2>
					<div class="flex items-center gap-2">
						{#if isRefreshing}<span class="loading loading-spinner loading-sm"></span>{/if}
						<Pager
							bind:pageIndex
							bind:pageSize
							totalCount={activeJobsTotalCount}
							currentCount={activeApiJobs.length}
							disabled={isRefreshing}
							showPageSize={true}
						/>
						<button class="btn btn-xs" on:click={refreshNow} disabled={isRefreshing}>Refresh</button>
					</div>
				</div>
				<div class="divider my-2"></div>
				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/70">
							<th>Job Definition</th>
							<th>State</th>
							<th>Scheduled At</th>
							<th>Queue Time</th>
							<th>Run Duration</th>
						</tr>
						</thead>
						<tbody>
						{#each activeApiJobs as j (j.id)}
							<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/jobs/${j.id}`)}>
								<td class="font-medium">{j.jobDefinitionId ?? j.id ?? "—"}</td>
								<td><span class={"badge badge-sm " + jobStatusBadgeClass(j.status)}>{jobStatusLabel(j.status)}</span></td>
								<td class="whitespace-nowrap opacity-70">{DateDisplayUtil.formatRelativeOrDate(j.scheduledAt)}</td>
								<td class="opacity-70">{formatDuration(j.createdAt, j.processStartedAt)}</td>
								<td class="opacity-70">{j.processStartedAt ? formatDuration(j.processStartedAt, new Date().toISOString()) : "—"}</td>
							</tr>
						{:else}
							<tr><td colspan="5" class="text-center opacity-60 py-6">No active jobs.</td></tr>
						{/each}
						</tbody>
					</table>
				</div>
			</div>
		</div>

		{/if}
	</div>
</div>
