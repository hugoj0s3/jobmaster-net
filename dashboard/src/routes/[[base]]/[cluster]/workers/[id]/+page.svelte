<script lang="ts">
	import { onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus as BucketStatusEnum, workerModeLabel } from "$lib/api/enums";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import Pager from "$lib/components/Pager.svelte";

	type ApiBucketModel = components["schemas"]["ApiBucketModel"];

	const clusterId = () => $page.params.cluster;
	const workerId = () => $page.params.id;

	let worker: any = null;
	let buckets: ApiBucketModel[] = [];
	let bucketsPage = 0;
	let bucketsPageSize = 10;
	let isRefreshing = false;
	let notFound = false;

	$: workerName = worker?.name ?? worker?.id ?? "Unknown";
	$: isAlive = worker?.isAlive === true;

	$: pagedBuckets = buckets.slice(bucketsPage * bucketsPageSize, (bucketsPage + 1) * bucketsPageSize);

	function bucketStatusLabel(status: number | null | undefined): string {
		if (status === BucketStatusEnum.Active) return "Active";
		if (status === BucketStatusEnum.Completing) return "Completing";
		if (status === BucketStatusEnum.ReadyToDrain) return "Ready to Drain";
		if (status === BucketStatusEnum.Draining) return "Draining";
		if (status === BucketStatusEnum.Lost) return "Lost";
		if (status === BucketStatusEnum.ReadyToDelete) return "Ready to Delete";
		return "—";
	}

	function bucketStatusBadgeClass(status: number | null | undefined): string {
		if (status === BucketStatusEnum.Active) return "badge-success";
		if (status === BucketStatusEnum.Completing) return "badge-info";
		if (status === BucketStatusEnum.Lost) return "badge-error";
		if (status === BucketStatusEnum.Draining || status === BucketStatusEnum.ReadyToDrain) return "badge-warning";
		return "badge-ghost";
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			const wid = workerId();
			if (!cid || !wid) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const workerResponse: any = await (jmApi as any).GET("/{clusterId}/workers/{workerId}", {
				params: { path: { clusterId: cid, workerId: wid } }
			});

			if (workerResponse.error) {
				console.error("API error (worker):", workerResponse.error);
				return;
			}

			worker = workerResponse.data ?? null;
			if (!worker) { notFound = true; return; }

			try {
				const bucketsResponse = await (jmApi as any).GET("/{clusterId}/buckets", {
					params: { path: { clusterId: cid }, query: { AgentWorkerId: wid } }
				});
				buckets = (bucketsResponse.data ?? []) as ApiBucketModel[];
			} catch (e) {
				console.error("Failed to fetch buckets:", e);
			}
		} catch (error) {
			console.error("Failed to fetch worker:", error);
		} finally {
			isRefreshing = false;
		}
	}

	onMount(() => { refreshNow(); });
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">

		<div class="space-y-2">
			<div class="text-sm breadcrumbs opacity-70">
				<ul>
					<li><a href="/{clusterId()}/workers" class="link link-hover">Workers</a></li>
					<li>{workerName}</li>
				</ul>
			</div>
			<h1 class="text-2xl font-semibold tracking-tight">{workerName}</h1>
			<div class="flex items-center gap-2">
				<span class="inline-block h-2 w-2 rounded-full {isAlive ? 'bg-success' : 'bg-error'}"></span>
				<span class={"badge badge-sm badge-outline " + (isAlive ? "badge-success" : "badge-error")}>{isAlive ? "Online" : "Offline"}</span>
			</div>
		</div>

		{#if notFound}
			<div class="alert alert-error text-sm mt-4"><span>Worker not found.</span></div>
		{:else if worker}

		<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">

			<!-- Configuration + Connection merged -->
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
				<div class="card-body">
					<h2 class="card-title text-base">Configuration</h2>
					<div class="divider my-2"></div>
					<div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 text-sm">
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Mode</span>
							<span class="badge badge-ghost badge-sm">{workerModeLabel(worker.mode)}</span>
						</div>
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Lane</span>
							<span class="font-medium">{worker.workerLane ?? "—"}</span>
						</div>
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Parallelism Factor</span>
							<span class="font-medium">{worker.parallelismFactor?.toFixed(1) ?? "—"}</span>
						</div>
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Agent Connection</span>
							<span class="font-medium">{worker.agentConnectionName ?? worker.agentConnectionId ?? "—"}</span>
						</div>
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Host</span>
							<a href="/{clusterId()}/hosts/{worker.hostId}" class="link link-hover link-primary font-medium">{worker.hostDisplayName ?? "—"}</a>
						</div>
						<div class="flex items-center justify-between gap-4">
							<span class="opacity-70">Last Heartbeat</span>
							<span class="font-medium">{DateDisplayUtil.formatRelativeOrDate(worker.lastHeartbeat)}</span>
						</div>
					</div>
				</div>
			</div>

			<!-- Buckets -->
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
				<div class="card-body">
					<div class="flex items-center justify-between gap-3">
						<h2 class="card-title text-base">Buckets</h2>
						<Pager
							bind:pageIndex={bucketsPage}
							bind:pageSize={bucketsPageSize}
							totalCount={buckets.length}
							currentCount={pagedBuckets.length}
							showPageSize={true}
						/>
					</div>
					<div class="divider my-2"></div>
					<div class="overflow-x-auto">
						<table class="table">
							<thead>
							<tr class="text-base-content/70">
								<th>Name</th>
								<th>Agent Connection</th>
								<th>Worker Lane</th>
								<th>Host</th>
								<th>Created At</th>
								<th class="text-right">Status</th>
							</tr>
							</thead>
							<tbody>
							{#each pagedBuckets as b}
								<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/buckets/${b.id}`)}>
									<td class="font-medium">{b.name ?? b.id ?? "—"}</td>
									<td class="opacity-70">{b.agentConnectionName ?? b.agentConnectionId ?? "—"}</td>
									<td class="opacity-70">{b.workerLane ?? "—"}</td>
									<td class="opacity-70">{b.hostDisplayName ?? "—"}</td>
									<td class="opacity-70 whitespace-nowrap">{DateDisplayUtil.formatRelativeOrDate(b.createdAt)}</td>
									<td class="text-right">
										<span class={"badge badge-sm badge-outline " + bucketStatusBadgeClass(b.status)}>{bucketStatusLabel(b.status)}</span>
									</td>
								</tr>
							{:else}
								<tr><td colspan="6" class="text-center opacity-60 py-6">No buckets.</td></tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>

		</div>
		{/if}
	</div>
</div>
