<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus as BucketStatusEnum } from "$lib/api/enums";
	import { PriorityUtil } from "$lib/helper/priority-util";
	import { WorkerModeUtil } from "$lib/helper/worker-mode-util";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";

	type ApiBucketModel = components["schemas"]["ApiBucketModel"];

	const clusterId = () => $page.params.cluster;
	const workerId = () => $page.params.id;

	// --- state ---
	let worker: any = null;
	let buckets: ApiBucketModel[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	// --- derived ---
	$: workerName = worker?.displayName ?? worker?.name ?? worker?.id ?? "Unknown";
	$: isAlive = worker?.isAlive === true;
	$: workerStatus = isAlive ? "Online" : "Offline";

	$: workerMode = WorkerModeUtil.getLabel(worker?.mode);
	$: workerModeBadge = WorkerModeUtil.getBadgeClass(workerMode);

	$: lastUpdated = DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt);

	function safeToLocaleString(d: string | null | undefined): string {
		return DateDisplayUtil.formatRelativeOrDate(d);
	}

	function bucketStatusLabel(status: number | null | undefined): string {
		if (status === BucketStatusEnum.Active) return "Active";
		if (status === BucketStatusEnum.Completing) return "Completing";
		if (status === BucketStatusEnum.ReadyToDrain) return "ReadyToDrain";
		if (status === BucketStatusEnum.Draining) return "Draining";
		if (status === BucketStatusEnum.Lost) return "Lost";
		if (status === BucketStatusEnum.ReadyToDelete) return "ReadyToDelete";
		return "—";
	}

	function badgeForStatus(status: string) {
		switch (status.toLowerCase()) {
			case "active":
				return "badge badge-success badge-outline";
			case "lost":
				return "badge badge-error badge-outline";
			case "draining":
			case "readytodrain":
				return "badge badge-warning badge-outline";
			case "completing":
				return "badge badge-info badge-outline";
			default:
				return "badge badge-ghost";
		}
	}

	function priorityLabel(p: number | null | undefined): string {
		try { return PriorityUtil.getLabel(p); } catch { return "—"; }
	}

	function goToBucket(bucketId: string) {
		const cid = clusterId();
		if (!cid) return;
		goto(`/${encodeURIComponent(cid)}/buckets/${encodeURIComponent(bucketId)}`);
	}

	// --- API ---
	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			const wid = workerId();
			if (!cid || !wid) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const workerResponse = await jmApi.GET("/{clusterId}/workers/{workerId}", {
				params: { path: { clusterId: cid, workerId: wid } }
			});

			if (workerResponse.error) {
				console.error("API error (worker):", workerResponse.error);
				return;
			}

			worker = (workerResponse.data ?? null) as any;

			try {
				const bucketsResponse = await jmApi.GET("/{clusterId}/buckets", {
					params: {
						path: { clusterId: cid },
						query: { AgentWorkerId: wid }
					}
				});
				buckets = ((bucketsResponse.data ?? []) as ApiBucketModel[]);
			} catch (e) {
				console.error("Failed to fetch buckets:", e);
			}

			lastUpdatedAt = new Date();
		} catch (error) {
			console.error("Failed to fetch worker:", error);
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
		refreshNow().then(restartPoller);
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<!-- Top row (title + refresh meta) -->
		<div class="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
			<div class="space-y-2">
				<div class="text-sm breadcrumbs">
					<ul>
						<li><a href="/{clusterId()}/workers" class="link link-hover">Workers</a></li>
						<li>{workerName}</li>
					</ul>
				</div>
				<div class="flex flex-wrap items-center gap-2">
					<h1 class="text-2xl font-semibold tracking-tight">{workerName}</h1>
					<span class="badge {isAlive ? 'badge-success' : 'badge-error'} gap-2">
						<span class="inline-block h-2 w-2 rounded-full {isAlive ? 'bg-success' : 'bg-error'}"></span>
						{workerStatus}
					</span>
					<span class={`badge ${workerModeBadge}`}>{workerMode}</span>
				</div>

				<div class="flex flex-wrap items-center gap-3 text-sm opacity-80">
					<div class="flex items-center gap-2">
						<span class="opacity-70">Agent:</span>
						<span class="font-medium">{worker?.agentConnectionName ?? worker?.agentConnectionId ?? '—'}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Lane:</span>
						<span class="font-medium">{worker?.workerLane ?? '—'}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Host:</span>
						<span class="font-medium">{worker?.hostDisplayName ?? 'N/A'}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Last heartbeat:</span>
						<span class="font-medium">{safeToLocaleString((worker?.lastHeartbeat ?? worker?.lastHeartbeatAt))}</span>
					</div>
				</div>
			</div>

			<div class="flex flex-col items-end gap-2">
				<div class="flex items-center gap-2 text-sm text-base-content/60">
					<span>Last Refresh: {lastUpdated}</span>
					<button class="btn btn-ghost btn-sm" on:click={refreshNow} disabled={isRefreshing}>
						{#if isRefreshing}Refreshing…{:else}Refresh{/if}
					</button>
				</div>
			</div>
		</div>

		<!-- Main grid -->
		<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
			<!-- Left column -->
			<div class="space-y-4">
				<!-- Statistics -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Statistics</h2>

						<div class="mt-1 space-y-3">
							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-info">⏱</span>
									<span>Heartbeat Time</span>
								</div>
								<span class="font-medium">{safeToLocaleString((worker?.lastHeartbeat ?? worker?.lastHeartbeatAt))}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-info">↗</span>
									<span>Pendent Jobs</span>
								</div>
								<span class="font-medium">{worker?.inflightJobs ?? '—'}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-success">▲</span>
									<span>Processed Jobs</span>
								</div>
								<span class="font-medium">{worker?.processedJobs?.toLocaleString() ?? '—'}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-warning">⚡</span>
									<span>Failed Jobs</span>
								</div>
								<span class="font-medium">{worker?.failedJobs?.toLocaleString() ?? '—'}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-primary">📦</span>
									<span>Buckets</span>
								</div>
								<span class="font-medium">{buckets.length}</span>
							</div>
						</div>
					</div>
				</div>
			</div>

			<!-- Right column -->
			<div class="space-y-4">
				<!-- Configuration -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Configuration</h2>

						<div class="grid grid-cols-1 gap-3 md:grid-cols-2">
							<div class="space-y-3">
								<div class="flex items-center justify-between">
									<div class="opacity-80">Mode</div>
									<div class="font-medium">{workerMode}</div>
								</div>
								<div class="flex items-center justify-between">
									<div class="opacity-80">Lane</div>
									<div class="font-medium">{worker?.workerLane ?? '—'}</div>
								</div>
							</div>

							<div class="space-y-3">
								<div class="flex items-center justify-between">
									<div class="opacity-80">Parallelism Factor</div>
									<div class="font-medium">{worker?.parallelismFactor?.toFixed(1) ?? '—'}</div>
								</div>
								<div class="flex items-center justify-between">
									<div class="opacity-80">Batch Size</div>
									<div class="font-medium">{worker?.batchSize?.toLocaleString() ?? '—'}</div>
								</div>
							</div>
						</div>
					</div>
				</div>

				<!-- Connection -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<h2 class="card-title text-base">Connection</h2>

						<div class="space-y-3">
							<div class="flex items-center justify-between">
								<div class="opacity-80">Agent Connection</div>
								<div class="font-medium">{worker?.agentConnectionName ?? worker?.agentConnectionId ?? '—'}</div>
							</div>
							<div class="flex items-center justify-between">
								<div class="opacity-80">Host</div>
								<div class="font-medium">{worker?.hostDisplayName ?? 'N/A'}</div>
							</div>
						</div>
					</div>
				</div>
			</div>

			<!-- Buckets (full width on large screens) -->
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg lg:col-span-2">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<h2 class="card-title text-base">Buckets</h2>
						<span class="text-sm opacity-60">{buckets.length}</span>
					</div>

					<div class="overflow-x-auto">
						<table class="table">
							<thead>
							<tr class="text-base-content/70">
								<th>ID</th>
								<th>Priority</th>
								<th>Status</th>
								<th>Host</th>
							</tr>
							</thead>
							<tbody>
							{#each buckets as b}
								{@const sLabel = bucketStatusLabel(b.status)}
								<tr class="hover cursor-pointer" on:click={() => goToBucket(b.id)}>
									<td class="font-mono text-xs">{b.id ?? '—'}</td>
									<td>{priorityLabel(b.priority)}</td>
									<td><span class={badgeForStatus(sLabel)}>{sLabel}</span></td>
									<td>{b.hostDisplayName ?? 'N/A'}</td>
								</tr>
							{:else}
								<tr><td colspan="4" class="text-center opacity-60">No buckets</td></tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
