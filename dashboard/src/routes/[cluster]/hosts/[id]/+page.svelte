<!-- src/routes/hosts/[id]/+page.svelte -->
<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { HostStatusUtil, type HostStatusLabel } from "$lib/helper/host-status-utils";


	type ApiHostModel = components["schemas"]["ApiHostModel"];

	const clusterId = () => $page.params.cluster;
	const hostId = () => $page.params.id;

	let host: ApiHostModel | null = null;
	let workers: any[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	$: hostName = host?.hostDisplayName ?? host?.displayName ?? host?.id ?? "Unknown";

	$: hostStatus = deriveStatus(host);

	function deriveStatus(h: ApiHostModel | null): { label: HostStatusLabel; dotClass: string; badgeClass: string } {
		if (!h || (h.cpuUsagePercent == null && h.memoryTotalBytes == null)) {
			return {
				label: HostStatusUtil.Label.Offline,
				dotClass: HostStatusUtil.getDotClass(HostStatusUtil.Label.Offline),
				badgeClass: `badge badge-outline ${HostStatusUtil.getBadgeClass(HostStatusUtil.Label.Offline)}`
			};
		}
		const cpu = h.cpuUsagePercent ?? 0;
		const memTotal = h.memoryTotalBytes ?? 0;
		const memUsed = h.memoryUsedBytes ?? 0;
		const memPercent = memTotal > 0 ? (memUsed / memTotal) * 100 : 0;

		const label: HostStatusLabel =
			cpu > 90 || memPercent > 90
				? HostStatusUtil.Label.Warning
				: HostStatusUtil.Label.Online;

		return {
			label,
			dotClass: HostStatusUtil.getDotClass(label),
			badgeClass: `badge badge-outline ${HostStatusUtil.getBadgeClass(label)}`
		};
	}

	$: memTotal = host?.memoryTotalBytes ?? 0;
	$: memGbTotal = memTotal > 0 ? (memTotal / 1024 ** 3).toFixed(1) : null;


	$: lastUpdated = lastUpdatedAt.toLocaleString("en-US", {
		month: "numeric",
		day: "numeric",
		year: "numeric",
		hour: "numeric",
		minute: "2-digit",
		second: "2-digit",
		hour12: true
	});


	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			const hid = hostId();
			if (!cid || !hid) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const hostResponse = await jmApi.GET("/{clusterId}/hosts/{hostId}", {
				params: { path: { clusterId: cid, hostId: hid } }
			});

			if (hostResponse.error) {
				console.error("API error (host):", hostResponse.error);
				return;
			}

			host = (hostResponse.data ?? null) as ApiHostModel | null;

	
			try {
				const workersResponse = await jmApi.GET("/{clusterId}/workers", {
					params: { path: { clusterId: cid } }
				});
				const allWorkers = ((workersResponse.data ?? []) as any[]);
				workers = allWorkers.filter((w: any) => w.hostId === hid || w.hostDisplayName === host?.displayName);
			} catch (e) {
				console.error("Failed to fetch workers:", e);
			}

			lastUpdatedAt = new Date();
		} catch (error) {
			console.error("Failed to fetch host:", error);
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
		refreshNow();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<div class="flex flex-col gap-4">
			<div class="flex items-start justify-between gap-4">
				<div class="space-y-2">
					<div class="text-sm breadcrumbs">
						<ul>
							<li><a href="/{clusterId()}/hosts" class="link link-hover">Hosts</a></li>
							<li>{host?.id ?? ''}</li>
						</ul>
					</div>

					<h1 class="text-3xl font-semibold tracking-tight">
						Host Details - {hostName}
					</h1>

					<div class="flex flex-wrap items-center gap-3 text-sm text-base-content/70">
            <span class="inline-flex items-center gap-2">
              <span class="inline-block h-2 w-2 rounded-full {hostStatus.dotClass}"></span>
              <span class={hostStatus.badgeClass}>{hostStatus.label}</span>
            </span>
						{#if memGbTotal}
							<span class="opacity-60">•</span>
							<span>{memGbTotal} GB RAM</span>
						{/if}
					</div>
				</div>

				<div class="flex flex-col items-end gap-2">
					<div class="flex items-center gap-2 text-sm text-base-content/60">
						<span>Last updated: {lastUpdated}</span>
						<button class="btn btn-ghost btn-sm" on:click={refreshNow} disabled={isRefreshing}>
							{#if isRefreshing}Refreshing…{:else}Refresh{/if}
						</button>
					</div>
				</div>
			</div>

			<!-- KPI cards row -->
			<div class="grid grid-cols-1 gap-4">
				<!-- Status card -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body gap-3">
						<div class="flex items-center justify-between">
							<div class="text-sm font-semibold text-base-content/80">{hostStatus.label}</div>
							<div class="text-error">
								<!-- lightning -->
								<svg width="26" height="26" viewBox="0 0 24 24" fill="none">
									<path
										d="M13 2 3 14h7l-1 8 12-14h-7l-1-6Z"
										stroke="currentColor"
										stroke-width="1.5"
										stroke-linejoin="round"
									/>
								</svg>
							</div>
						</div>

						<div class="text-3xl font-semibold">{hostStatus.label}</div>
						<div class="text-sm text-base-content/60">Host ID: {host?.id ?? '—'}</div>
					</div>
				</div>
			</div>

		</div>

		<!-- Content -->
			<div class="mt-6 space-y-6">

				<!-- Workers table -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
						</div>

						<div class="overflow-x-auto">
							<table class="table">
								<thead>
								<tr class="text-base-content/70">
									<th>Worker</th>
									<th>ID</th>
									<th>Last Heartbeat</th>
									<th>Parallelism Factor</th>
									<th>Lane</th>
									<th class="text-right">Status</th>
								</tr>
								</thead>
								<tbody>
								{#each workers as w}
									<tr class="hover">
										<td class="font-medium">
											<div class="flex items-center gap-3">
												<span class="h-2 w-2 rounded-full {w.isAlive ? 'bg-success' : 'bg-error'} opacity-80"></span>
												{w.displayName ?? w.id ?? '—'}
											</div>
										</td>
										<td class="font-mono text-sm opacity-80">{w.id ?? '—'}</td>
										<td class="opacity-80">{w.lastHeartbeatAt ?? '—'}</td>
										<td>
											<span class="opacity-80">{w.parallelismFactor ?? '—'}</span>
										</td>
										<td class="opacity-80">{w.workerLane ?? '—'}</td>
										<td class="text-right">
											<span class="badge {w.isAlive ? 'badge-success' : 'badge-error'} badge-outline">{w.isAlive ? 'Online' : 'Offline'}</span>
										</td>
									</tr>
								{:else}
									<tr>
										<td colspan="6" class="text-center opacity-60 py-6">No workers assigned to this host.</td>
									</tr>
								{/each}
								</tbody>
							</table>
						</div>

						<div class="mt-2 text-sm text-base-content/60">
							{workers.length} worker{workers.length !== 1 ? 's' : ''} assigned
						</div>
					</div>
				</div>
			</div>
	</div>
</div>

