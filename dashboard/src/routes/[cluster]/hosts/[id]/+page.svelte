<!-- src/routes/hosts/[id]/+page.svelte -->
<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { HostStatusUtil, type HostStatusLabel } from "$lib/helper/host-status-utils";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";


	type ApiHostModel = components["schemas"]["ApiHostModel"];

	const clusterId = () => $page.params.cluster;
	const hostId = () => $page.params.id;

	let host: ApiHostModel | null = null;
	let workers: any[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;
	let lastClusterId: string | null = null;
	let notFound = false;

	$: hostName = host?.hostDisplayName ?? host?.id ?? "Unknown";

	$: hostStatus = deriveStatus(host);

	function deriveStatus(h: ApiHostModel | null): { label: HostStatusLabel; dotClass: string; badgeClass: string } {
		if (!h) {
			return {
				label: HostStatusUtil.Label.Offline,
				dotClass: HostStatusUtil.getDotClass(HostStatusUtil.Label.Offline),
				badgeClass: `badge badge-outline ${HostStatusUtil.getBadgeClass(HostStatusUtil.Label.Offline)}`
			};
		}
		
		// Use isAlive property from API (same logic as hosts list page)
		const label: HostStatusLabel = h.isAlive === false 
			? HostStatusUtil.Label.Offline 
			: HostStatusUtil.Label.Online;

		return {
			label,
			dotClass: HostStatusUtil.getDotClass(label),
			badgeClass: `badge badge-outline ${HostStatusUtil.getBadgeClass(label)}`
		};
	}

	$: memTotal = host?.memoryTotalBytes ?? 0;
	$: memUsed = host?.memoryUsedBytes ?? 0;
	$: memGbTotal = memTotal > 0 ? (memTotal / 1024 ** 3).toFixed(1) : null;
	$: memGbUsed = memUsed > 0 ? (memUsed / 1024 ** 3).toFixed(1) : null;
	$: memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : null;

	$: cpuPercent = host?.cpuUsagePercent != null ? Math.round(host.cpuUsagePercent) : null;

	$: lastUpdated = DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt);


	async function refreshNow() {
		isRefreshing = true;
		notFound = false;
		try {
			const cid = clusterId();
			const hid = hostId();
			if (!cid || !hid) return;

			// Se o cluster mudou, redireciona para a lista
			if (lastClusterId && lastClusterId !== cid) {
				goto(`/${cid}/hosts`);
				return;
			}
			lastClusterId = cid;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const hostResponse = await jmApi.GET("/{clusterId}/hosts/{hostId}", {
				params: { path: { clusterId: cid, hostId: hid } }
			});

			if (hostResponse.error) {
				console.error("API error (host):", hostResponse.error);
				if (hostResponse.response?.status === 404) {
					notFound = true;
				}
				return;
			}

			host = (hostResponse.data ?? null) as ApiHostModel | null;

			if (!host) {
				notFound = true;
				return;
			}

			try {
				const workersResponse = await jmApi.GET("/{clusterId}/workers", {
					params: { path: { clusterId: cid } }
				});
				const allWorkers = ((workersResponse.data ?? []) as any[]);

				workers = allWorkers.filter((w: any) => {
					// Try multiple matching strategies with string conversion
					const workerIdStr = String(w.id || '').toLowerCase();
					const workerDisplayNameStr = String(w.displayName || '').toLowerCase();
					const hostIdStr = String(hid || '').toLowerCase();
					const hostNameStr = String(host?.name || '').toLowerCase();
					const hostDisplayNameStr = String(host?.displayName || '').toLowerCase();
					
					const matchesById = workerIdStr === hostIdStr;
					const matchesByName = workerDisplayNameStr === hostNameStr || workerDisplayNameStr === hostDisplayNameStr;
					const matchesWorkerName = workerIdStr === hostNameStr || workerIdStr === hostDisplayNameStr;
					
					const matches = matchesById || matchesByName || matchesWorkerName;

					return matches;
				});
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
		{#if notFound}
			<div class="flex items-center justify-center py-20">
				<div class="text-center max-w-2xl">
					<div class="mb-8">
						<h1 class="text-9xl font-bold text-primary opacity-20">404</h1>
					</div>
					<div class="space-y-4">
						<h2 class="text-3xl font-semibold">Host Not Found</h2>
						<p class="text-base-content/70 text-lg">
							The host you're looking for doesn't exist in this cluster or has been removed.
						</p>
					</div>
					<div class="mt-8 flex gap-4 justify-center">
						<button
							class="btn btn-primary"
							on:click={() => goto(`/${clusterId()}/hosts`)}
						>
							Go to Hosts List
						</button>
						<button
							class="btn btn-ghost"
							on:click={() => window.history.back()}
						>
							Go Back
						</button>
					</div>
				</div>
			</div>
		{:else}
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
									<th>CPU Usage</th>
									<th>Memory Usage</th>
									<th>Last Heartbeat</th>
									<th>Parallelism Factor</th>
									<th>Lane</th>
									<th class="text-right">Status</th>
								</tr>
								</thead>
								<tbody>
								{#each workers as w (w.id)}
									{@const cpu = w.cpuUsagePercent != null ? Math.round(w.cpuUsagePercent) : null}
									{@const memTotal = w.memoryTotalBytes ?? 0}
									{@const memUsed = w.memoryUsedBytes ?? 0}
									{@const memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : null}
									{@const memGb = memUsed > 0 ? (memUsed / 1024 ** 3).toFixed(1) : null}
									<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${w.id}`)}>
										<td class="font-medium">
											<div class="flex items-center gap-3">
												<span class="h-2 w-2 rounded-full {w.isAlive ? 'bg-success' : 'bg-error'} opacity-80"></span>
												{w.displayName ?? w.id ?? '—'}
											</div>
										</td>
										<td class="font-mono text-sm opacity-80">{w.id ?? '—'}</td>
										<td>
											{#if !w.isAlive}
												<span class="opacity-60">—</span>
											{:else if cpu === null}
												<span class="opacity-60">N/A</span>
											{:else}
												<div class="flex items-center gap-3">
													<progress class="progress progress-secondary w-20" value={cpu} max="100" />
													<span class="tabular-nums text-sm">{cpu}%</span>
												</div>
											{/if}
										</td>
										<td>
											{#if !w.isAlive}
												<span class="opacity-60">—</span>
											{:else if memPercent === null}
												<span class="opacity-60">N/A</span>
											{:else}
												<div class="flex items-center gap-3">
													<progress class="progress progress-info w-20" value={memPercent} max="100" />
													<span class="tabular-nums text-sm">{memPercent}% ({memGb} GB)</span>
												</div>
											{/if}
										</td>
										<td class="opacity-80">{DateDisplayUtil.formatRelativeOrDate(w.lastHeartbeatAt)}</td>
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
										<td colspan="8" class="text-center opacity-60 py-6">No workers assigned to this host.</td>
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
		{/if}
	</div>
</div>
