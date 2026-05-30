<!-- src/routes/hosts/[id]/+page.svelte -->
<script lang="ts">
	import { onMount } from "svelte";
	import Pager from "$lib/components/Pager.svelte";
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
	let workersPageSize = 10;
	let workersPage = 0;
	let isRefreshing = false;
	let lastClusterId: string | null = null;
	let notFound = false;

	$: pagedWorkers = workers.slice(workersPage * workersPageSize, (workersPage + 1) * workersPageSize);

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
		const label: HostStatusLabel = (h as any).isAlive === false 
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

	async function refreshNow() {
		isRefreshing = true;
		notFound = false;
		try {
			const cid = clusterId();
			const hid = hostId();
			if (!cid || !hid) return;

			// If the cluster changed, redirect to the list
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
					params: { path: { clusterId: cid }, query: { HostId: hid } as any }
				});
				workers = (workersResponse.data ?? []) as any[];
			} catch (e) {
				console.error("Failed to fetch workers:", e);
			}
		} catch (error) {
			console.error("Failed to fetch host:", error);
		} finally {
			isRefreshing = false;
		}
	}

	onMount(() => {
		refreshNow();
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
			<div class="space-y-2">
				<div class="text-sm breadcrumbs opacity-70">
					<ul>
						<li><a href="/{clusterId()}/hosts" class="link link-hover">Hosts</a></li>
						<li>{hostName}</li>
					</ul>
				</div>
				<h1 class="text-3xl font-semibold tracking-tight">{hostName}</h1>
				<div class="flex items-center gap-2">
					<span class="inline-block h-2 w-2 rounded-full {hostStatus.dotClass}"></span>
					<span class={hostStatus.badgeClass}>{hostStatus.label}</span>
				</div>
			</div>
		</div>

		<div class="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">CPU Usage</div>
							<div class="mt-1 text-4xl font-semibold">{cpuPercent !== null ? cpuPercent + '%' : 'N/A'}</div>
							{#if cpuPercent !== null}
								<progress class="progress progress-success w-32 mt-2" value={cpuPercent} max="100" />
							{/if}
						</div>
						<div class="rounded-2xl bg-secondary/15 p-3 text-secondary">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M8 2h8v4H8z"/><path d="M6 6h12v16H6z"/><path d="M9 10h6M9 14h6M9 18h6"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Memory Usage</div>
							<div class="mt-1 text-4xl font-semibold">{memPercent !== null ? memPercent + '%' : 'N/A'}</div>
							{#if memPercent !== null}
								<div class="mt-1 text-xs opacity-60">{memGbUsed} / {memGbTotal} GB</div>
								<progress class="progress progress-info w-32 mt-1" value={memPercent} max="100" />
							{/if}
						</div>
						<div class="rounded-2xl bg-info/15 p-3 text-info">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M7 7h10v10H7z"/><path d="M4 10h3M17 10h3M4 14h3M17 14h3M10 4v3M14 4v3M10 17v3M14 17v3"/>
							</svg>
						</div>
					</div>
				</div>
			</div>
		</div>

		<div class="mt-6 space-y-6">
				<!-- Workers table -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
							<Pager
								bind:pageIndex={workersPage}
								bind:pageSize={workersPageSize}
								totalCount={workers.length}
								currentCount={pagedWorkers.length}
								showPageSize={true}
							/>
						</div>
						<div class="divider my-2"></div>

						<div class="overflow-x-auto">
							<table class="table">
								<thead>
								<tr class="text-base-content/70">
									<th>Worker</th>
									<th>Worker Lane</th>
									<th>Status</th>
								</tr>
								</thead>
								<tbody>
								{#each pagedWorkers as w (w.id)}
									<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${w.id}`)}>
										<td class="font-medium">{w.name ?? w.displayName ?? w.id ?? '—'}</td>
										<td class="opacity-80">{w.workerLane ?? '—'}</td>
										<td>
											<span class="badge badge-sm badge-outline {w.isAlive ? 'badge-success' : 'badge-error'}">{w.isAlive ? 'Online' : 'Offline'}</span>
										</td>
									</tr>
								{:else}
									<tr>
										<td colspan="3" class="text-center opacity-60 py-6">No workers assigned to this host.</td>
									</tr>
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
