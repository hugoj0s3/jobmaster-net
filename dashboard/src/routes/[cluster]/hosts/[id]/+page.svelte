<!-- src/routes/hosts/[id]/+page.svelte -->
<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { HostStatusUtil, type HostStatusLabel } from "$lib/helper/host-status-utils";
	import AreaChart from "$lib/components/AreaChart.svelte";

	type ApiHostModel = components["schemas"]["ApiHostModel"];

	const clusterId = () => $page.params.cluster;
	const hostId = () => $page.params.id;

	let host: ApiHostModel | null = null;
	let workers: any[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	$: hostName = host?.displayName ?? host?.id ?? "Unknown";

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

	$: cpuPercent = host?.cpuUsagePercent != null ? Math.round(host.cpuUsagePercent) : null;

	$: memTotal = host?.memoryTotalBytes ?? 0;
	$: memUsed = host?.memoryUsedBytes ?? 0;
	$: memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : null;
	$: memGbUsed = memTotal > 0 ? (memUsed / 1024 ** 3).toFixed(1) : null;
	$: memGbTotal = memTotal > 0 ? (memTotal / 1024 ** 3).toFixed(1) : null;

	$: kpis = [
		{
			title: "CPU Load",
			value: cpuPercent != null ? `${cpuPercent}%` : "—",
			sub: host?.threadCount != null ? `${host.threadCount} threads` : "",
			class: "bg-base-200/60 border-base-300"
		},
		{
			title: "Memory Usage",
			value: memPercent != null ? `${memPercent}%` : "—",
			sub: memGbUsed != null ? `${memGbUsed} GB / ${memGbTotal} GB` : "",
			class: "bg-base-200/60 border-base-300"
		},
		{
			title: "Threads / Handles",
			value: host?.threadCount != null ? `${host.threadCount}` : "—",
			sub: host?.handleCount != null ? `${host.handleCount} handles` : "",
			class: "bg-base-200/60 border-base-300"
		}
	];

	$: lastUpdated = lastUpdatedAt.toLocaleString("en-US", {
		month: "numeric",
		day: "numeric",
		year: "numeric",
		hour: "numeric",
		minute: "2-digit",
		second: "2-digit",
		hour12: true
	});

	type MetricPoint = { time: number; value: number };

	let cpuHistory: MetricPoint[] = [];
	let memHistory: MetricPoint[] = [];

	const MAX_HISTORY_POINTS = 8640;

	const rangeSec: Record<string, number> = {
		"1m": 60,
		"15m": 15 * 60,
		"1h": 60 * 60,
		"8h": 8 * 60 * 60,
		"24h": 24 * 60 * 60
	};

	function pushMetric(arr: MetricPoint[], value: number | null | undefined): MetricPoint[] {
		if (value == null) return arr;
		const next = [...arr, { time: Date.now(), value }];
		return next.length > MAX_HISTORY_POINTS ? next.slice(next.length - MAX_HISTORY_POINTS) : next;
	}

	function filterByRange(arr: MetricPoint[], range: string): MetricPoint[] {
		const cutoff = Date.now() - (rangeSec[range] ?? 3600) * 1000;
		return arr.filter((p) => p.time >= cutoff);
	}

	$: cpuChartData = filterByRange(cpuHistory, selectedRange);
	$: memChartData = filterByRange(memHistory, selectedRange);

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

			if (host) {
				cpuHistory = pushMetric(cpuHistory, host.cpuUsagePercent);
				const mt = host.memoryTotalBytes ?? 0;
				const mu = host.memoryUsedBytes ?? 0;
				const mp = mt > 0 ? (mu / mt) * 100 : undefined;
				memHistory = pushMetric(memHistory, mp);
			}

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

	let tab: "metrics" | "workers" = "metrics";
	const ranges = ["1m", "15m", "1h", "8h", "24h"];
	let selectedRange = "1h";

	onMount(() => {
		refreshNow();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<!-- Page background -->
<div class="min-h-screen w-full bg-base-100">
	<!-- Top spacing container (no navbar/sidebar) -->
	<div class="mx-auto w-full max-w-6xl px-6 py-8">
		<!-- Header row -->
		<div class="flex flex-col gap-4">
			<div class="flex items-start justify-between gap-4">
				<div class="space-y-2">
					<div class="text-sm breadcrumbs">
						<ul>
							<li><a href="/{clusterId()}/hosts" class="link link-hover">Hosts</a></li>
							<li>{hostName}</li>
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
			<div class="grid grid-cols-1 gap-4 md:grid-cols-4">
				<!-- Status card -->
				<div class="card border border-base-300 bg-base-200/60">
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

				{#each kpis as k}
					<div class={"card border " + k.class}>
						<div class="card-body gap-3">
							<div class="flex items-center justify-between">
								<div class="text-sm font-semibold text-base-content/80">{k.title}</div>
								<div class="opacity-70">
									<!-- simple icon -->
									<svg width="26" height="26" viewBox="0 0 24 24" fill="none">
										<path
											d="M4 7h16M4 12h16M4 17h16"
											stroke="currentColor"
											stroke-width="1.5"
											stroke-linecap="round"
										/>
									</svg>
								</div>
							</div>

							<div class="text-3xl font-semibold">{k.value}</div>
							<div class="text-sm text-base-content/60">{k.sub}</div>
						</div>
					</div>
				{/each}
			</div>

			<!-- Tabs + actions -->
			<div class="flex flex-col gap-3 pt-2 md:flex-row md:items-center md:justify-between">
				<div role="tablist" class="tabs tabs-bordered">
					<button
						role="tab"
						class={"tab " + (tab === "metrics" ? "tab-active" : "")}
						on:click={() => (tab = "metrics")}
					>
						Metrics
					</button>
					<button
						role="tab"
						class={"tab " + (tab === "workers" ? "tab-active" : "")}
						on:click={() => (tab = "workers")}
					>
						Workers Assigned
					</button>
				</div>

				<div class="flex items-center gap-2">
					<details class="dropdown dropdown-end">
						<summary class="btn btn-outline btn-sm">Actions</summary>
						<ul class="menu dropdown-content z-[1] mt-2 w-52 rounded-box bg-base-200 p-2 shadow">
							<li><a>Restart host</a></li>
							<li><a>Mark as drained</a></li>
							<li><a>View logs</a></li>
						</ul>
					</details>

					<button class="btn btn-ghost btn-sm btn-square" aria-label="panel settings">
						<svg width="18" height="18" viewBox="0 0 24 24" fill="none" class="opacity-80">
							<path
								d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"
								stroke="currentColor"
								stroke-width="1.5"
							/>
							<path
								d="M19.4 15a8.4 8.4 0 0 0 .1-2l2-1.2-2-3.4-2.2.7a7.9 7.9 0 0 0-1.7-1l-.3-2.3H9.7L9.4 8a7.9 7.9 0 0 0-1.7 1l-2.2-.7-2 3.4 2 1.2a8.4 8.4 0 0 0 0 2l-2 1.2 2 3.4 2.2-.7c.5.4 1.1.7 1.7 1l.3 2.3h5.6l.3-2.3c.6-.3 1.2-.6 1.7-1l2.2.7 2-3.4-2-1.2Z"
								stroke="currentColor"
								stroke-width="1.5"
								stroke-linejoin="round"
							/>
						</svg>
					</button>
				</div>
			</div>
		</div>

		<!-- Content -->
		{#if tab === "metrics"}
			<div class="mt-6 space-y-6">
				<!-- Charts row -->
				<div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
					<div class="card border border-base-300 bg-base-200/40">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<h2 class="card-title text-base">CPU Usage</h2>

								<div class="join">
									{#each ranges as r}
										<button
											class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
											on:click={() => (selectedRange = r)}
										>
											{r}
										</button>
									{/each}
								</div>
							</div>

							<div class="mt-4 rounded-xl border border-base-300 bg-base-100/40 p-2">
								<AreaChart
									data={cpuChartData}
									maxValue={100}
									color="#36d399"
									label="CPU"
									unit="%"
								/>
							</div>

							<div class="mt-2 flex flex-wrap gap-4 text-sm text-base-content/60">
								<span class="inline-flex items-center gap-2">
									<span class="h-2 w-2 rounded-full bg-success"></span> CPU %
								</span>
							</div>
						</div>
					</div>

					<div class="card border border-base-300 bg-base-200/40">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<h2 class="card-title text-base">Memory Usage</h2>

								<div class="join">
									{#each ranges as r}
										<button
											class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
											on:click={() => (selectedRange = r)}
										>
											{r}
										</button>
									{/each}
								</div>
							</div>

							<div class="mt-4 rounded-xl border border-base-300 bg-base-100/40 p-2">
								<AreaChart
									data={memChartData}
									maxValue={100}
									color="#3abff8"
									label="Memory"
									unit="%"
								/>
							</div>

							<div class="mt-2 flex flex-wrap gap-4 text-sm text-base-content/60">
								<span class="inline-flex items-center gap-2">
									<span class="h-2 w-2 rounded-full bg-info"></span> Memory %
								</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Workers table -->
				<div class="card border border-base-300 bg-base-200/40">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
							<button class="btn btn-link btn-sm">View all Workers →</button>
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
									<tr>
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
		{:else}
			<!-- Workers tab (simple view) -->
			<div class="mt-6">
				<div class="card border border-base-300 bg-base-200/40">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
							<div class="join">
								{#each ranges as r}
									<button
										class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
										on:click={() => (selectedRange = r)}
									>
										{r}
									</button>
								{/each}
							</div>
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
									<tr>
										<td class="font-medium">{w.displayName ?? w.id ?? '—'}</td>
										<td class="font-mono text-sm opacity-80">{w.id ?? '—'}</td>
										<td class="opacity-80">{w.lastHeartbeatAt ?? '—'}</td>
										<td class="opacity-80">{w.parallelismFactor ?? '—'}</td>
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

						<div class="mt-2 flex justify-end text-sm text-base-content/60">
							{workers.length} worker{workers.length !== 1 ? 's' : ''} assigned
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>

<style>
    /* Optional: subtle "glow" vibe similar to the screenshot */
    :global(body) {
        background: radial-gradient(1200px 600px at 70% 0%, rgba(120, 90, 255, 0.18), transparent 60%),
        radial-gradient(900px 500px at 20% 20%, rgba(0, 180, 255, 0.12), transparent 55%),
        radial-gradient(900px 600px at 80% 60%, rgba(255, 120, 180, 0.10), transparent 55%),
        hsl(var(--b1));
    }
</style>
