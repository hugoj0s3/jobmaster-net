<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import Pager from "$lib/components/Pager.svelte";

	type ApiHostModel = components["schemas"]["ApiHostModel"];

	type WorkerStatus = "Online" | "Offline";

	type WorkerRow = {
		id: string;
		name: string;
		status: WorkerStatus;
		lane: string;
		hostName?: string;
		cpu: number; // %
		ram: number; // %
		ramText: string; // e.g. "64% (3.2 GB)"
		parallelism?: number;
		lastHeartbeat?: string;
	};

	const clusterId = () => $page.params.cluster;

	let rows: WorkerRow[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	$: lastUpdated = lastUpdatedAt.toLocaleString('en-US', {
		month: 'numeric',
		day: 'numeric',
		year: 'numeric',
		hour: 'numeric',
		minute: '2-digit',
		second: '2-digit',
		hour12: true
	});

	$: allCount = rows.length;
	$: onlineCount = rows.filter((r) => r.status === "Online").length;
	$: offlineCount = rows.filter((r) => r.status === "Offline").length;

	$: avgCpu = Math.round(
		rows.filter((r) => r.status === "Online").reduce((acc, r) => acc + r.cpu, 0) /
		Math.max(1, rows.filter((r) => r.status === "Online").length)
	);

	$: avgMem = Math.round(
		rows.filter((r) => r.status === "Online").reduce((acc, r) => acc + r.ram, 0) /
		Math.max(1, rows.filter((r) => r.status === "Online").length)
	);

	type Tab = "All" | "Online" | "Offline";
	let tab: Tab = "All";
	let query = "";
	let sortBy: "Host" | "CPU" | "Memory" = "Host";
	let asc = true;

	let pageIndex = 0;
	let pageSize = 10;

	function mapWorkerToRow(w: any, hostsMap: Map<string, ApiHostModel>): WorkerRow {
		const isAlive = w.isAlive === true;
		const host = w.hostId ? hostsMap.get(w.hostId) : undefined;

		const cpu = host?.cpuUsagePercent != null ? Math.round(host.cpuUsagePercent) : 0;
		const memTotal = host?.memoryTotalBytes ?? 0;
		const memUsed = host?.memoryUsedBytes ?? 0;
		const memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : 0;
		const memGb = memTotal > 0 ? (memUsed / 1024 ** 3).toFixed(1) : null;

		const status: WorkerStatus = isAlive ? "Online" : "Offline";

		return {
			id: w.id ?? "",
			name: w.displayName ?? w.id ?? "Unknown",
			status,
			lane: w.workerLane ?? "—",
			hostName: w.hostDisplayName ?? host?.displayName ?? "—",
			cpu: isAlive ? cpu : 0,
			ram: isAlive ? memPercent : 0,
			ramText: isAlive && memGb != null ? `${memPercent}% (${memGb} GB)` : "—",
			parallelism: w.parallelismFactor ?? undefined,
			lastHeartbeat: w.lastHeartbeatAt ?? undefined
		};
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const [workersResponse, hostsResponse] = await Promise.all([
				jmApi.GET("/{clusterId}/workers", {
					params: { path: { clusterId: cid } }
				}),
				jmApi.GET("/{clusterId}/hosts", {
					params: { path: { clusterId: cid } }
				})
			]);

			if (workersResponse.error) {
				console.error("API error (workers):", workersResponse.error);
				return;
			}

			const apiHosts = ((hostsResponse.data ?? []) as ApiHostModel[]);
			const hostsMap = new Map<string, ApiHostModel>();
			for (const h of apiHosts) {
				if (h.id) hostsMap.set(h.id, h);
			}

			const apiWorkers = ((workersResponse.data ?? []) as any[]);
			rows = apiWorkers.map((w) => mapWorkerToRow(w, hostsMap));
			lastUpdatedAt = new Date();
		} catch (error) {
			console.error("Failed to fetch workers:", error);
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

	const statusDot = (s: WorkerStatus) => {
		if (s === "Online") return "bg-success";
		return "bg-error";
	};

	const statusPill = (s: WorkerStatus) => {
		if (s === "Online") return "badge badge-outline badge-success rounded-full px-4 py-3";
		return "badge badge-outline badge-error rounded-full px-4 py-3";
	};

	const cpuBarClass = (s: WorkerStatus) => {
		if (s === "Online") return "progress progress-success";
		return "progress";
	};

	const memBarClass = (s: WorkerStatus) => {
		if (s === "Online") return "progress progress-info";
		return "progress";
	};

	$: filteredAll = rows
		.filter((r) => {
			if (tab !== "All" && r.status !== tab) return false;
			const q = query.trim().toLowerCase();
			if (!q) return true;
			return (
				r.name.toLowerCase().includes(q) ||
				r.lane.toLowerCase().includes(q) ||
				(r.hostName ?? "").toLowerCase().includes(q)
			);
		})
		.sort((a, b) => {
			const dir = asc ? 1 : -1;
			const cmpStr = (x: string, y: string) => x.localeCompare(y) * dir;
			const cmpNum = (x: number, y: number) => (x - y) * dir;

			if (sortBy === "Host") return cmpStr(a.name, b.name);
			if (sortBy === "CPU") return cmpNum(a.cpu, b.cpu);
			return cmpNum(a.ram, b.ram);
		});

	$: totalCount = filteredAll.length;
	$: filtered = filteredAll.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = filtered.length;

	$: if (tab || query || sortBy || asc) pageIndex = 0;

	function refresh() {
		refreshNow();
	}

	onMount(() => {
		refreshNow();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});
</script>

<div class="min-h-screen w-full bg-base-100 text-base-content">
	<div class="pointer-events-none fixed inset-0 opacity-60" />

	<main class="relative mx-auto max-w-6xl px-8 py-10">
		<div class="flex items-start justify-between gap-4">
			<h1 class="text-5xl font-bold tracking-tight text-base-content">Workers</h1>

			<div class="flex items-center gap-4 text-sm text-base-content/60">
				<span>Last execution: <span class="text-base-content/80">{lastUpdated}</span></span>
				<button
					class="btn btn-ghost btn-sm text-base-content/80 hover:text-base-content"
					on:click={refresh}
				>
					⟳ <span class="ml-1 font-semibold">Refresh</span>
				</button>
				<button
					class="btn btn-ghost btn-sm text-base-content/80 hover:text-base-content"
					aria-label="Settings"
				>
					⚙
				</button>
			</div>
		</div>

		<section class="mt-10 grid grid-cols-1 gap-6 md:grid-cols-4">
			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Workers Online</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{onlineCount}</p>
							<p class="mt-2 text-base-content/40">Workers Online</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-success/20 grid place-items-center text-success text-2xl">
							✓
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Workers Offline</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{offlineCount}</p>
							<p class="mt-2 text-base-content/40">Workers Offline</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-error/20 grid place-items-center text-error text-2xl">
							⨯
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Avg. CPU Usage</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{avgCpu}%</p>
							<p class="mt-2 text-base-content/40">Avg. CPU Usage</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-secondary/20 grid place-items-center text-secondary">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M8 2h8v4H8z"/><path d="M6 6h12v16H6z"/><path d="M9 10h6M9 14h6M9 18h6"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Avg. Memory Usage</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{avgMem}%</p>
							<p class="mt-2 text-base-content/40">Avg. Memory Usage</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-info/20 grid place-items-center text-info text-2xl">
							▦
						</div>
					</div>
				</div>
			</div>
		</section>

		<section class="mt-8 card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
			<div class="card-body">
				<div class="flex justify-end">
					<Pager
						bind:pageIndex
						bind:pageSize
						{totalCount}
						{currentCount}
						disabled={isRefreshing}
						showPageSize={true}
					/>
				</div>

				<div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
					<div class="flex items-center gap-10">
						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "All"}
							on:click={() => (tab = "All")}
						>
							All
							<span class="ml-3 badge rounded-full bg-base-300/50 border-base-300/60 text-base-content/80"
							>{allCount}</span
							>
						</button>

						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "Online"}
							on:click={() => (tab = "Online")}
						>
							Online
							<span class="ml-3 badge rounded-full bg-success text-black border-0">{onlineCount}</span>
						</button>

						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "Offline"}
							on:click={() => (tab = "Offline")}
						>
							Offline
							<span class="ml-3 badge rounded-full bg-error text-black border-0">{offlineCount}</span>
						</button>
					</div>

					<div class="flex flex-wrap items-center gap-3 w-full lg:w-auto lg:justify-end">
						<label
							class="input input-bordered bg-transparent border-base-300/60 text-base-content w-full sm:w-[320px] rounded-xl"
						>
							<span class="opacity-60">🔎</span>
							<input class="placeholder:text-base-content/40" placeholder="Search Workers" bind:value={query} />
						</label>

						<div class="join">
							<button class="btn join-item bg-transparent border-base-300/60 text-base-content/80 rounded-xl">
								Sort: {sortBy}
							</button>

							<details class="dropdown dropdown-end join-item">
								<summary class="btn bg-transparent border-base-300/60 text-base-content/80 rounded-xl">▾</summary>
								<ul class="menu dropdown-content mt-2 w-44 rounded-xl bg-base-200 border border-base-300 shadow">
									<li><button on:click={() => (sortBy = "Host")}>Host</button></li>
									<li><button on:click={() => (sortBy = "CPU")}>CPU</button></li>
									<li><button on:click={() => (sortBy = "Memory")}>Memory</button></li>
								</ul>
							</details>

							<button
								class="btn join-item bg-transparent border-base-300/60 text-base-content/80 rounded-xl"
								on:click={() => (asc = !asc)}
								title="Toggle sort direction"
							>
								{asc ? "A→Z" : "Z→A"}
							</button>
						</div>
					</div>
				</div>

				<div class="divider my-3 opacity-30" />

				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/60">
							<th>Status</th>
							<th>Worker</th>
							<th>Host</th>
							<th>Lane</th>
							<th>CPU Load</th>
							<th>Memory Usage</th>
							<th>Last Heartbeat</th>
						</tr>
						</thead>

						<tbody>
						{#each filtered as r (r.id)}
							<tr class="hover:bg-base-300/30 cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${r.id}`)}>
								<td>
									<div class="flex items-center gap-3">
										<span class={"h-2.5 w-2.5 rounded-full " + statusDot(r.status)} />
										<span class={statusPill(r.status)}>{r.status}</span>
									</div>
								</td>

								<td class="text-base-content font-medium">{r.name}</td>

								<td class="text-base-content/70">{r.hostName ?? "—"}</td>

								<td class="text-base-content/70">{r.lane}</td>

								<td>
									{#if r.status === "Online"}
										<div class="flex items-center gap-4">
											<progress class={cpuBarClass(r.status)} value={r.cpu} max="100" />
											<span class="w-12 text-base-content/80">{r.cpu}%</span>
										</div>
									{:else}
										<span class="text-base-content/40">—</span>
									{/if}
								</td>

								<td>
									{#if r.status === "Online"}
										<div class="flex items-center gap-4">
											<progress class={memBarClass(r.status)} value={r.ram} max="100" />
											<span class="text-base-content/80">{r.ramText}</span>
										</div>
									{:else}
										<span class="text-base-content/40">—</span>
									{/if}
								</td>

								<td class="text-base-content/70">{r.lastHeartbeat ?? "—"}</td>
							</tr>
						{/each}

						{#if filtered.length === 0}
							<tr>
								<td colspan="8" class="py-10 text-base-content/60">No workers found.</td>
							</tr>
						{/if}
						</tbody>
					</table>
				</div>
			</div>
		</section>
	</main>
</div>