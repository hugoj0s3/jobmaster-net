<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import Pager from "$lib/components/Pager.svelte";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";

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

	const urlParamDefs = {
		tab: { defaultValue: "All" as "All" | "Online" | "Offline" },
		q: { defaultValue: "" },
		sortBy: { defaultValue: "Host" as "Host" | "CPU" | "Memory" },
		asc: { defaultValue: true, ...Serializers.boolean },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	const _initParams = readUrlParams(urlParamDefs);
	type Tab = "All" | "Online" | "Offline";
	let tab: Tab = _initParams.tab;
	let query = _initParams.q;
	let sortBy: "Host" | "CPU" | "Memory" = _initParams.sortBy;
	let asc = _initParams.asc;

	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			tab: tab,
			q: query,
			sortBy,
			asc,
			page: pageIndex,
			size: pageSize
		});
	}

	$: tab, query, sortBy, asc, pageIndex, pageSize, syncToUrl();

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

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-6xl px-6 py-6">
		<div class="flex items-start justify-between gap-4">
			<h1 class="text-3xl font-semibold tracking-tight">Workers</h1>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last execution: {lastUpdated}</span>
				<button
					class="btn btn-ghost btn-sm btn-square"
					aria-label="Refresh now"
					on:click={refresh}
					disabled={isRefreshing}
				>
					<svg
						xmlns="http://www.w3.org/2000/svg"
						class={"h-4 w-4 " + (isRefreshing ? "animate-spin" : "")}
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path d="M21 12a9 9 0 1 1-3-6.7" />
						<path d="M21 3v6h-6" />
					</svg>
				</button>
			</div>
		</div>

		<section class="mt-6 grid grid-cols-1 gap-4 md:grid-cols-4">
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Workers Online</div>
							<div class="mt-1 text-4xl font-semibold">{onlineCount}</div>
						</div>
						<div class="rounded-2xl bg-success/15 p-3 text-success">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M20 6 9 17l-5-5"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Workers Offline</div>
							<div class="mt-1 text-4xl font-semibold">{offlineCount}</div>
						</div>
						<div class="rounded-2xl bg-error/15 p-3 text-error">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<circle cx="12" cy="12" r="9"/>
								<path d="M15 9l-6 6M9 9l6 6"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Avg. CPU Usage</div>
							<div class="mt-1 text-4xl font-semibold">{avgCpu}%</div>
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
							<div class="text-sm text-base-content/70">Avg. Memory Usage</div>
							<div class="mt-1 text-4xl font-semibold">{avgMem}%</div>
						</div>
						<div class="rounded-2xl bg-info/15 p-3 text-info">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M7 7h10v10H7z"/><path d="M4 10h3M17 10h3M4 14h3M17 14h3M10 4v3M14 4v3M10 17v3M14 17v3"/>
							</svg>
						</div>
					</div>
				</div>
			</div>
		</section>

		<div class="flex justify-end mt-6">
			<Pager
				bind:pageIndex
				bind:pageSize
				{totalCount}
				{currentCount}
				disabled={isRefreshing}
				showPageSize={true}
			/>
		</div>

		<section class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body gap-4">
				<div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
					<div class="tabs tabs-bordered">
						<button class:tab-active={tab === "All"} class="tab" on:click={() => (tab = "All")}>
							All <span class="ml-2 badge badge-ghost">{allCount}</span>
						</button>
						<button class:tab-active={tab === "Online"} class="tab" on:click={() => (tab = "Online")}>
							Online <span class="ml-2 badge badge-success">{onlineCount}</span>
						</button>
						<button class:tab-active={tab === "Offline"} class="tab" on:click={() => (tab = "Offline")}>
							Offline <span class="ml-2 badge badge-error">{offlineCount}</span>
						</button>
					</div>

					<div class="flex flex-col sm:flex-row gap-3 sm:items-center sm:justify-end w-full lg:w-auto">
						<label class="input input-bordered flex items-center gap-2 w-full sm:w-80">
							<span class="opacity-60">🔎</span>
							<input class="grow" placeholder="Search Workers" bind:value={query} />
						</label>

						<div class="join">
							<select class="select select-bordered join-item" bind:value={sortBy} aria-label="Sort field">
								<option value="Host">Sort: Host</option>
								<option value="CPU">Sort: CPU</option>
								<option value="Memory">Sort: Memory</option>
							</select>
							<button
								class="btn btn-bordered join-item"
								on:click={() => (asc = !asc)}
								title="Toggle sort direction"
							>
								{asc ? "A→Z" : "Z→A"}
							</button>
						</div>

					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/70">
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
							<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${r.id}`)}>
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
	</div>
</div>