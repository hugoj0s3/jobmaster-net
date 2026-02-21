<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import Pager from "$lib/components/Pager.svelte";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";

	type ApiHostModel = components["schemas"]["ApiHostModel"];

	const clusterId = () => $page.params.cluster;

	type HostStatus = "Online" | "Offline" | "Warning";

	type HostRow = {
		id: string;
		status: HostStatus;
		host: string;
		ip: string;
		cpu: number;
		memPercent?: number;
		memGb?: number;
		workers?: number;
		uptime?: string;
	};

	let rows: HostRow[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	const urlParamDefs = {
		tab: { defaultValue: "All" as "All" | "Online" | "Offline" },
		q: { defaultValue: "" },
		sortBy: { defaultValue: "host" as "host" | "cpu" | "mem" },
		sortDir: { defaultValue: "asc" as "asc" | "desc" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	const _initParams = readUrlParams(urlParamDefs);
	let activeTab: "All" | "Online" | "Offline" = _initParams.tab;
	let q = _initParams.q;
	let sortBy: "host" | "cpu" | "mem" = _initParams.sortBy;
	let sortDir: "asc" | "desc" = _initParams.sortDir;

	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			tab: activeTab,
			q,
			sortBy,
			sortDir,
			page: pageIndex,
			size: pageSize
		});
	}

	$: activeTab, q, sortBy, sortDir, pageIndex, pageSize, syncToUrl();

	$: onlineCount = rows.filter(r => r.status === "Online" || r.status === "Warning").length;
	$: offlineCount = rows.filter(r => r.status === "Offline").length;
	$: warningCount = rows.filter(r => r.status === "Warning").length;

	$: avgCpu =
		Math.round(
			rows.filter(r => r.status !== "Offline").reduce((acc, r) => acc + (r.cpu ?? 0), 0) /
			Math.max(1, rows.filter(r => r.status !== "Offline").length)
		);

	$: avgMem =
		Math.round(
			rows
				.filter(r => r.status !== "Offline" && typeof r.memPercent === "number")
				.reduce((acc, r) => acc + (r.memPercent ?? 0), 0) /
			Math.max(1, rows.filter(r => r.status !== "Offline" && typeof r.memPercent === "number").length)
		);

	$: lastUpdated = lastUpdatedAt.toLocaleString('en-US', {
		month: 'numeric',
		day: 'numeric',
		year: 'numeric',
		hour: 'numeric',
		minute: '2-digit',
		second: '2-digit',
		hour12: true
	});

	function mapHostToRow(host: ApiHostModel): HostRow {
		const memTotal = host.memoryTotalBytes ?? 0;
		const memUsed = host.memoryUsedBytes ?? 0;
		const memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : undefined;
		const memGb = memTotal > 0 ? Number((memUsed / (1024 ** 3)).toFixed(1)) : undefined;

		const cpu = host.cpuUsagePercent ?? 0;
		
		let status: HostStatus;
		if (host.cpuUsagePercent == null && host.memoryTotalBytes == null) {
			status = "Offline";
		} else if (cpu > 90 || (memPercent != null && memPercent > 90)) {
			status = "Warning";
		} else {
			status = "Online";
		}

		return {
			id: host.id ?? "",
			status,
			host: host.displayName ?? host.id ?? "Unknown",
			ip: "—",
			cpu: Math.round(cpu),
			memPercent,
			memGb,
			workers: undefined,
			uptime: undefined
		};
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const response = await jmApi.GET("/{clusterId}/hosts", {
				params: { path: { clusterId: cid } }
			});

			if (response.error) {
				console.error("API error:", response.error);
				return;
			}

			const apiHosts = (response.data ?? []) as ApiHostModel[];
			rows = apiHosts.map(mapHostToRow);
			lastUpdatedAt = new Date();
		} catch (error) {
			console.error("Failed to fetch hosts:", error);
			rows = [];
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

	function tabFilter(r: HostRow) {
		if (activeTab === "All") return true;
		if (activeTab === "Online") return r.status === "Online" || r.status === "Warning";
		return r.status === activeTab;
	}

	function handleTabChange(tab: "All" | "Online" | "Offline") {
		activeTab = tab;
		pageIndex = 0;
	}

	function textFilter(r: HostRow) {
		const s = `${r.host} ${r.ip} ${r.status}`.toLowerCase();
		return s.includes(q.trim().toLowerCase());
	}

	function sortValue(r: HostRow) {
		if (sortBy === "host") return r.host.toLowerCase();
		if (sortBy === "cpu") return r.cpu ?? 0;
		return r.memPercent ?? -1;
	}

	$: filteredAll = rows
		.filter(r => {
			if (activeTab === "Online") return r.status === "Online" || r.status === "Warning";
			if (activeTab === "Offline") return r.status === "Offline";
			return true;
		})
		.filter(r => {
			if (!q.trim()) return true;
			const s = `${r.host} ${r.ip} ${r.status}`.toLowerCase();
			return s.includes(q.trim().toLowerCase());
		})
		.sort((a, b) => {
			const av = sortBy === "host" ? a.host.toLowerCase() : sortBy === "cpu" ? (a.cpu ?? 0) : (a.memPercent ?? -1);
			const bv = sortBy === "host" ? b.host.toLowerCase() : sortBy === "cpu" ? (b.cpu ?? 0) : (b.memPercent ?? -1);
			const cmp = av < bv ? -1 : av > bv ? 1 : 0;
			return sortDir === "asc" ? cmp : -cmp;
		});

	$: totalCount = filteredAll.length;
	$: paginatedHosts = filteredAll.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = paginatedHosts.length;

	function refresh() {
		refreshNow();
	}

	function badgeColor(status: HostStatus) {
		if (status === "Online") return "badge-success";
		if (status === "Warning") return "badge-warning";
		return "badge-error";
	}

	function dotClass(status: HostStatus) {
		if (status === "Online") return "bg-success";
		if (status === "Warning") return "bg-warning";
		return "bg-error";
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
			<h1 class="text-3xl font-semibold tracking-tight">Hosts</h1>

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

		<div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-6">
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Hosts Online</div>
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
							<div class="text-sm text-base-content/70">Hosts Offline</div>
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
		</div>

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

		<!-- Filters row -->
		<div class="card bg-base-200/60 border border-base-300/60 shadow-lg mt-4">
			<div class="card-body gap-4">
				<div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
					<!-- Tabs -->
					<div class="tabs tabs-bordered">
						<button class:tab-active={activeTab === "All"} class="tab" on:click={() => handleTabChange("All")}>
							All <span class="ml-2 badge badge-ghost">{rows.length}</span>
						</button>
						<button class:tab-active={activeTab === "Online"} class="tab" on:click={() => handleTabChange("Online")}>
							Online <span class="ml-2 badge badge-success">{onlineCount}</span>
						</button>
						<button class:tab-active={activeTab === "Offline"} class="tab" on:click={() => handleTabChange("Offline")}>
							Offline <span class="ml-2 badge badge-error">{offlineCount}</span>
						</button>
					</div>

					<!-- Search + Sort + Pager -->
					<div class="flex flex-col sm:flex-row gap-3 sm:items-center sm:justify-end w-full lg:w-auto">
						<label class="input input-bordered flex items-center gap-2 w-full sm:w-80">
							<span class="opacity-60">🔎</span>
							<input class="grow" placeholder="Search Hosts" bind:value={q} />
						</label>

						<div class="join">
							<select class="select select-bordered join-item" bind:value={sortBy} aria-label="Sort field">
								<option value="host">Sort: Host</option>
								<option value="cpu">Sort: CPU</option>
								<option value="mem">Sort: Memory</option>
							</select>
							<button
								class="btn btn-bordered join-item"
								on:click={() => (sortDir = sortDir === "asc" ? "desc" : "asc")}
								title="Toggle sort direction"
							>
								{sortDir === "asc" ? "A→Z" : "Z→A"}
							</button>
						</div>

					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/70">
							<th>Status</th>
							<th>Host</th>
							<th>IP Address</th>
							<th>CPU Load</th>
							<th>Memory Usage</th>
							<th>Workers</th>
							<th>Uptime</th>
						</tr>
						</thead>
						<tbody>
						{#each paginatedHosts as r (r.id)}
							<tr class="hover cursor-pointer {r.status === 'Offline' ? 'opacity-80' : ''}" on:click={() => goto(`/${clusterId()}/hosts/${r.id}`)}>
								<td>
									<div class="flex items-center gap-2">
										<span class={`inline-block h-2.5 w-2.5 rounded-full ${dotClass(r.status)}`} />
										<span class={`badge badge-outline ${badgeColor(r.status)}`}>{r.status}</span>
									</div>
								</td>

								<td class="font-medium">{r.host}</td>
								<td class="opacity-80">{r.ip}</td>

								<td>
									{#if r.status === "Offline"}
										<span class="opacity-60">—</span>
									{:else}
										<div class="flex items-center gap-3">
											<progress class="progress progress-success w-28" value={r.cpu} max="100" />
											<span class="tabular-nums">{r.cpu}%</span>
										</div>
									{/if}
								</td>

								<td>
									{#if r.status === "Offline" || r.memPercent == null}
										<span class="opacity-60">—</span>
									{:else}
										<div class="flex items-center gap-3">
											<progress class="progress progress-info w-28" value={r.memPercent} max="100" />
											<span class="tabular-nums">{r.memPercent}% ({r.memGb} GB)</span>
										</div>
									{/if}
								</td>

								<td class="tabular-nums">
									{#if r.status === "Offline"}
										<span class="badge badge-ghost">Offline</span>
									{:else}
										{r.workers ?? "—"}
									{/if}
								</td>

								<td class="tabular-nums">
									{#if r.status === "Offline"}
										<span class="opacity-60">Offline</span>
									{:else}
										{r.uptime ?? "—"}
									{/if}
								</td>
							</tr>
						{/each}
						</tbody>
					</table>
				</div>

				<div class="flex items-center gap-6 text-sm opacity-80">
					<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-success"></span> Online</div>
					<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-warning"></span> Warning</div>
					<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-error"></span> Offline</div>
				</div>
			</div>
		</div>
	</div>
</div>