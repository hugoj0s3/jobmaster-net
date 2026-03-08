<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
	import FilterItem from "$lib/components/filters/FilterItem.svelte";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";

	type ApiHostModel = components["schemas"]["ApiHostModel"];

	const clusterId = () => $page.params.cluster;

	type HostStatus = "Online" | "Offline";

	type HostRow = {
		id: string;
		status: HostStatus;
		host: string;
		hostDisplayName?: string;
		ip: string;
		cpu: number | null;
		memPercent?: number;
		memGb?: number;
		workers?: number;
		uptime?: string;
		createdAt?: string;
	};

	let rows: HostRow[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;
	const refreshIntervalSec = 10;

	const urlParamDefs = {
		statuses: { defaultValue: [] as string[], ...Serializers.stringArray },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	let _initParams = readUrlParams(urlParamDefs);

	let selectedStatuses: string[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];

	type FilterValues = Record<string, unknown>;
	let filterValues: FilterValues = {};

	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
		filterValues = {};
		refreshNow();
	}

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			statuses: selectedStatuses,
			page: pageIndex,
			size: pageSize
		});
	}

	$: filterValues, selectedStatuses, pageIndex, pageSize, syncToUrl();

	$: onlineCount = rows.filter(r => r.status === "Online").length;
	$: offlineCount = rows.filter(r => r.status === "Offline").length;

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

	function mapHostToRow(host: any): HostRow {
		const memTotal = host.memoryTotalBytes ?? 0;
		const memUsed = host.memoryUsedBytes ?? 0;
		const memPercent = memTotal > 0 ? Math.round((memUsed / memTotal) * 100) : undefined;
		const memGb = memTotal > 0 ? Number((memUsed / (1024 ** 3)).toFixed(1)) : undefined;

		const cpu = host.cpuUsagePercent != null ? Math.round(host.cpuUsagePercent) : null;
		
		let status: HostStatus;
		if (host.isAlive === false) {
			status = "Offline";
		} else {
			status = "Online";
		}

		return {
			id: host.id ?? "",
			status,
			host: host.id ?? "Unknown",
			hostDisplayName: host.hostDisplayName,
			ip: "—",
			cpu,
			memPercent,
			memGb,
			workers: undefined,
			uptime: undefined,
			createdAt: host?.createdAt ?? host?.registeredAt ?? undefined
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

	$: filteredAll = rows
		.filter(r => {
			if (selectedStatuses.length > 0 && !selectedStatuses.includes(r.status)) return false;

			const dt = (filterValues.createdAt ?? {}) as { from?: string; to?: string };
			if (dt.from && r.createdAt && new Date(r.createdAt) < new Date(dt.from)) return false;
			if (dt.to && r.createdAt && new Date(r.createdAt) > new Date(dt.to)) return false;

			return true;
		});

	$: totalCount = filteredAll.length;
	$: paginatedHosts = filteredAll.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = paginatedHosts.length;

	function refresh() {
		refreshNow();
	}

	function badgeColor(status: HostStatus) {
		if (status === "Online") return "badge-success";
		return "badge-error";
	}

	function dotClass(status: HostStatus) {
		if (status === "Online") return "bg-success";
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
				<span>Last Refresh: {lastUpdated}</span>
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

		<div class="flex items-center justify-between gap-4 mt-6">
			{#key filterKey}
			<div class="flex flex-wrap items-center gap-2">
				<FilterDropdownMulti
					label="Status"
					options={[
						{ value: "Online", label: "Online" },
						{ value: "Offline", label: "Offline" }
					]}
					bind:values={selectedStatuses}
					on:change={() => { pageIndex = 0; }}
				/>

				<FilterContainer
					initialValues={filterValues}
					on:change={(e) => {
						filterValues = e.detail;
						pageIndex = 0;
					}}
				>
					<FilterItem
						id="createdAt"
						label="Created at"
						type="datetime"
						presets={[
							{ type: "LAST_MINUTES", minutes: 15, label: "Last 15 min" },
							{ type: "LAST_MINUTES", minutes: 30, label: "Last 30 min" },
							{ type: "LAST_MINUTES", minutes: 60, label: "Last 60 min" }
						]}
					/>
				</FilterContainer>
			</div>
			{/key}

			<Pager
				bind:pageIndex
				bind:pageSize
				{totalCount}
				{currentCount}
				disabled={isRefreshing}
				showPageSize={true}
			/>
		</div>

		<div class="card bg-base-200/60 border border-base-300/60 shadow-lg mt-4">
			<div class="card-body gap-4">
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

								<td>
									<div class="font-medium">{r.hostDisplayName ?? r.host}</div>
									{#if r.hostDisplayName}
										<div class="text-xs opacity-50">{r.host}</div>
									{/if}
								</td>
								<td class="opacity-80">{r.ip}</td>

								<td>
									{#if r.status === "Offline"}
										<span class="opacity-60">—</span>
									{:else if r.cpu == null}
										<span class="opacity-60">N/A</span>
									{:else}
										<div class="flex items-center gap-3">
											<progress class="progress progress-success w-28" value={r.cpu} max="100" />
											<span class="tabular-nums">{r.cpu}%</span>
										</div>
									{/if}
								</td>

								<td>
									{#if r.status === "Offline"}
										<span class="opacity-60">—</span>
									{:else if r.memPercent == null}
										<span class="opacity-60">N/A</span>
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
					<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-error"></span> Offline</div>
				</div>
			</div>
		</div>
	</div>
</div>