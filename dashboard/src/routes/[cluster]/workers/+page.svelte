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
	import { WorkerModeUtil, type WorkerMode } from "$lib/helper/worker-mode-util";
	import { parseDatetimeParam, datetimeToParam, passesDatetimeFilter, type DatetimeFilterValue } from "$lib/helper/datetime-filter-url";

	type ApiHostModel = components["schemas"]["ApiHostModel"];

	type WorkerStatus = "Online" | "Offline";

	type WorkerRow = {
		id: string;
		name: string;
		status: WorkerStatus;
		mode: WorkerMode;
		lane: string;
		hostName?: string;
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

	$: onlineCount = rows.filter((r) => r.status === "Online").length;
	$: offlineCount = rows.filter((r) => r.status === "Offline").length;


	const urlParamDefs = {
		statuses: { defaultValue: [] as string[], ...Serializers.stringArray },
		modes: { defaultValue: [] as string[], ...Serializers.stringArray },
		sortBy: { defaultValue: "Host" as "Host" },
		asc: { defaultValue: true, ...Serializers.boolean },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number },
		lastHeartbeat: { defaultValue: "" as string }
	};

	let _initParams = readUrlParams(urlParamDefs);
	let sortBy: "Host" = _initParams.sortBy;
	let asc = _initParams.asc;

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
		sortBy = _initParams.sortBy;
		asc = _initParams.asc;
		selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
		selectedModes = _initParams.modes.length > 0 ? [..._initParams.modes] : [];
		filterValues = parseDatetimeParam(_initParams.lastHeartbeat, "lastHeartbeat");
		refreshNow();
	}

	let selectedStatuses: string[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
	let selectedModes: string[] = _initParams.modes.length > 0 ? [..._initParams.modes] : [];

	type FilterValues = Record<string, unknown>;
	let filterValues: FilterValues = parseDatetimeParam(_initParams.lastHeartbeat, "lastHeartbeat");

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			statuses: selectedStatuses,
			modes: selectedModes,
			sortBy,
			asc,
			page: pageIndex,
			size: pageSize,
			lastHeartbeat: datetimeToParam(filterValues, "lastHeartbeat")
		});
	}

	$: filterValues, selectedStatuses, selectedModes, sortBy, asc, pageIndex, pageSize, syncToUrl();

	function mapWorkerToRow(w: any, hostsMap: Map<string, ApiHostModel>): WorkerRow {
		const isAlive = w.isAlive === true;
		const host = w.hostId ? hostsMap.get(w.hostId) : undefined;

		const status: WorkerStatus = isAlive ? "Online" : "Offline";

		return {
			id: w.id ?? "",
			name: w.displayName ?? w.name ?? w.id ?? "Unknown",
			status,
			mode: WorkerModeUtil.getLabel(w.mode),
			lane: w.workerLane ?? "—",
			hostName: w.hostDisplayName ?? host?.displayName ?? "N/A",
			parallelism: w.parallelismFactor ?? undefined,
			lastHeartbeat: w.lastHeartbeat ?? w.lastHeartbeatAt ?? undefined
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


	$: heartbeatFilter = (filterValues.lastHeartbeat ?? {}) as DatetimeFilterValue;

	$: filteredAll = rows
		.filter((r) => {
			if (selectedStatuses.length > 0 && !selectedStatuses.includes(r.status)) return false;
			if (selectedModes.length > 0 && !selectedModes.includes(r.mode)) return false;
			if (!passesDatetimeFilter(heartbeatFilter, r.lastHeartbeat)) return false;
			return true;
		})
		.sort((a, b) => {
			const dir = asc ? 1 : -1;
			const cmpStr = (x: string, y: string) => x.localeCompare(y) * dir;
			return cmpStr(a.name, b.name);
		});

	$: totalCount = filteredAll.length;
	$: filtered = filteredAll.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = filtered.length;


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
	<div class="mx-auto max-w-full px-6 py-6">
		<div class="flex items-start justify-between gap-4">
			<h1 class="text-3xl font-semibold tracking-tight">Workers</h1>

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

		<section class="mt-6 grid grid-cols-1 gap-4 md:grid-cols-2">
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

		</section>

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

				<FilterDropdownMulti
					label="Mode"
					options={[
						{ value: "Execution", label: "Execution" },
						{ value: "Full", label: "Full" },
						{ value: "Draining", label: "Draining" }
					]}
					bind:values={selectedModes}
					on:change={() => { pageIndex = 0; }}
				/>

				<FilterContainer
					initialValues={filterValues}
					onChange={(v) => {
						filterValues = v;
						pageIndex = 0;
					}}
				>
					<FilterItem
						id="lastHeartbeat"
						label="Last Heartbeat"
						type="datetime"
						presets={[
							{ type: "LAST_MINUTES", minutes: 5, label: "Last 5 min" },
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

		<section class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body gap-4">
				<div class="flex items-center justify-end gap-4">
					<div class="join">
						<button
							class="btn btn-bordered join-item"
							on:click={() => (asc = !asc)}
							title="Toggle sort direction"
						>
							Sort: Host {asc ? "A→Z" : "Z→A"}
						</button>
					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/70">
							<th>Status</th>
							<th>Mode</th>
							<th>Worker</th>
							<th>Host</th>
							<th>Lane</th>
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

								<td>
									<span class={`badge badge-sm ${WorkerModeUtil.getBadgeClass(r.mode)}`}>{r.mode}</span>
								</td>

								<td class="text-base-content font-medium">{r.name}</td>

								<td class="text-base-content/70">{r.hostName ?? "N/A"}</td>

								<td class="text-base-content/70">{r.lane}</td>

								<td class="text-base-content/70">{r.lastHeartbeat ?? "—"}</td>
							</tr>
						{/each}

						{#if filtered.length === 0}
							<tr>
								<td colspan="6" class="py-10 text-base-content/60">No workers found.</td>
							</tr>
						{/if}
						</tbody>
					</table>
				</div>
			</div>
		</section>
	</div>
</div>