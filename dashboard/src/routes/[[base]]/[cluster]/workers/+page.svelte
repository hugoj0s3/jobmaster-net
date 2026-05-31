<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";
	import { WorkerMode, workerModeLabel, workerModeBadgeClass } from "$lib/api/enums";
	import { readSavedFilter, writeSavedFilter } from "$lib/helper/filter-persistence";

	type WorkerStatus = "Online" | "Offline";

	type WorkerRow = {
		id: string;
		name: string;
		status: WorkerStatus;
		mode: number | undefined;
		lane: string;
		hostName?: string;
		lastHeartbeat?: string;
	};

	const clusterId = () => $page.params.cluster;

	let rows: WorkerRow[] = [];
	let isRefreshing = false;
	let activeFiltersCount = 0;

	const DEFAULT_STATUSES = ["Online"];
	const DEFAULT_SORT_DIRECTION: "asc" | "desc" = "desc";

	$: onlineCount = rows.filter((r) => r.status === "Online").length;
	$: offlineCount = rows.filter((r) => r.status === "Offline").length;

	const urlParamDefs = {
		statuses: { defaultValue: [] as string[], ...Serializers.stringArray },
		modes: { defaultValue: [] as string[], ...Serializers.stringArray },
		sortDirection: { defaultValue: "" as "" | "asc" | "desc" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	const LS_KEY = `workers-filters-${$page.params.cluster}`;

	function loadSavedFilters() {
		const saved = readSavedFilter(LS_KEY, "");
		if (!saved) return null;
		try { return JSON.parse(saved); } catch { return null; }
	}

	let _initParams = readUrlParams(urlParamDefs);
	let sortDirection: "" | "asc" | "desc" = _initParams.sortDirection;
	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;
	let selectedStatuses: string[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
	let selectedModes: string[] = _initParams.modes.length > 0 ? [..._initParams.modes] : [];

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		sortDirection = _initParams.sortDirection;
		selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
		selectedModes = _initParams.modes.length > 0 ? [..._initParams.modes] : [];
		refreshNow();
	}

	function syncToUrl() {
		writeUrlParams(urlParamDefs, { statuses: selectedStatuses, modes: selectedModes, sortDirection, page: pageIndex, size: pageSize });
	}

	$: selectedStatuses, selectedModes, sortDirection, pageIndex, pageSize, syncToUrl();

	function resetFilters() {
		selectedStatuses = [];
		selectedModes = [];
		sortDirection = "";
		pageIndex = 0;
	}

	function mapWorkerToRow(w: any): WorkerRow {
		return {
			id: w.id ?? "",
			name: w.name ?? w.id ?? "Unknown",
			status: w.isAlive === true ? "Online" : "Offline",
			mode: w.mode ?? undefined,
			lane: w.workerLane ?? "—",
			hostName: w.hostDisplayName || "—",
			lastHeartbeat: w.lastHeartbeat ?? w.lastHeartbeatAt ?? undefined
		};
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;
			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);
			const response = await jmApi.GET("/{clusterId}/workers", {
				params: { path: { clusterId: cid } }
			});
			if (response.error) { console.error("API error (workers):", response.error); return; }
			rows = ((response.data ?? []) as any[]).map(mapWorkerToRow);
		} catch (error) {
			console.error("Failed to fetch workers:", error);
		} finally {
			isRefreshing = false;
		}
	}

	const statusDot = (s: WorkerStatus) => s === "Online" ? "bg-success" : "bg-error";
	const statusBadgeClass = (s: WorkerStatus) => s === "Online" ? "badge-success" : "badge-error";

	$: filteredAll = rows
		.filter((r) => {
			if (selectedStatuses.length > 0 && !selectedStatuses.includes(r.status)) return false;
			if (selectedModes.length > 0 && !selectedModes.includes(String(r.mode ?? ""))) return false;
			return true;
		})
		.sort((a, b) => {
			const dir = sortDirection === "asc" ? 1 : -1;
			const t = (iso: string | undefined) => {
				if (!iso) return Number.MIN_SAFE_INTEGER;
				const v = new Date(iso).getTime();
				return Number.isFinite(v) ? v : Number.MIN_SAFE_INTEGER;
			};
			return (t(a.lastHeartbeat) - t(b.lastHeartbeat)) * dir;
		});

	$: totalCount = filteredAll.length;
	$: filtered = filteredAll.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = filtered.length;
	$: activeFiltersCount = (selectedStatuses.length > 0 ? 1 : 0) + (selectedModes.length > 0 ? 1 : 0);

	onMount(() => {
		const rawSearch = window.location.search;
		const hasUserUrlParams = rawSearch.length > 0 && new URLSearchParams(rawSearch).toString().length > 0;
		if (!hasUserUrlParams) {
			const saved = loadSavedFilters();
			if (saved) {
				selectedStatuses = saved.statuses !== undefined ? saved.statuses : [...DEFAULT_STATUSES];
				selectedModes = saved.modes !== undefined ? saved.modes : [];
				sortDirection = saved.sortDirection !== undefined ? saved.sortDirection : DEFAULT_SORT_DIRECTION;
				pageSize = saved.pageSize !== undefined ? saved.pageSize : 10;
			} else {
				selectedStatuses = [...DEFAULT_STATUSES];
				sortDirection = DEFAULT_SORT_DIRECTION;
			}
			syncToUrl();
		}
		refreshNow();
	});

	onDestroy(() => {
		writeSavedFilter(LS_KEY, JSON.stringify({ statuses: selectedStatuses, modes: selectedModes, sortDirection, pageSize }));
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<h1 class="text-2xl font-semibold tracking-tight">Workers</h1>

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
						{ value: String(WorkerMode.Full),        label: "Full" },
						{ value: String(WorkerMode.Execution),   label: "Execution" },
						{ value: String(WorkerMode.Drain),       label: "Drain" },
						{ value: String(WorkerMode.Coordinator), label: "Coordinator" }
					]}
					bind:values={selectedModes}
					on:change={() => { pageIndex = 0; }}
				/>

				{#if activeFiltersCount > 0}
					<button
						type="button"
						class="btn btn-sm bg-red-200 text-red-900 border border-red-300 hover:bg-red-300"
						on:click={resetFilters}
					>
						Clear filters
					</button>
				{/if}
			</div>
			{/key}

			<div class="flex items-center gap-2">
				<FilterDropdown
					label="Sort By"
					options={[
						{ value: "desc", label: "Recents" },
						{ value: "asc", label: "Olders" }
					]}
					value={sortDirection}
					on:change={(e) => {
						sortDirection = (e.detail as "asc" | "desc") ?? "desc";
						pageIndex = 0;
					}}
				/>
				<Pager
					bind:pageIndex
					bind:pageSize
					{totalCount}
					{currentCount}
					disabled={isRefreshing}
					showPageSize={true}
				/>
				<button class="btn btn-xs" on:click={refreshNow} disabled={isRefreshing}>Refresh</button>
			</div>
		</div>

		<div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-base-content/70">
						<th>Worker</th>
						<th>Status</th>
						<th>Mode</th>
						<th>Host</th>
						<th>Lane</th>
						<th>Last Heartbeat</th>
					</tr>
					</thead>
					<tbody>
					{#each filtered as r (r.id)}
						<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${r.id}`)}>
							<td class="font-medium">{r.name}</td>

							<td>
								<div class="flex items-center gap-2">
									<span class={"h-2 w-2 rounded-full " + statusDot(r.status)} />
									<span class={"badge badge-sm badge-outline whitespace-nowrap " + statusBadgeClass(r.status)}>{r.status}</span>
								</div>
							</td>

							<td>
								<span class={"badge badge-sm whitespace-nowrap " + workerModeBadgeClass(r.mode)}>{workerModeLabel(r.mode)}</span>
							</td>

							<td class="opacity-70">{r.hostName}</td>

							<td class="opacity-70">{r.lane}</td>

							<td class="opacity-70 whitespace-nowrap">{DateDisplayUtil.formatRelativeOrDate(r.lastHeartbeat)}</td>
						</tr>
					{/each}

					{#if filtered.length === 0}
						<tr>
							<td colspan="6" class="py-10 text-center opacity-60">No workers found.</td>
						</tr>
					{/if}
					</tbody>
				</table>
			</div>
		</div>
	</div>
</div>
