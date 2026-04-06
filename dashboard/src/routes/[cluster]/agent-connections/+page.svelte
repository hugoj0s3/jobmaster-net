<script lang="ts">
	import { onDestroy, onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
	import FilterItem from "$lib/components/filters/FilterItem.svelte";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";
	import {
		parseDatetimeParam,
		datetimeToParam,
		passesDatetimeFilter,
		type DatetimeFilterValue
	} from "$lib/helper/datetime-filter-url";

	type Health = "OK" | "Offline";

	type AgentConnRow = {
		id: string;
		name: string;
		sub: string;
		cluster: string;
		clusterSub: string;
		health: Health;
		workers: number;
		bucketsUsed: number;
		bucketsTotal: number;
		draining?: number;
		selected?: boolean;
		createdAt?: string;
	};

	export let data: { agentConnections: any[]; error: string | null };

	const clusterId = () => $page.params.cluster;
	const refreshIntervalSec = 15;

	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let poller: number | undefined;

	const urlParamDefs = {
		sortDirection: { defaultValue: "desc" as "asc" | "desc" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number },
		createdAt: { defaultValue: "" as string }
	};

	let _initParams = readUrlParams(urlParamDefs);
	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;
	let sortDirection: "asc" | "desc" = _initParams.sortDirection;

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		sortDirection = _initParams.sortDirection;
		selectedHealths = [];
		filterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");
		refreshNow();
	}

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			sortDirection,
			page: pageIndex,
			size: pageSize,
			createdAt: datetimeToParam(filterValues, "createdAt")
		});
	}

	let selectedHealths: string[] = [];

	type FilterValues = Record<string, unknown>;
	let filterValues: FilterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");

	$: filterValues, sortDirection, pageIndex, pageSize, syncToUrl();

	function resetFilters() {
		selectedHealths = [];
		filterValues = {};
		sortDirection = "desc";
		pageIndex = 0;
		refreshNow();
	}

	let rows: AgentConnRow[] = [];

	function mapHealth(isAlive: any): Health {
		return isAlive === true ? "OK" : "Offline";
	}

	function mapConnToRow(c: any): AgentConnRow {
		const id = String(c?.id ?? c?.agentConnectionId ?? c?.name ?? "");
		const name = String(c?.displayName ?? c?.name ?? id ?? "Unknown");
		const health = mapHealth(c?.isAlive);

		const bucketsUsed = Number(c?.bucketsUsed ?? c?.bucketUsed ?? c?.bucketCountUsed ?? 0);
		const bucketsTotal = Number(c?.bucketsTotal ?? c?.bucketTotal ?? c?.bucketCountTotal ?? 0);
		const draining = c?.draining != null ? Number(c.draining) : undefined;

		return {
			id,
			name,
			sub: String(c?.sub ?? c?.agentType ?? c?.type ?? "—"),
			cluster: String(c?.cluster ?? clusterId()),
			clusterSub: String(c?.clusterSub ?? c?.clusterType ?? "—"),
			health,
			workers: Number(c?.workers ?? c?.workersBound ?? c?.workerCount ?? 0),
			bucketsUsed,
			bucketsTotal: bucketsTotal || Math.max(bucketsUsed, 1),
			draining,
			selected: false,
			createdAt: c?.createdAt ?? c?.connectedAt ?? c?.registeredAt ?? undefined
		};
	}

	$: rows = (data?.agentConnections ?? []).map(mapConnToRow);

	const healthBadge = (h: Health) => {
		if (h === "OK") return "badge badge-success gap-2";
		return "badge badge-error gap-2";
	};

	const healthIcon = (h: Health) => {
		if (h === "OK") return "✅";
		return "⛔";
	};

	$: createdAtFilter = (filterValues.createdAt ?? {}) as DatetimeFilterValue;

	$: filteredRows = rows.filter((r) => {
		if (selectedHealths.length > 0 && !selectedHealths.includes(r.health)) return false;
		if (!passesDatetimeFilter(createdAtFilter, r.createdAt)) return false;
		return true;
	});

	$: sorted = filteredRows.slice().sort((a, b) => {
		const dir = sortDirection === "asc" ? 1 : -1;
		const safeTime = (iso: string | undefined) => {
			if (!iso) return Number.MIN_SAFE_INTEGER;
			const t = new Date(iso).getTime();
			return Number.isFinite(t) ? t : Number.MIN_SAFE_INTEGER;
		};
		return (safeTime(a.createdAt) - safeTime(b.createdAt)) * dir;
	});

	$: list = sorted;
	$: totalCount = list.length;
	$: view = list.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = view.length;
	$: activeFiltersCount =
		(selectedHealths.length > 0 ? 1 : 0) +
		(filterValues?.createdAt ? 1 : 0) +
		(sortDirection !== "desc" ? 1 : 0);

	async function refreshNow() {
		isRefreshing = true;
		try {
			// The data is already loaded via the page data function
			// We just update the timestamp and trigger reactivity
			lastUpdatedAt = new Date();
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
			<h1 class="text-3xl font-semibold tracking-tight">Agent Connections</h1>
			
			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last Refresh: {DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt)}</span>
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

		{#if data?.error}
			<div class="alert alert-error text-sm mt-4">
				<span>{data.error}</span>
			</div>
		{/if}

		<div class="flex items-center justify-between gap-4 mt-6">
			{#key filterKey}
				<div class="flex flex-wrap items-center gap-2">
					<FilterDropdownMulti
						label="Health"
						options={[
							{ value: "Ok", label: "Ok" },
							{ value: "Offline", label: "Offline" }
						]}
						bind:values={selectedHealths}
						on:change={() => {
							pageIndex = 0;
							refreshNow();
						}}
					/>

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
							refreshNow();
						}}
					/>

					<FilterContainer
						initialValues={filterValues}
						onChange={(v) => {
							filterValues = v;
							pageIndex = 0;
							refreshNow();
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

					{#if activeFiltersCount > 0}
						<button
							type="button"
							class="btn btn-sm btn-ghost"
							on:click={resetFilters}
						>
							Reset filters
						</button>
					{/if}
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

		<div class="mt-2 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-base-content/70">
						<th>Agent Connection</th>
						<th>Cluster</th>
						<th>Health</th>
						<th class="text-right"># Workers</th>
						<th>Buckets</th>
					</tr>
					</thead>

					<tbody>
					{#each view as r (r.id)}
						<tr
							class="hover cursor-pointer transition"
							on:click={() => goto(`/${clusterId()}/agent-connections/${r.id}`)}
						>
							<td>
								<div class="flex flex-col">
									<div class="font-semibold text-lg leading-tight">{r.name}</div>
									<div class="text-sm text-base-content/60">{r.sub}</div>
								</div>
							</td>

							<td>
								<div class="flex flex-col">
									<div class="badge badge-ghost">{r.cluster}</div>
									<div class="text-sm text-base-content/60 mt-1">{r.clusterSub}</div>
								</div>
							</td>

							<td>
									<span class={healthBadge(r.health)}>
										<span aria-hidden="true">{healthIcon(r.health)}</span>
										{r.health}
									</span>
							</td>

							<td class="text-right font-semibold">{r.workers}</td>

							<td class="min-w-[240px]">
								<div class="flex items-center justify-between gap-3">
									<div class="font-semibold">
										{r.bucketsUsed} / {r.bucketsTotal}
									</div>
									{#if r.draining}
										<div class="text-sm text-base-content/60">{r.draining} draining</div>
									{/if}
								</div>

								<progress
									class="progress progress-success w-full mt-2"
									value={r.bucketsUsed}
									max={r.bucketsTotal}
								/>
							</td>
						</tr>
					{/each}

					{#if view.length === 0}
						<tr>
							<td colspan="5" class="py-10 text-center text-base-content/60">
								No agent connections match your filters.
							</td>
						</tr>
					{/if}
					</tbody>
				</table>
			</div>
		</div>
	</div>
</div>
