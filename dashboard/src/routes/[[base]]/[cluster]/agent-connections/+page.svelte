<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";
	import { readSavedFilter, writeSavedFilter } from "$lib/helper/filter-persistence";

	type ApiAgentConnection = components["schemas"]["ApiAgentConnection"];

	const clusterId = () => $page.params.cluster;

	const urlParamDefs = {
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number },
		sortDirection: { defaultValue: "desc" as "asc" | "desc" }
	};

	let _initParams = readUrlParams(urlParamDefs);
	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;
	let sortDirection: "asc" | "desc" = _initParams.sortDirection;
	let selectedHealths: string[] = [];

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		sortDirection = _initParams.sortDirection;
		refreshNow();
	}

	function syncToUrl() {
		writeUrlParams(urlParamDefs, { page: pageIndex, size: pageSize, sortDirection });
	}

	$: sortDirection, pageIndex, pageSize, syncToUrl();

	function resetFilters() {
		selectedHealths = [];
		sortDirection = "desc";
		pageIndex = 0;
		syncToUrl();
	}

	let rows: ApiAgentConnection[] = [];
	let isRefreshing = false;
	let refreshError: string | null = null;

	$: filtered = rows.filter((r) => {
		if (selectedHealths.length === 0) return true;
		const health = r.isAlive === true ? "OK" : "Offline";
		return selectedHealths.includes(health);
	});

	$: sorted = filtered.slice().sort((a, b) => {
		const dir = sortDirection === "asc" ? 1 : -1;
		const t = (iso: string | null | undefined) => {
			if (!iso) return Number.MIN_SAFE_INTEGER;
			const v = new Date(iso).getTime();
			return Number.isFinite(v) ? v : Number.MIN_SAFE_INTEGER;
		};
		return (t(a.createdAt) - t(b.createdAt)) * dir;
	});

	$: totalCount = sorted.length;
	$: view = sorted.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);

	$: activeFiltersCount = (selectedHealths.length > 0 ? 1 : 0) + (sortDirection !== "desc" ? 1 : 0);

	async function refreshNow() {
		isRefreshing = true;
		refreshError = null;
		try {
			const cid = clusterId();
			if (!cid) return;
			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);
			const response = await jm.GET("/{clusterId}/agent-connections", {
				params: { path: { clusterId: cid } }
			});
			if (response.error) {
				refreshError = "Failed to load agent connections.";
				return;
			}
			rows = (response.data ?? []) as ApiAgentConnection[];
		} catch (e) {
			refreshError = e instanceof Error ? e.message : String(e);
		} finally {
			isRefreshing = false;
		}
	}

	onMount(() => {
		const rawSearch = window.location.search;
		const hasUserUrlParams = rawSearch.length > 0 && new URLSearchParams(rawSearch).toString().length > 0;
		if (!hasUserUrlParams) {
			const saved = readSavedFilter(`agent-connections-filters-${clusterId()}`, "");
			if (saved) {
				try {
					const s = JSON.parse(saved);
					selectedHealths = s.selectedHealths ?? [];
					sortDirection = s.sortDirection ?? "desc";
					pageSize = s.pageSize ?? 10;
				} catch { /* ignore */ }
			}
			syncToUrl();
		}
		refreshNow();
	});

	onDestroy(() => {
		writeSavedFilter(
			`agent-connections-filters-${clusterId()}`,
			JSON.stringify({ selectedHealths, sortDirection, pageSize })
		);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<h1 class="text-3xl font-semibold tracking-tight">Agent Connections</h1>

		{#if refreshError}
			<div class="alert alert-error text-sm mt-4"><span>{refreshError}</span></div>
		{/if}

		<div class="mt-6 flex items-center justify-between gap-4">
			{#key filterKey}
			<div class="flex flex-wrap items-center gap-2">
				<FilterDropdownMulti
					label="Health"
					options={[
						{ value: "OK", label: "OK" },
						{ value: "Offline", label: "Offline" }
					]}
					bind:values={selectedHealths}
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
					currentCount={view.length}
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
						<th>Name</th>
						<th>Repository Type</th>
						<th>Health</th>
						<th>Last Heartbeat</th>
						<th>Created At</th>
					</tr>
					</thead>
					<tbody>
					{#each view as r (r.id)}
						<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/agent-connections/${r.id}`)}>
							<td class="font-medium">{r.name ?? r.id ?? "—"}</td>
							<td class="font-mono text-sm">{r.repositoryTypeId ?? "—"}</td>
							<td>
								<span class={"badge badge-sm whitespace-nowrap " + (r.isAlive ? "badge-success" : "badge-error")}>
									{r.isAlive ? "OK" : "Offline"}
								</span>
							</td>
							<td class="whitespace-nowrap">{DateDisplayUtil.formatRelativeOrDate(r.lastHeartbeat)}</td>
							<td class="whitespace-nowrap">{DateDisplayUtil.formatRelativeOrDate(r.createdAt)}</td>
						</tr>
					{/each}

					{#if view.length === 0}
						<tr>
							<td colspan="5" class="py-10 text-center opacity-60">
								{isRefreshing ? "Loading..." : "No agent connections found."}
							</td>
						</tr>
					{/if}
					</tbody>
				</table>
			</div>
		</div>
	</div>
</div>
