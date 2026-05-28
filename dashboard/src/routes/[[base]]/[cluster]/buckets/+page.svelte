<script lang="ts">
	import { onDestroy, onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus } from "$lib/api/enums";
	import Pager from "$lib/components/Pager.svelte";
	import FilterDropdown from "$lib/components/filters/FilterDropdown.svelte";
	import FilterDropdownMulti from "$lib/components/filters/FilterDropdownMulti.svelte";
	import FilterContainer from "$lib/components/filters/FilterContainer.svelte";
	import FilterItem from "$lib/components/filters/FilterItem.svelte";
	import { createCopyFeedback } from "$lib/helper/clipboard-util";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";
	import { parseDatetimeParam, datetimeToParam, passesDatetimeFilter, type DatetimeFilterValue } from "$lib/helper/datetime-filter-url";
	import { readSavedFilter, writeSavedFilter } from "$lib/helper/filter-persistence";

	const refreshIntervalSec = 20;
	const clusterId = () => $page.params.cluster;

	type ApiBucketModel = components["schemas"]["ApiBucketModel"];
	type BucketStatusLabel = "Active" | "Completing" | "ReadyToDrain" | "Draining" | "Lost" | "ReadyToDelete";

	function mapBucketStatus(status: number | undefined): BucketStatusLabel {
		switch (status) {
			case BucketStatus.Active: return "Active";
			case BucketStatus.Completing: return "Completing";
			case BucketStatus.ReadyToDrain: return "ReadyToDrain";
			case BucketStatus.Draining: return "Draining";
			case BucketStatus.Lost: return "Lost";
			case BucketStatus.ReadyToDelete: return "ReadyToDelete";
			default: return "Active";
		}
	}

	type BucketRow = {
		id: string;
		name: string;
		agentConnectionName: string;
		workerLane: string;
		hostDisplayName: string;
		status: BucketStatusLabel;
		createdAt: string;
	};

	let lastUpdatedAt = new Date();
	let isRefreshing = false;
	let activeFiltersCount = 0;

	const DEFAULT_STATUSES = ["Active", "Completing", "ReadyToDrain", "Draining", "Lost"];
	const DEFAULT_SORT_DIRECTION: "asc" | "desc" = "desc";

	const urlParamDefs = {
		statuses: { defaultValue: [] as string[], ...Serializers.stringArray },
		sortDirection: { defaultValue: "" as "" | "asc" | "desc" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number },
		createdAt: { defaultValue: "" as string }
	};

	const LS_KEY_BUCKETS_FILTERS = `buckets-filters-${$page.params.cluster}`;

	// Carrega filtros salvos da localStorage
	function loadSavedFilters() {
		const saved = readSavedFilter(LS_KEY_BUCKETS_FILTERS, "");
		if (!saved) return null;
		try {
			return JSON.parse(saved);
		} catch {
			return null;
		}
	}

	let _initParams = readUrlParams(urlParamDefs);

	let filterKey = $page.url.search;
	let lastSearch = $page.url.search;
	$: if ($page.url.search !== lastSearch) {
		lastSearch = $page.url.search;
		filterKey = $page.url.search;
		_initParams = readUrlParams(urlParamDefs);
		pageSize = _initParams.size;
		pageIndex = _initParams.page;
		selectedStatuses = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];
		sortDirection = _initParams.sortDirection;
		filterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");
		refreshNow();
	}

	let allBuckets: BucketRow[] = [];


	let kpis = {
		total: 0,
		active: 0,
		lost: 0,
		draining: 0
	};

	let pageSize = _initParams.size;
	let pageIndex = _initParams.page;
	let sortDirection: "" | "asc" | "desc" = _initParams.sortDirection;

	let selectedStatuses: string[] = _initParams.statuses.length > 0 ? [..._initParams.statuses] : [];

	type FilterValues = Record<string, unknown>;
	let filterValues: FilterValues = parseDatetimeParam(_initParams.createdAt, "createdAt");

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			statuses: selectedStatuses,
			sortDirection,
			page: pageIndex,
			size: pageSize,
			createdAt: datetimeToParam(filterValues, "createdAt")
		});
	}

	$: filterValues, selectedStatuses, sortDirection, pageIndex, pageSize, syncToUrl();

	function resetFilters() {
		selectedStatuses = [];
		filterValues = {};
		sortDirection = "";
		pageIndex = 0;
	}
	let poller: number | undefined;

	const copyFeedback = createCopyFeedback({ resetAfterMs: 1200 });
	const copiedId = copyFeedback.copiedId;

	$: createdAtFilter = (filterValues.createdAt ?? {}) as DatetimeFilterValue;

	$: filtered = allBuckets
		.filter((r) => {
			if (selectedStatuses.length > 0 && !selectedStatuses.includes(r.status)) return false;
			if (!passesDatetimeFilter(createdAtFilter, r.createdAt)) return false;
			return true;
		})
		.sort((a, b) => {
			const dir = sortDirection === "asc" ? 1 : -1;
			const safeTime = (iso: string | undefined) => {
				if (!iso) return Number.MIN_SAFE_INTEGER;
				const t = new Date(iso).getTime();
				return Number.isFinite(t) ? t : Number.MIN_SAFE_INTEGER;
			};
			return (safeTime(a.createdAt) - safeTime(b.createdAt)) * dir;
		});

	$: bucketsTotalCount = filtered.length;
	$: paginatedBuckets = filtered.slice(pageIndex * pageSize, pageIndex * pageSize + pageSize);
	$: activeFiltersCount =
		(selectedStatuses.length > 0 ? 1 : 0) +
		(filterValues?.createdAt ? 1 : 0) +
		(sortDirection ? 1 : 0);

	let lastPageIndexForRefresh = pageIndex;
	$: if (pageIndex !== lastPageIndexForRefresh) {
		lastPageIndexForRefresh = pageIndex;
	}

	let lastPageSizeForRefresh = pageSize;
	$: if (pageSize !== lastPageSizeForRefresh) {
		lastPageSizeForRefresh = pageSize;
		pageIndex = 0;
	}

	const badgeFor = (s: BucketStatusLabel) => {
		if (s === "Active") return "badge-success";
		if (s === "Lost") return "badge-error";
		if (s === "Draining" || s === "ReadyToDrain" || s === "Completing") return "badge-warning";
		if (s === "ReadyToDelete") return "badge-ghost";
		return "badge-ghost";
	};

	const dotFor = (s: BucketStatusLabel) => {
		if (s === "Active") return "bg-success";
		if (s === "Lost") return "bg-error";
		if (s === "Draining" || s === "ReadyToDrain" || s === "Completing") return "bg-warning";
		return "bg-base-content/30";
	};

	function formatDate(iso: string | undefined): string {
		return DateDisplayUtil.formatRelativeOrDate(iso);
	}

	function goToBucket(bucketId: string) {
		const cid = clusterId();
		if (!cid) return;
		goto(`/${encodeURIComponent(cid)}/buckets/${encodeURIComponent(bucketId)}`);
	}


	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			try {
				const [
					apiBuckets,
					totalCount,
					activeCount,
					drainingCount,
					lostCount
				] = await Promise.all([
					jm.GET("/{clusterId}/buckets", {
						params: { path: { clusterId: cid } }
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as ApiBucketModel[];
					}),
					jm.GET("/{clusterId}/buckets/count", {
						params: { path: { clusterId: cid } }
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as number;
					}),
					jm.GET("/{clusterId}/buckets/count", {
						params: { path: { clusterId: cid }, query: { Status: BucketStatus.Active } }
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as number;
					}),
					jm.GET("/{clusterId}/buckets/count", {
						params: { path: { clusterId: cid }, query: { Status: BucketStatus.Draining } }
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as number;
					}),
					jm.GET("/{clusterId}/buckets/count", {
						params: { path: { clusterId: cid }, query: { Status: BucketStatus.Lost } }
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as number;
					}),

				]);

				kpis = {
					total: totalCount,
					active: activeCount,
					lost: lostCount,
					draining: drainingCount
				};

				allBuckets = apiBuckets.map((b) => ({
					id: b.id ?? "",
					name: b.name ?? b.id ?? "—",
					agentConnectionName: b.agentConnectionName ?? "—",
					workerLane: b.workerLane ?? "—",
					hostDisplayName: b.hostDisplayName ?? "—",
					status: mapBucketStatus(b.status),
					createdAt: b.createdAt ?? ""
				}));

				lastUpdatedAt = new Date();
			} catch (e) {
				console.error("Buckets refresh failed", e);
				allBuckets = [];
				kpis = { total: 0, active: 0, lost: 0, draining: 0 };
			}
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

	onMount(() => {
		// Se não tem params na URL, tenta carregar da localStorage
		const hasUrlParams = $page.url.search.length > 0;
		if (!hasUrlParams) {
			const savedFilters = loadSavedFilters();
			if (savedFilters) {
				// Carrega filtros salvos (respeita até valores vazios)
				selectedStatuses = savedFilters.statuses !== undefined ? savedFilters.statuses : [...DEFAULT_STATUSES];
				sortDirection = savedFilters.sortDirection !== undefined ? savedFilters.sortDirection : DEFAULT_SORT_DIRECTION;
				filterValues = savedFilters.createdAt ? parseDatetimeParam(savedFilters.createdAt, "createdAt") : {};
			} else {
				// Aplica defaults quando não há localStorage (primeira vez)
				selectedStatuses = [...DEFAULT_STATUSES];
				sortDirection = DEFAULT_SORT_DIRECTION;
			}
			syncToUrl();
		}

		refreshNow();
		restartPoller();

		return () => {
			if (poller) window.clearInterval(poller);
		};
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
		copyFeedback.destroy();

		// Salva filtros atuais na localStorage no unmount
		writeSavedFilter(
			LS_KEY_BUCKETS_FILTERS,
			JSON.stringify({
				statuses: selectedStatuses,
				sortDirection,
				createdAt: datetimeToParam(filterValues, "createdAt")
			})
		);
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">
		<div class="flex flex-wrap items-start justify-between gap-4">
			<h1 class="text-3xl font-semibold tracking-tight">Buckets</h1>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last Refresh: {DateDisplayUtil.formatRelativeOrDate(lastUpdatedAt)}</span>
				<button
					class="btn btn-ghost btn-sm btn-square"
					aria-label="Refresh now"
					on:click={refreshNow}
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

		<section class="mt-6 grid gap-4 md:grid-cols-4">
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Total Buckets</div>
							<div class="mt-1 text-4xl font-semibold">{kpis.total}</div>
						</div>
						<div class="avatar placeholder">
							<div
								class="flex h-12 w-12 items-center justify-center rounded-xl bg-primary/20 text-primary"
							>
								<span class="text-lg leading-none">⛁</span>
							</div>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Active</div>
							<div class="mt-1 text-4xl font-semibold">{kpis.active}</div>
						</div>
						<div class="avatar placeholder">
							<div
								class="flex h-12 w-12 items-center justify-center rounded-xl bg-success/20 text-success"
							>
								<span class="text-lg leading-none">✓</span>
							</div>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Lost</div>
							<div class="mt-1 text-4xl font-semibold">{kpis.lost}</div>
						</div>
						<div class="avatar placeholder">
							<div
								class="flex h-12 w-12 items-center justify-center rounded-xl bg-error/20 text-error"
							>
								<span class="text-lg leading-none">✕</span>
							</div>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="card-body p-5">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-sm text-base-content/70">Draining</div>
							<div class="mt-1 text-4xl font-semibold">{kpis.draining}</div>
						</div>
						<div class="avatar placeholder">
							<div
								class="flex h-12 w-12 items-center justify-center rounded-xl bg-warning/20 text-warning"
							>
								<span class="text-lg leading-none">⟳</span>
							</div>
						</div>
					</div>


				</div>
			</div>
		</section>

		<!-- Table -->
		<section class="mt-10">
			<div class="flex items-center justify-between gap-4">
				{#key filterKey}
				<div class="flex flex-wrap items-center gap-2">
					<FilterDropdownMulti
						label="Status"
						options={[
							{ value: "Active", label: "Active" },
							{ value: "Completing", label: "Completing" },
							{ value: "ReadyToDrain", label: "Ready to Drain" },
							{ value: "Draining", label: "Draining" },
							{ value: "Lost", label: "Lost" },
							{ value: "ReadyToDelete", label: "Ready to Delete" }
						]}
						bind:values={selectedStatuses}
						on:change={() => { pageIndex = 0; }}
					/>

					<FilterDropdown
						label="Sort By"
						options={[
							{ value: "desc", label: "Recents" },
							{ value: "asc", label: "Olders" }
						]}
						value={sortDirection}
						on:change={(e) => {
							sortDirection = (e.detail as "" | "asc" | "desc") ?? "";
							pageIndex = 0;
						}}
					/>

					<FilterContainer
						initialValues={filterValues}
						onChange={(v) => {
							filterValues = v;
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

				<Pager
					bind:pageIndex
					bind:pageSize
					totalCount={bucketsTotalCount}
					currentCount={paginatedBuckets.length}
					disabled={isRefreshing}
					showPageSize={true}
				/>
			</div>

				<div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="overflow-x-auto">
					<table class="table table-zebra">
						<thead>
						<tr class="text-base-content/70">
							<th>Name</th>
							<th>Agent Connection</th>
							<th>Worker Lane</th>
							<th>Host</th>
							<th>Created At</th>
							<th class="text-right">Status</th>
						</tr>
						</thead>
						<tbody>
						{#each paginatedBuckets as r (r.id)}
							<tr class="hover cursor-pointer" on:click|stopPropagation={() => goToBucket(r.id)}>
								<td>
									<div class="flex items-center gap-3">
										<span class={`h-2.5 w-2.5 rounded-full ${dotFor(r.status)}`}></span>
										<div class="font-medium">{r.name}</div>
										<button
											class="btn btn-ghost btn-xs btn-square opacity-40 hover:opacity-100"
											aria-label="Copy bucket id"
											on:click|stopPropagation={() => copyFeedback.copy(r.id)}
										>
											{#if $copiedId === r.id}
												<svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5 text-success" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
													<path d="M20 6 9 17l-5-5"/>
												</svg>
											{:else}
												<svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
													<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
													<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>
												</svg>
											{/if}
										</button>
									</div>
								</td>
								<td>{r.agentConnectionName}</td>
								<td>{r.workerLane}</td>
								<td>{r.hostDisplayName}</td>
								<td>{formatDate(r.createdAt)}</td>
								<td class="text-right">
										<span class={`badge badge-sm ${badgeFor(r.status)}`}>
											{r.status}
										</span>
								</td>
							</tr>
						{/each}

						{#if paginatedBuckets.length === 0}
							<tr>
								<td colspan="6" class="py-10 text-center text-base-content/60">
									No buckets match your filters.
								</td>
							</tr>
						{/if}
						</tbody>
					</table>
				</div>
			</div>
		</section>
	</div>
</div>
