<script lang="ts">
	import { onDestroy, onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus } from "$lib/api/enums";
	import Pager from "$lib/components/Pager.svelte";
	import { createCopyFeedback } from "$lib/helper/clipboard-util";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";

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

	const urlParamDefs = {
		status: { defaultValue: "all" as "all" | BucketStatusLabel },
		search: { defaultValue: "" },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 12, ...Serializers.number }
	};

	const _initParams = readUrlParams(urlParamDefs);
	let statusFilter: "all" | BucketStatusLabel = _initParams.status;
	let search = _initParams.search;

	let allBuckets: BucketRow[] = [];


	let kpis = {
		total: 0,
		active: 0,
		lost: 0,
		draining: 0
	};

	let pageSize = _initParams.size;
	let pageIndex = _initParams.page;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			status: statusFilter,
			search,
			page: pageIndex,
			size: pageSize
		});
	}

	$: statusFilter, search, pageIndex, pageSize, syncToUrl();
	let poller: number | undefined;

	const copyFeedback = createCopyFeedback({ resetAfterMs: 1200 });
	const copiedId = copyFeedback.copiedId;

	$: filtered = allBuckets
		.filter((r) => (statusFilter === "all" ? true : r.status === statusFilter))
		.filter((r) => r.name.toLowerCase().includes(search.trim().toLowerCase()));

	$: bucketsTotalCount = filtered.length;
	$: paginatedBuckets = filtered.slice(pageIndex * pageSize, pageIndex * pageSize + pageSize);

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
		if (!iso) return "—";
		return new Date(iso).toLocaleString();
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
		refreshNow();
		restartPoller();

		return () => {
			if (poller) window.clearInterval(poller);
		};
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
		copyFeedback.destroy();
	});
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-6xl px-6 py-6">
		<div class="flex flex-wrap items-start justify-between gap-4">
			<h1 class="text-3xl font-semibold tracking-tight">Buckets</h1>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last execution: {lastUpdatedAt.toLocaleString()}</span>
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
			<div class="flex flex-col gap-3">
				<select class="select select-sm select-bordered w-fit" bind:value={statusFilter}>
					<option value="all">Status: All</option>
					<option value="Active">Active</option>
					<option value="Completing">Completing</option>
					<option value="ReadyToDrain">Ready to Drain</option>
					<option value="Draining">Draining</option>
					<option value="Lost">Lost</option>
					<option value="ReadyToDelete">Ready to Delete</option>
				</select>

				<div class="flex items-center justify-between gap-3">
					<label class="input input-bordered input-sm flex items-center gap-2 w-full sm:w-80">
						<span class="opacity-60">🔎</span>
						<input class="grow" placeholder="Search buckets..." bind:value={search} />
					</label>

					<Pager
						bind:pageIndex
						bind:pageSize
						totalCount={bucketsTotalCount}
						currentCount={paginatedBuckets.length}
						disabled={isRefreshing}
						showPageSize={true}
					/>
				</div>
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