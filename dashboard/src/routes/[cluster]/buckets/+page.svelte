<script lang="ts">
	import { onDestroy, onMount } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus, JobStatus } from "$lib/api/enums";
	import Pager from "$lib/components/Pager.svelte";
	import AreaChart from "$lib/components/AreaChart.svelte";

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

	let statusFilter: "all" | BucketStatusLabel = "all";
	let search = "";

	let allBuckets: BucketRow[] = [];

	type MetricPoint = { time: number; value: number };
	const MAX_HISTORY_POINTS = 300;

	let jobsCompletedHistory: MetricPoint[] = [];
	let activeJobsHistory: MetricPoint[] = [];
	let avgRunSecHistory: MetricPoint[] = [];
	let avgQueueSecHistory: MetricPoint[] = [];

	let metrics = {
		jobsCompletedLast5m: null as number | null,
		activeJobs: null as number | null,
		avgRunSec: null as number | null,
		avgQueueSec: null as number | null
	};

	let kpis = {
		total: 0,
		active: 0,
		lost: 0,
		draining: 0
	};

	let pageSize = 12;
	let pageIndex = 0;
	let poller: number | undefined;

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

	function pushMetric(arr: MetricPoint[], value: number | null | undefined): MetricPoint[] {
		if (value == null || Number.isNaN(value)) return arr;
		const next = [...arr, { time: Date.now(), value }];
		return next.length > MAX_HISTORY_POINTS ? next.slice(next.length - MAX_HISTORY_POINTS) : next;
	}

	function safeMsBetween(startIso: string | null | undefined, endIso: string | null | undefined): number | null {
		if (!startIso || !endIso) return null;
		const s = Date.parse(startIso);
		const e = Date.parse(endIso);
		if (!Number.isFinite(s) || !Number.isFinite(e)) return null;
		const diff = e - s;
		return diff >= 0 ? diff : null;
	}

	function avg(nums: number[]): number | null {
		if (nums.length === 0) return null;
		return nums.reduce((a, b) => a + b, 0) / nums.length;
	}

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			if (!cid) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			try {
				const last5mFrom = new Date(Date.now() - 5 * 60 * 1000).toISOString();

				const [
					apiBuckets,
					totalCount,
					activeCount,
					drainingCount,
					lostCount,
					activeJobsCount,
					recentCompletedJobs
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
					jm.GET("/{clusterId}/jobs/count", {
						params: {
							path: { clusterId: cid },
							query: { Statuses: [JobStatus.Queued, JobStatus.Processing, JobStatus.AssignedToBucket] }
						}
					}).then((r) => {
						if (r.error) throw r.error;
						return r.data as number;
					}),
					jm.GET("/{clusterId}/jobs", {
						params: {
							path: { clusterId: cid },
							query: {
								Statuses: [JobStatus.Succeeded, JobStatus.Failed, JobStatus.Cancelled],
								ScheduledFrom: last5mFrom,
								CountLimit: 200,
								OrderByProperty: "succeedExecutedAt",
								OrderByAsc: false
							}
						}
					}).then((r) => {
						if (r.error) throw r.error;
						return (r.data ?? []) as components["schemas"]["ApiJobModel"][];
					})
				]);

				const now = Date.now();
				const cutoff = now - 5 * 60 * 1000;

				const completedIn5m = recentCompletedJobs.filter((j) => {
					const ts = j.succeedExecutedAt ? Date.parse(j.succeedExecutedAt) : NaN;
					return Number.isFinite(ts) && ts >= cutoff;
				});

				const runDurationsMs = completedIn5m
					.map((j) => safeMsBetween(j.processingStartedAt, j.succeedExecutedAt))
					.filter((x): x is number => x != null);

				const queueDurationsMs = completedIn5m
					.map((j) => safeMsBetween(j.createdAt, j.processingStartedAt))
					.filter((x): x is number => x != null);

				metrics = {
					jobsCompletedLast5m: completedIn5m.length,
					activeJobs: activeJobsCount,
					avgRunSec: (() => {
						const v = avg(runDurationsMs);
						return v == null ? null : v / 1000;
					})(),
					avgQueueSec: (() => {
						const v = avg(queueDurationsMs);
						return v == null ? null : v / 1000;
					})()
				};

				jobsCompletedHistory = pushMetric(jobsCompletedHistory, metrics.jobsCompletedLast5m);
				activeJobsHistory = pushMetric(activeJobsHistory, metrics.activeJobs);
				avgRunSecHistory = pushMetric(avgRunSecHistory, metrics.avgRunSec);
				avgQueueSecHistory = pushMetric(avgQueueSecHistory, metrics.avgQueueSec);

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
				metrics = { jobsCompletedLast5m: null, activeJobs: null, avgRunSec: null, avgQueueSec: null };
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
	});
</script>

<div class="min-h-screen bg-base-100">
	<main class="relative mx-auto max-w-6xl px-6 py-10">
		<!-- Header -->
		<div class="flex flex-wrap items-start justify-between gap-4">
			<div>
				<h1 class="text-3xl font-semibold text-base-content">Buckets</h1>
				<p class="mt-1 text-sm text-base-content/60">Cluster: QA - Testing • Admin • Active • Connected</p>
			</div>

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

		<!-- KPI cards -->
		<section class="mt-8 grid gap-4 md:grid-cols-4">
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

					<!-- subtle sparkline -->
					<div class="mt-4 h-8 w-full rounded-lg bg-base-300/40 overflow-hidden">
						<svg viewBox="0 0 120 24" class="h-full w-full">
							<path
								d="M0,16 C12,16 16,10 28,10 C40,10 44,18 56,18 C68,18 72,8 84,8 C96,8 100,14 120,14"
								fill="none"
								stroke="currentColor"
								stroke-width="2"
								class="text-primary/70"
							/>
						</svg>
					</div>
				</div>
			</div>
		</section>

		<!-- Metrics -->
		<section class="mt-10">
			<div class="flex items-center justify-between gap-4">
				<h2 class="text-xl font-semibold text-base-content">Performance Metrics</h2>
			</div>

			<div class="mt-4 grid gap-4 lg:grid-cols-2">
				<!-- Chart 1 -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body p-5">
						<div class="flex items-start justify-between">
							<div>
								<div class="text-sm text-base-content/70">
									Bucket Jobs Count <span class="opacity-60">(Past 5 Min)</span>
								</div>
							</div>
							<button class="btn btn-ghost btn-sm opacity-70">⋯</button>
						</div>

						<div class="relative mt-4 h-40 w-full rounded-xl bg-base-300/30 overflow-hidden">
							<div class="absolute right-3 top-3 rounded-lg bg-base-100/60 px-2.5 py-1 text-xs font-semibold text-base-content/80 backdrop-blur">
								{metrics.jobsCompletedLast5m != null ? metrics.jobsCompletedLast5m.toLocaleString() : "—"}
							</div>
							<div class="absolute right-3 top-11 rounded-lg bg-base-100/50 px-2.5 py-1 text-[11px] font-medium text-base-content/70 backdrop-blur">
								Active: {metrics.activeJobs != null ? metrics.activeJobs.toLocaleString() : "—"}
							</div>
							<div class="h-full w-full p-2">
								<AreaChart
									data={jobsCompletedHistory}
									maxValue={Math.max(10, ...(jobsCompletedHistory.map((p) => p.value) ?? [10]))}
									color="oklch(var(--p))"
									unit=""
									label="Jobs completed"
								/>
							</div>
						</div>

						<div class="mt-3 flex items-center gap-4 text-sm text-base-content/60">
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-primary/70"></span>
								Jobs Completed
							</div>
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-secondary/70"></span>
								Active Jobs ({metrics.activeJobs != null ? metrics.activeJobs.toLocaleString() : "—"})
							</div>
						</div>
					</div>
				</div>

				<!-- Chart 2 -->
				<div class="card bg-base-200/60 border border-base-300/60 shadow-lg">
					<div class="card-body p-5">
						<div class="flex items-start justify-between">
							<div>
								<div class="text-sm text-base-content/70">
									Bucket Performance <span class="opacity-60">(Past 5 Min)</span>
								</div>
							</div>
							<button class="btn btn-ghost btn-sm opacity-70">⋯</button>
						</div>

						<div class="relative mt-4 h-40 w-full rounded-xl bg-base-300/30 overflow-hidden">
							<div class="absolute right-3 top-3 rounded-lg bg-base-100/60 px-2.5 py-1 text-xs font-semibold text-base-content/80 backdrop-blur">
								{metrics.avgRunSec != null ? `${metrics.avgRunSec.toFixed(2)}s` : "—"}
							</div>
							<div class="absolute right-3 top-11 rounded-lg bg-base-100/50 px-2.5 py-1 text-[11px] font-medium text-base-content/70 backdrop-blur">
								Queue: {metrics.avgQueueSec != null ? `${metrics.avgQueueSec.toFixed(2)}s` : "—"}
							</div>
							<div class="h-full w-full p-2">
								<AreaChart
									data={avgRunSecHistory}
									maxValue={Math.max(2, ...(avgRunSecHistory.map((p) => p.value) ?? [2]))}
									color="oklch(var(--su))"
									unit="s"
									label="Avg run"
								/>
							</div>
						</div>

						<div class="mt-3 flex items-center gap-4 text-sm text-base-content/60">
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-secondary/70"></span>
								Avg Run Duration
							</div>
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-primary/60"></span>
								Avg Queue Duration {metrics.avgQueueSec != null ? `(${metrics.avgQueueSec.toFixed(2)}s)` : ""}
							</div>
						</div>
					</div>
				</div>
			</div>
		</section>

		<!-- Table -->
		<section class="mt-10">
			<div class="flex justify-end">
				<Pager
					bind:pageIndex
					bind:pageSize
					totalCount={bucketsTotalCount}
					currentCount={paginatedBuckets.length}
					disabled={isRefreshing}
					showPageSize={true}
				/>
			</div>

			<div class="mt-3 flex flex-wrap items-center justify-between gap-3">
				<h2 class="text-xl font-semibold text-base-content">Buckets Table</h2>

				<div class="flex flex-nowrap items-center gap-2">
					<select class="select select-sm select-bordered bg-base-200/60 w-56 shrink-0" bind:value={statusFilter}>
						<option value="all">Status: All</option>
						<option value="Active">Active</option>
						<option value="Completing">Completing</option>
						<option value="ReadyToDrain">Ready to Drain</option>
						<option value="Draining">Draining</option>
						<option value="Lost">Lost</option>
						<option value="ReadyToDelete">Ready to Delete</option>
					</select>

					<label class="input input-sm input-bordered flex items-center gap-2 bg-base-200/60 w-72 flex-1">
						<span class="opacity-60 text-base leading-none">🔎</span>
						<input class="grow" placeholder="Search buckets..." bind:value={search} />
					</label>
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
							<tr class="hover">
								<td>
									<div class="flex items-center gap-3">
										<span class={`h-2.5 w-2.5 rounded-full ${dotFor(r.status)}`}></span>
										<div class="font-medium">{r.name}</div>
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
	</main>
</div>