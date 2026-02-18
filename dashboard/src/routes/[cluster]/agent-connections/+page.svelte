<script lang="ts">
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import Pager from "$lib/components/Pager.svelte";

	type Health = "OK" | "Warning" | "Error";

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
		draining?: number; // ex: "1 draining"
		selected?: boolean;
	};

	export let data: { agentConnections: any[]; error: string | null };

	const clusterId = () => $page.params.cluster;

	const allClusters = ["All", clusterId()];

	let clusterFilter = "All";
	let query = "";
	let pageIndex = 0;
	let pageSize = 10;

	type SortCol = "name" | "cluster" | "health" | "workers" | "buckets";
	let sortBy: SortCol = "name";
	let sortAsc = true;

	function toggleSort(col: SortCol) {
		if (sortBy === col) {
			sortAsc = !sortAsc;
		} else {
			sortBy = col;
			sortAsc = true;
		}
		pageIndex = 0;
	}

	const sortIcon = (col: SortCol) => {
		if (sortBy !== col) return "⇅";
		return sortAsc ? "↑" : "↓";
	};

	let rows: AgentConnRow[] = [];

	function mapHealth(x: any): Health {
		const v = String(x ?? "").toLowerCase();
		if (v === "ok" || v === "healthy") return "OK";
		if (v === "warning" || v === "warn") return "Warning";
		if (v === "error" || v === "err" || v === "failed") return "Error";
		return "OK";
	}

	function mapConnToRow(c: any): AgentConnRow {
		const id = String(c?.id ?? c?.agentConnectionId ?? c?.name ?? "");
		const name = String(c?.displayName ?? c?.name ?? id ?? "Unknown");
		const health = mapHealth(c?.health ?? c?.status ?? c?.state);

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
			selected: false
		};
	}

	$: rows = (data?.agentConnections ?? []).map(mapConnToRow);

	const healthBadge = (h: Health) => {
		if (h === "OK") return "badge badge-success gap-2";
		if (h === "Warning") return "badge badge-warning gap-2";
		return "badge badge-error gap-2";
	};

	const healthIcon = (h: Health) => {
		if (h === "OK") return "✅";
		if (h === "Warning") return "⚠️";
		return "⛔";
	};

	const filtered = () => {
		const q = query.trim().toLowerCase();
		return rows.filter((r) => {
			const byCluster = clusterFilter === "All" || r.cluster === clusterFilter;
			const byQuery =
				!q ||
				r.name.toLowerCase().includes(q) ||
				r.sub.toLowerCase().includes(q) ||
				r.cluster.toLowerCase().includes(q) ||
				r.clusterSub.toLowerCase().includes(q);
			return byCluster && byQuery;
		});
	};

	const healthOrder: Record<Health, number> = { "OK": 0, "Warning": 1, "Error": 2 };

	$: sorted = filtered().sort((a, b) => {
		const dir = sortAsc ? 1 : -1;
		switch (sortBy) {
			case "name":
				return a.name.localeCompare(b.name) * dir;
			case "cluster":
				return a.cluster.localeCompare(b.cluster) * dir;
			case "health":
				return (healthOrder[a.health] - healthOrder[b.health]) * dir;
			case "workers":
				return (a.workers - b.workers) * dir;
			case "buckets":
				return (a.bucketsUsed - b.bucketsUsed) * dir;
			default:
				return 0;
		}
	});

	$: list = sorted;
	$: totalCount = list.length;
	$: view = list.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);
	$: currentCount = view.length;

</script>

<div class="min-h-screen bg-base-300 relative overflow-hidden">
	<div class="relative max-w-6xl mx-auto px-8 py-10">
		<!-- Header -->
		<div class="flex items-start justify-between gap-6">
			<div>
				<h1 class="text-4xl font-semibold tracking-tight">Agent Connections</h1>
				<p class="text-base-content/60 mt-2">Select an agent connection to view details.</p>
			</div>

		</div>

		<!-- Filters -->
		<div class="mt-8 flex items-center justify-end gap-3">
			{#if data?.error}
				<div class="alert alert-error text-sm">
					<span>{data.error}</span>
				</div>
			{/if}

			<div class="join">
				<button class="btn btn-sm btn-ghost join-item pointer-events-none text-base-content/70">
					Cluster: {clusterFilter}
				</button>
				<select
					class="select select-sm select-bordered join-item"
					bind:value={clusterFilter}
					aria-label="Cluster filter"
				>
					{#each allClusters as c}
						<option value={c}>{c}</option>
					{/each}
				</select>
			</div>

			<label class="input input-bordered input-sm flex items-center gap-2 w-[320px]">
				<span class="opacity-60">🔍</span>
				<input
					type="text"
					class="grow"
					placeholder="Search..."
					bind:value={query}
				/>
			</label>
		</div>

		<!-- Table Card -->
		<div class="mt-6 rounded-2xl bg-base-200/60 backdrop-blur border border-base-content/10 shadow-xl">
			<div class="flex justify-end px-5 pt-4">
				<Pager
					bind:pageIndex
					bind:pageSize
					{totalCount}
					{currentCount}
					showPageSize={true}
				/>
			</div>
			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-base-content/70">
						<th class="cursor-pointer select-none" on:click={() => toggleSort("name")}>
							<div class="flex items-center gap-2">
								<span>Agent Connection</span>
								<span class:opacity-40={sortBy !== "name"}>{sortIcon("name")}</span>
							</div>
						</th>
						<th class="cursor-pointer select-none" on:click={() => toggleSort("cluster")}>
							<div class="flex items-center gap-2">
								<span>Cluster</span>
								<span class:opacity-40={sortBy !== "cluster"}>{sortIcon("cluster")}</span>
							</div>
						</th>
						<th class="cursor-pointer select-none" on:click={() => toggleSort("health")}>
							<div class="flex items-center gap-2">
								<span>Health</span>
								<span class:opacity-40={sortBy !== "health"}>{sortIcon("health")}</span>
							</div>
						</th>
						<th class="cursor-pointer select-none text-right" on:click={() => toggleSort("workers")}>
							<div class="flex items-center justify-end gap-2">
								<span># Workers</span>
								<span class:opacity-40={sortBy !== "workers"}>{sortIcon("workers")}</span>
							</div>
						</th>
						<th class="cursor-pointer select-none" on:click={() => toggleSort("buckets")}>
							<div class="flex items-center gap-2">
								<span>Buckets</span>
								<span class:opacity-40={sortBy !== "buckets"}>{sortIcon("buckets")}</span>
							</div>
						</th>
					</tr>
					</thead>

					<tbody>
					{#each view as r (r.id)}
						<tr
							class="cursor-pointer transition hover:bg-base-100/40"
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
					</tbody>
				</table>
			</div>

		</div>
	</div>
</div>