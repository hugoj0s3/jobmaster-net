<script lang="ts">
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import Pager from "$lib/components/Pager.svelte";
	import { readUrlParams, writeUrlParams, Serializers } from "$lib/helper/url-filters";

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

	type SortCol = "name" | "cluster" | "health" | "workers" | "buckets";

	const urlParamDefs = {
		sortBy: { defaultValue: "name" as SortCol },
		sortAsc: { defaultValue: true, ...Serializers.boolean },
		page: { defaultValue: 0, ...Serializers.number },
		size: { defaultValue: 10, ...Serializers.number }
	};

	const _initParams = readUrlParams(urlParamDefs);
	let pageIndex = _initParams.page;
	let pageSize = _initParams.size;
	let sortBy: SortCol = _initParams.sortBy;
	let sortAsc = _initParams.sortAsc;

	function syncToUrl() {
		writeUrlParams(urlParamDefs, {
			sortBy,
			sortAsc,
			page: pageIndex,
			size: pageSize
		});
	}

	$: sortBy, sortAsc, pageIndex, pageSize, syncToUrl();

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

	const healthOrder: Record<Health, number> = { "OK": 0, "Warning": 1, "Error": 2 };

	$: sorted = rows.slice().sort((a, b) => {
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

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-6xl px-6 py-6">
		<div class="flex items-start justify-between gap-4">
			<h1 class="text-3xl font-semibold tracking-tight">Agent Connections</h1>
		</div>

		{#if data?.error}
			<div class="alert alert-error text-sm mt-4">
				<span>{data.error}</span>
			</div>
		{/if}

		<div class="flex items-center justify-end gap-3 mt-6">
			<Pager
				bind:pageIndex
				bind:pageSize
				{totalCount}
				{currentCount}
				showPageSize={true}
			/>
		</div>

		<div class="mt-2 card bg-base-200/60 border border-base-300/60 shadow-lg">
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