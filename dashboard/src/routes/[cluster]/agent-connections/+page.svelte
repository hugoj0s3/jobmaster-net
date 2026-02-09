<script lang="ts">
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

	const allClusters = ["All", "QA - Testing", "Production", "Test Cluster", "Dev Cluster"];

	let clusterFilter = "All";
	let query = "";
	let rowsPerPage = 6;
	let page = 1;

	let rows: AgentConnRow[] = [
		{
			id: "payroll-worker-02",
			name: "Payroll-Worker-02",
			sub: "PostgreSQL  v12.0.0",
			cluster: "QA - Testing",
			clusterSub: "PostgreSQL v12.0.0",
			health: "Warning",
			workers: 3,
			bucketsUsed: 14,
			bucketsTotal: 15,
			draining: 1,
			selected: true
		},
		{
			id: "postgres-01",
			name: "Postgres-01",
			sub: "PostgreSQL  v12.0.0",
			cluster: "QA - Testing",
			clusterSub: "PostgreSQL v12.0.0",
			health: "OK",
			workers: 7,
			bucketsUsed: 8,
			bucketsTotal: 10
		},
		{
			id: "mssql-agent-01",
			name: "MSSQL-Agent-01",
			sub: "SQL Server v2019",
			cluster: "Production",
			clusterSub: "SQL Server v2019",
			health: "OK",
			workers: 5,
			bucketsUsed: 10,
			bucketsTotal: 20
		},
		{
			id: "nats-worker-01",
			name: "NATS-Worker-01",
			sub: "NATS",
			cluster: "Test Cluster",
			clusterSub: "NATS",
			health: "OK",
			workers: 12,
			bucketsUsed: 22,
			bucketsTotal: 30,
			draining: 2
		},
		{
			id: "postgres-02",
			name: "Postgres-02",
			sub: "PostgreSQL  v14.0.0",
			cluster: "Dev Cluster",
			clusterSub: "PostgreSQL v14.0.0",
			health: "Error",
			workers: 0,
			bucketsUsed: 0,
			bucketsTotal: 15
		},
		{
			id: "mysql-agent-01",
			name: "MySQL-Agent-01",
			sub: "MySQL v8.0.2",
			cluster: "Production",
			clusterSub: "MySQL v8.0.2",
			health: "OK",
			workers: 8,
			bucketsUsed: 20,
			bucketsTotal: 25
		}
	];

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

	$: list = filtered();
	$: totalPages = Math.max(1, Math.ceil(list.length / rowsPerPage));
	$: page = Math.min(page, totalPages);
	$: start = (page - 1) * rowsPerPage;
	$: view = list.slice(start, start + rowsPerPage);

	function selectRow(id: string) {
		rows = rows.map((r) => ({ ...r, selected: r.id === id }));
	}

	function toggleAll(e: Event) {
		const checked = (e.currentTarget as HTMLInputElement).checked;
		rows = rows.map((r) => ({ ...r, selected: checked }));
	}

	function toggleRow(id: string, e: Event) {
		const checked = (e.currentTarget as HTMLInputElement).checked;
		rows = rows.map((r) => (r.id === id ? { ...r, selected: checked } : r));
	}
</script>

<div class="min-h-screen bg-base-300 relative overflow-hidden">
	<div class="relative max-w-6xl mx-auto px-8 py-10">
		<!-- Header -->
		<div class="flex items-start justify-between gap-6">
			<div>
				<h1 class="text-4xl font-semibold tracking-tight">Agent Connections</h1>
				<p class="text-base-content/60 mt-2">Select an agent connection to view details.</p>
			</div>

			<div class="flex items-center gap-3">
				<button class="btn btn-success btn-sm gap-2">
					<span class="text-lg leading-none">＋</span>
					Ad Connection
				</button>
			</div>
		</div>

		<!-- Filters -->
		<div class="mt-8 flex items-center justify-end gap-3">
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
			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-base-content/70">
						<th class="w-12">
							<input
								type="checkbox"
								class="checkbox checkbox-sm"
								checked={rows.every((r) => r.selected)}
								on:change={toggleAll}
								aria-label="Select all"
							/>
						</th>
						<th>
							<div class="flex items-center gap-2">
								<span>Agent Connection</span>
								<span class="opacity-40">⇅</span>
							</div>
						</th>
						<th>
							<div class="flex items-center gap-2">
								<span>Cluster</span>
								<span class="opacity-40">⇅</span>
							</div>
						</th>
						<th>
							<div class="flex items-center gap-2">
								<span>Health</span>
								<span class="opacity-40">⇅</span>
							</div>
						</th>
						<th class="text-right">
							<div class="flex items-center justify-end gap-2">
								<span># Workers</span>
								<span class="opacity-40">⇅</span>
							</div>
						</th>
						<th>
							<div class="flex items-center gap-2">
								<span>Buckets</span>
								<span class="opacity-40">⇅</span>
							</div>
						</th>
					</tr>
					</thead>

					<tbody>
					{#each view as r (r.id)}
						<tr
							class={`cursor-pointer transition
                  ${r.selected ? "bg-primary/10 outline outline-1 outline-primary/30" : "hover:bg-base-100/40"}`}
							on:click={() => selectRow(r.id)}
						>
							<td>
								<input
									type="checkbox"
									class="checkbox checkbox-sm"
									checked={!!r.selected}
									on:click|stopPropagation
									on:change={(e) => toggleRow(r.id, e)}
									aria-label={`Select ${r.name}`}
								/>
							</td>

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

			<!-- Footer / Pagination -->
			<div class="flex items-center justify-between px-5 py-4 border-t border-base-content/10">
				<div class="flex items-center gap-2 text-sm text-base-content/70">
					<span>Rows per page:</span>
					<select
						class="select select-bordered select-sm"
						bind:value={rowsPerPage}
						on:change={() => (page = 1)}
						aria-label="Rows per page"
					>
						<option value={6}>6</option>
						<option value={10}>10</option>
						<option value={20}>20</option>
					</select>
				</div>

				<div class="flex items-center gap-2">
					<button class="btn btn-ghost btn-sm" on:click={() => (page = Math.max(1, page - 1))} aria-label="Previous">
						‹
					</button>

					<div class="join">
						<button class="btn btn-outline btn-sm join-item pointer-events-none w-12">{page}</button>
						<button class="btn btn-ghost btn-sm join-item pointer-events-none text-base-content/60">/</button>
						<button class="btn btn-ghost btn-sm join-item pointer-events-none text-base-content/60 w-12">
							{totalPages}
						</button>
					</div>

					<button class="btn btn-ghost btn-sm" on:click={() => (page = Math.min(totalPages, page + 1))} aria-label="Next">
						›
					</button>
				</div>
			</div>
		</div>
	</div>
</div>