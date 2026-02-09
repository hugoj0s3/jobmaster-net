<!-- src/routes/buckets/[id]/+page.svelte -->
<script lang="ts">
	type ActiveJob = {
		name: string;
		id: string;
		since: string;
		state: "Queued" | "Processing" | "Succeeded" | "Failed";
		agent: string;
		assigned: string;
		queueTime: string;
		runDuration: string;
	};

	type HistoryJob = { id: string; completedAt: string };
	type LogItem = { time: string; message: string; jobId: string };

	// Mock view-model (replace with real API data)
	const bucket = {
		name: "Payroll-Bucket-01",
		health: "Healthy",
		host: "Host 1",
		agent: "Postgres-1",
		worker: "Payroll",
		createdAgo: "3 days ago",
		status: "Opening",
		warnings: 3,
		lastUpdated: "12s ago",
	};

	const stats = {
		capacityPct: 81,
		capacityUsed: "81 GB",
		capacityTotal: "100 GB",
		queuedJobs: 3,
		avgQueueTime: "180ms",
		avgRunDuration: "1.2s",
	};

	const activeJobs: ActiveJob[] = [
		{
			name: "File Import",
			id: "abcde123",
			since: "5 min",
			state: "Queued",
			agent: "Postgres-Worker-01",
			assigned: "",
			queueTime: "180ms",
			runDuration: "1.1s",
		},
		{
			name: "Settle Payroll",
			id: "xyz7890",
			since: "2 min",
			state: "Processing",
			agent: "Postgres-Worker-01",
			assigned: "",
			queueTime: "1.4s",
			runDuration: "1.3s",
		},
		{
			name: "Payroll Consolidation",
			id: "",
			since: "1 min",
			state: "Processing",
			agent: "Postgres-Worker-01",
			assigned: "",
			queueTime: "1.1s",
			runDuration: "--",
		},
	];

	const jobHistory: HistoryJob[] = [
		{ id: "abcd1221", completedAt: "6 min ago" },
		{ id: "fghj999", completedAt: "8 min ago" },
	];

	const bucketLog: LogItem[] = [
		{ time: "1m ago", message: "Payroll Consolidation job moved to Processing", jobId: "deigh456" },
		{ time: "2m ago", message: "Settle Payroll job moved to Processing", jobId: "xyz7890" },
		{ time: "6m ago", message: "File Import job queued", jobId: "abcde123" },
		{ time: "9m ago", message: "Bucket ownership claimed by Postgres-Worker-01", jobId: "" },
	];

	let q = "";

	const stateBadge: Record<ActiveJob["state"], string> = {
		Queued: "badge-ghost",
		Processing: "badge-warning",
		Succeeded: "badge-success",
		Failed: "badge-error",
	};

	function filteredActiveJobs() {
		const s = q.trim().toLowerCase();
		if (!s) return activeJobs;
		return activeJobs.filter((j) => (j.name + " " + j.id + " " + j.state + " " + j.agent).toLowerCase().includes(s));
	}
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-7xl px-6 py-6">
		<!-- Top bar -->
		<div class="flex flex-wrap items-center justify-between gap-3">
			<a class="link link-hover text-sm opacity-70" href="/buckets">← Back to Buckets</a>

			<div class="flex items-center gap-3">
				<div class="text-sm opacity-70">
					<span class="hidden sm:inline">Last updated:</span>
					<span class="font-semibold">{bucket.lastUpdated}</span>
				</div>
				<button class="btn btn-sm btn-ghost">
					<span class="i">⟳</span>
					Refresh
				</button>
			</div>
		</div>

		<!-- Title -->
		<div class="mt-3">
			<h1 class="text-2xl font-semibold">Bucket Detail</h1>
		</div>

		<!-- Summary card -->
		<div class="card mt-4 bg-base-100 shadow">
			<div class="card-body gap-4">
				<div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
					<div class="flex items-center gap-4">
						<!-- Icon -->
						<div class="grid h-14 w-14 place-items-center rounded-2xl bg-base-200">
							<span class="text-xl">🗄️</span>
						</div>

						<div>
							<div class="flex items-center gap-2">
								<div class="text-xl font-semibold">{bucket.name}</div>
								<span class="badge badge-success badge-outline">●</span>
							</div>
							<div class="text-sm opacity-70">{bucket.health}</div>

							<div class="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm opacity-80">
								<span>Host: <span class="font-semibold">{bucket.host}</span></span>
								<span class="opacity-40">|</span>
								<span>Agent: <span class="font-semibold">{bucket.agent}</span></span>
								<span class="opacity-40">|</span>
								<span>Worker: <span class="font-semibold">🏷 {bucket.worker}</span></span>
								<span class="opacity-40">|</span>
								<span>{bucket.createdAgo}</span>
								<span class="opacity-40">|</span>
								<span>‹ {bucket.status}</span>
								<span class="badge badge-ghost badge-sm">⚠ {bucket.warnings}</span>
							</div>
						</div>
					</div>

					<button class="btn btn-sm btn-warning btn-outline">Draining Bucket</button>
				</div>

				<!-- Stats row -->
				<div class="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
					<!-- Capacity -->
					<div class="card bg-base-200/60">
						<div class="card-body">
							<div class="flex items-start justify-between gap-3">
								<div>
									<div class="text-sm opacity-70">Capacity</div>
									<div class="mt-1 text-3xl font-semibold">{stats.capacityPct}%</div>
								</div>
								<div class="radial-progress" style="--value:{stats.capacityPct}; --size:3.1rem; --thickness:0.45rem;">
									<span class="text-xs">{stats.capacityPct}%</span>
								</div>
							</div>
							<div class="mt-2 text-sm opacity-70">
								{stats.capacityUsed} / {stats.capacityTotal}
							</div>
						</div>
					</div>

					<!-- Queued jobs -->
					<div class="card bg-base-200/60">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<div class="text-sm opacity-70">Queued Jobs</div>
								<details class="dropdown dropdown-end">
									<summary class="btn btn-ghost btn-xs">⋯</summary>
									<ul class="menu dropdown-content z-[1] w-44 rounded-box bg-base-100 p-2 shadow">
										<li><a>View queued</a></li>
										<li><a>Export</a></li>
									</ul>
								</details>
							</div>
							<div class="mt-1 text-3xl font-semibold">{stats.queuedJobs}</div>
							<progress class="progress progress-primary mt-3" value="55" max="100"></progress>
							<div class="mt-2 text-xs opacity-60">{stats.capacityUsed} / {stats.capacityTotal}</div>
						</div>
					</div>

					<!-- Avg queue time -->
					<div class="card bg-base-200/60">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<div class="text-sm opacity-70">Avg Queue Time</div>
								<details class="dropdown dropdown-end">
									<summary class="btn btn-ghost btn-xs">⋯</summary>
									<ul class="menu dropdown-content z-[1] w-44 rounded-box bg-base-100 p-2 shadow">
										<li><a>Last 15m</a></li>
										<li><a>Last 1h</a></li>
									</ul>
								</details>
							</div>
							<div class="mt-1 text-3xl font-semibold">{stats.avgQueueTime}</div>
							<progress class="progress progress-secondary mt-3" value="35" max="100"></progress>
							<div class="mt-2 text-xs opacity-60">{stats.avgQueueTime}</div>
						</div>
					</div>

					<!-- Avg run duration -->
					<div class="card bg-base-200/60">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<div class="text-sm opacity-70">Avg Run Duration</div>
								<details class="dropdown dropdown-end">
									<summary class="btn btn-ghost btn-xs">⋯</summary>
									<ul class="menu dropdown-content z-[1] w-44 rounded-box bg-base-100 p-2 shadow">
										<li><a>Last 15m</a></li>
										<li><a>Last 1h</a></li>
									</ul>
								</details>
							</div>
							<div class="mt-1 text-3xl font-semibold">{stats.avgRunDuration}</div>
							<progress class="progress progress-accent mt-3" value="45" max="100"></progress>
							<div class="mt-2 text-xs opacity-60">1.4s ago</div>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Active Jobs -->
		<div class="mt-6">
			<div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
				<div class="flex items-center gap-3">
					<h2 class="text-xl font-semibold">Active Jobs</h2>
					<span class="badge badge-ghost">{activeJobs.length}</span>
					<button class="btn btn-sm btn-ghost">Abort All</button>
				</div>

				<label class="input input-bordered flex w-full items-center gap-2 md:w-80">
					<span class="opacity-60">🔎</span>
					<input class="grow" placeholder="Search jobs..." bind:value={q} />
				</label>
			</div>

			<div class="card mt-3 bg-base-100 shadow">
				<div class="card-body p-0">
					<div class="overflow-x-auto">
						<table class="table table-zebra">
							<thead>
							<tr>
								<th>Job</th>
								<th class="hidden md:table-cell">ID</th>
								<th>Since</th>
								<th>State</th>
								<th class="hidden lg:table-cell">Agent</th>
								<th class="hidden lg:table-cell">Assigned</th>
								<th>Queue Time</th>
								<th class="text-right">Run Duration</th>
							</tr>
							</thead>
							<tbody>
							{#each filteredActiveJobs() as j (j.name + j.id)}
								<tr>
									<td>
										<div class="flex items-center gap-2">
											<span class="text-success">♥</span>
											<span class="font-medium">{j.name}</span>
										</div>
									</td>
									<td class="hidden md:table-cell opacity-80">{j.id || "--"}</td>
									<td class="opacity-80">{j.since}</td>
									<td>
										<span class={"badge badge-sm " + stateBadge[j.state]}>{j.state}</span>
									</td>
									<td class="hidden lg:table-cell opacity-80">{j.agent}</td>
									<td class="hidden lg:table-cell opacity-80">{j.assigned || "--"}</td>
									<td class="opacity-80">{j.queueTime}</td>
									<td class="text-right opacity-80">{j.runDuration}</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>

		<!-- Bottom panels -->
		<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
			<!-- Job History -->
			<div class="card bg-base-100 shadow">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-2">
							<h3 class="text-lg font-semibold">Job History</h3>
							<span class="badge badge-ghost">412</span>
						</div>
						<details class="dropdown dropdown-end">
							<summary class="btn btn-ghost btn-sm">Filter</summary>
							<ul class="menu dropdown-content z-[1] w-48 rounded-box bg-base-100 p-2 shadow">
								<li><a>Total</a></li>
								<li><a>Completed Jobs</a></li>
								<li><a>Failed Jobs</a></li>
							</ul>
						</details>
					</div>

					<div class="overflow-x-auto">
						<table class="table table-sm">
							<thead>
							<tr>
								<th>Job ID</th>
								<th class="text-right">Completed</th>
							</tr>
							</thead>
							<tbody>
							{#each jobHistory as h (h.id)}
								<tr>
									<td>
										<div class="flex items-center gap-2">
											<span class="text-success">♥</span>
											<span class="font-medium">{h.id}</span>
										</div>
									</td>
									<td class="text-right opacity-80">{h.completedAt}</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>

			<!-- Bucket Log -->
			<div class="card bg-base-100 shadow">
				<div class="card-body">
					<div class="flex items-center justify-between gap-3">
						<div class="flex items-center gap-2">
							<h3 class="text-lg font-semibold">Bucket Log</h3>
							<details class="dropdown dropdown-end">
								<summary class="btn btn-ghost btn-sm">⋯</summary>
								<ul class="menu dropdown-content z-[1] w-44 rounded-box bg-base-100 p-2 shadow">
									<li><a>Copy</a></li>
									<li><a>Export</a></li>
								</ul>
							</details>
						</div>

						<button class="btn btn-sm btn-error btn-outline">Abandon Bucket</button>
					</div>

					<div class="overflow-x-auto">
						<table class="table table-sm">
							<thead>
							<tr>
								<th>Time</th>
								<th>Message</th>
								<th class="text-right">Job ID</th>
							</tr>
							</thead>
							<tbody>
							{#each bucketLog as l (l.time + l.message)}
								<tr>
									<td class="opacity-80">{l.time}</td>
									<td class="opacity-90">{l.message}</td>
									<td class="text-right opacity-70">{l.jobId || "--"}</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>

		<div class="mt-8 text-xs opacity-50">
			Tip: replace mock data with API calls and bind to route param <code>[id]</code>.
		</div>
	</div>
</div>
