<script lang="ts">
	type BucketRow = {
		name: string;
		jobsCompleted: number;
		active: number;
		usedLabel: string;
		queueTime: string;
		runDuration: string;
		status: "healthy" | "lost" | "draining";
	};

	const kpis = {
		total: 24,
		healthy: 23,
		lost: 0,
		draining: 1
	};

	let statusFilter: "all" | "healthy" | "lost" | "draining" = "all";
	let search = "";

	const rows: BucketRow[] = [
		{
			name: "Payroll-Bucket-01",
			jobsCompleted: 412,
			active: 0,
			usedLabel: "81 GB (81%)",
			queueTime: "200ms",
			runDuration: "1.2s",
			status: "healthy"
		},
		{
			name: "DNS-Bucket-Webhook",
			jobsCompleted: 304,
			active: 1,
			usedLabel: "27 GB (45%)",
			queueTime: "250ms",
			runDuration: "2.4s",
			status: "healthy"
		},
		{
			name: "FileImport-Bucket-Backup",
			jobsCompleted: 287,
			active: 0,
			usedLabel: "40 GB (62%)",
			queueTime: "150ms",
			runDuration: "2.1s",
			status: "healthy"
		},
		{
			name: "UserImport-Bucket-Prod",
			jobsCompleted: 252,
			active: 1,
			usedLabel: "71 GB (71%)",
			queueTime: "190ms",
			runDuration: "1.3s",
			status: "healthy"
		},
		{
			name: "Payroll-Bucket-02",
			jobsCompleted: 177,
			active: 0,
			usedLabel: "41 GB (41%)",
			queueTime: "90ms",
			runDuration: "1.6s",
			status: "draining"
		}
	];

	$: filtered = rows
		.filter((r) => (statusFilter === "all" ? true : r.status === statusFilter))
		.filter((r) => r.name.toLowerCase().includes(search.trim().toLowerCase()));

	const badgeFor = (s: BucketRow["status"]) => {
		if (s === "healthy") return "badge-success";
		if (s === "lost") return "badge-error";
		return "badge-warning";
	};

	const dotFor = (s: BucketRow["status"]) => {
		if (s === "healthy") return "bg-success";
		if (s === "lost") return "bg-error";
		return "bg-warning";
	};
</script>

<div class="min-h-screen bg-base-100">
	<main class="relative mx-auto max-w-6xl px-6 py-10">
		<!-- Header -->
		<div class="flex flex-wrap items-start justify-between gap-4">
			<div>
				<h1 class="text-3xl font-semibold text-base-content">Buckets</h1>
				<p class="mt-1 text-sm text-base-content/60">Cluster: QA - Testing • Admin • Active • Connected</p>
			</div>

			<div class="flex items-center gap-3">
				<div class="text-sm text-base-content/60">Last updated: 6s ago</div>
				<button class="btn btn-sm btn-outline">Refresh</button>
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
							<div class="text-sm text-base-content/70">Healthy</div>
							<div class="mt-1 text-4xl font-semibold">{kpis.healthy}</div>
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

				<div class="flex flex-wrap items-center gap-2">
					<select class="select select-sm select-bordered bg-base-200/60" bind:value={statusFilter}>
						<option value="all">Status: All</option>
						<option value="healthy">Healthy</option>
						<option value="lost">Lost</option>
						<option value="draining">Draining</option>
					</select>

					<label class="input input-sm input-bordered flex items-center gap-2 bg-base-200/60">
						<span class="opacity-60 text-base leading-none">🔎</span>
						<input class="grow" placeholder="Search" bind:value={search} />
					</label>
				</div>
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
								<div class="mt-2 text-2xl font-semibold">1,189</div>
							</div>
							<button class="btn btn-ghost btn-sm opacity-70">⋯</button>
						</div>

						<div class="mt-4 h-40 w-full rounded-xl bg-base-300/30 overflow-hidden">
							<!-- simple bar+line placeholder -->
							<svg viewBox="0 0 360 120" class="h-full w-full">
								{#each Array(12) as _, i}
									<rect
										x={(i * 28) + 18}
										y={30 + (i % 4) * 10}
										width="16"
										height={70 - (i % 4) * 10}
										class="fill-primary/35"
										rx="4"
									/>
								{/each}
								<path
									d="M10,92 C60,96 90,78 130,80 C170,82 190,70 220,62 C260,50 300,46 350,28"
									fill="none"
									stroke="currentColor"
									stroke-width="2.5"
									class="text-secondary/70"
								/>
							</svg>
						</div>

						<div class="mt-3 flex items-center gap-4 text-sm text-base-content/60">
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-primary/70"></span>
								Jobs Completed
							</div>
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-secondary/70"></span>
								Active Jobs
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
								<div class="mt-2 text-2xl font-semibold">1.4s</div>
							</div>
							<button class="btn btn-ghost btn-sm opacity-70">⋯</button>
						</div>

						<div class="mt-4 h-40 w-full rounded-xl bg-base-300/30 overflow-hidden">
							<svg viewBox="0 0 360 120" class="h-full w-full">
								<path
									d="M10,50 C60,44 110,64 150,56 C190,48 220,62 260,54 C300,46 320,50 350,30"
									fill="none"
									stroke="currentColor"
									stroke-width="2.5"
									class="text-secondary/70"
								/>
								<path
									d="M10,72 C80,72 110,66 150,70 C190,74 220,70 260,72 C300,74 330,78 350,60"
									fill="none"
									stroke="currentColor"
									stroke-width="2.5"
									class="text-primary/55"
								/>
							</svg>
						</div>

						<div class="mt-3 flex items-center gap-4 text-sm text-base-content/60">
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-secondary/70"></span>
								Avg Run Duration
							</div>
							<div class="flex items-center gap-2">
								<span class="h-2 w-2 rounded-full bg-primary/60"></span>
								Avg Queue Duration
							</div>
						</div>
					</div>
				</div>
			</div>
		</section>

		<!-- Table -->
		<section class="mt-10">
			<div class="flex flex-wrap items-center justify-between gap-3">
				<h2 class="text-xl font-semibold text-base-content">Buckets Table</h2>

				<label class="input input-sm input-bordered flex items-center gap-2 bg-base-200/60">
					<span class="opacity-60 text-base leading-none">🔎</span>
					<input class="grow" placeholder="Search buckets..." bind:value={search} />
				</label>
			</div>

			<div class="mt-4 card bg-base-200/60 border border-base-300/60 shadow-lg">
				<div class="overflow-x-auto">
					<table class="table table-zebra">
						<thead>
						<tr class="text-base-content/70">
							<th class="w-[44%]">Name</th>
							<th class="text-right">Jobs Completed</th>
							<th class="text-right">Active</th>
							<th class="text-right">Used</th>
							<th class="text-right">Queue Time</th>
							<th class="text-right">Run Duration</th>
							<th class="text-right">Status</th>
						</tr>
						</thead>
						<tbody>
						{#each filtered as r}
							<tr>
								<td>
									<div class="flex items-center gap-3">
										<span class={`h-2.5 w-2.5 rounded-full ${dotFor(r.status)}`}></span>
										<div class="font-medium">{r.name}</div>
									</div>
								</td>
								<td class="text-right font-medium">{r.jobsCompleted}</td>
								<td class="text-right">{r.active}</td>
								<td class="text-right text-base-content/70">{r.usedLabel}</td>
								<td class="text-right">{r.queueTime}</td>
								<td class="text-right">{r.runDuration}</td>
								<td class="text-right">
										<span class={`badge badge-sm ${badgeFor(r.status)}`}>
											{r.status}
										</span>
								</td>
							</tr>
						{/each}

						{#if filtered.length === 0}
							<tr>
								<td colspan="7" class="py-10 text-center text-base-content/60">
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