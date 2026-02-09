<script lang="ts">
	type Job = {
		id: string;
		type: string;
		priority: "Critical" | "High" | "Medium" | "Low";
		status: "Processing" | "Queued" | "SavePending" | "Failed";
		worker: string;
		host: string;
		scheduled: string;
	};

	let query = "";

	const jobs: Job[] = [
		{
			id: "8b12...f9a7",
			type: "FetchDataHandler",
			priority: "Critical",
			status: "Processing",
			worker: "Payroll-Worker-02",
			host: "Host 3",
			scheduled: "44s ago"
		},
		{
			id: "f19b...770e",
			type: "SendEmailHandler",
			priority: "High",
			status: "Queued",
			worker: "Payroll-Worker-01",
			host: "Host 3",
			scheduled: "24s ago"
		},
		{
			id: "50d2...de4c",
			type: "MethodHandler",
			priority: "Medium",
			status: "SavePending",
			worker: "Payroll-Worker-01",
			host: "Host 4",
			scheduled: "22m ago"
		},
		{
			id: "717e...7d7a",
			type: "InvoicingHandler",
			priority: "Low",
			status: "Failed",
			worker: "DNS-Worker-01",
			host: "Host 1",
			scheduled: "20m ago"
		}
	];

	const jobsInProgress = 12;
	const failedToday = 5;
	const workersOnline = "4 / 4";
	const buckets = "24 / 24";

	$: filtered = jobs.filter((j) => {
		const q = query.trim().toLowerCase();
		if (!q) return true;
		return (
			j.id.toLowerCase().includes(q) ||
			j.type.toLowerCase().includes(q) ||
			j.worker.toLowerCase().includes(q)
		);
	});

	const statusClasses = {
		Processing: "badge-info",
		Queued: "badge-warning",
		SavePending: "badge-neutral",
		Failed: "badge-error",
		Succeeded: "badge-success"
	};

	const priorityClasses = {
		Critical: "badge-error",
		High: "badge-warning",
		Medium: "badge-info",
		Low: "badge-neutral"
	};
</script>

<div class="min-h-screen bg-base-100">
	<!-- glow background -->
	<div
		class="pointer-events-none fixed inset-0 opacity-50"
		style="
      background:
        radial-gradient(1200px 600px at 30% 10%, rgba(45,212,191,0.10), transparent 60%),
        radial-gradient(900px 500px at 70% 20%, rgba(96,165,250,0.10), transparent 60%),
        radial-gradient(900px 500px at 80% 80%, rgba(167,139,250,0.10), transparent 60%);
    "
	/>

	<div class="relative mx-auto w-full max-w-6xl px-6 py-10">
		<!-- Header -->
		<div class="flex items-center justify-between">
			<h1 class="text-4xl font-semibold">Jobs</h1>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last updated: 10s ago</span>

				<label class="flex items-center gap-2">
					<input type="checkbox" class="toggle toggle-sm" checked />
					<span class="font-semibold">Refresh</span>
				</label>

				<button class="btn btn-ghost btn-sm btn-square">
					<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="currentColor" viewBox="0 0 24 24">
						<circle cx="12" cy="5" r="2" />
						<circle cx="12" cy="12" r="2" />
						<circle cx="12" cy="19" r="2" />
					</svg>
				</button>
			</div>
		</div>

		<!-- Stat cards -->
		<div class="mt-7 grid grid-cols-1 gap-5 md:grid-cols-4">
			<div class="card bg-base-200/70 shadow-xl backdrop-blur">
				<div class="card-body">
					<div class="flex justify-between">
						<div>
							<div class="text-sm opacity-80">Jobs In Progress</div>
							<div class="mt-2 text-5xl font-semibold">{jobsInProgress}</div>
							<div class="mt-1 text-sm opacity-70">in-flight now</div>
						</div>
					</div>
				</div>
			</div>

			<!-- Failed Jobs (icon fixed) -->
			<div class="card bg-base-200/70 shadow-xl backdrop-blur">
				<div class="card-body">
					<div class="flex justify-between">
						<div>
							<div class="text-sm opacity-80">Failed Jobs</div>
							<div class="mt-2 text-5xl font-semibold text-error">{failedToday}</div>
							<div class="mt-1 text-sm opacity-70">/ today</div>
						</div>
						<div class="text-error">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-10 w-10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<circle cx="12" cy="12" r="9" />
								<path d="M15 9l-6 6" />
								<path d="M9 9l6 6" />
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/70 shadow-xl backdrop-blur">
				<div class="card-body">
					<div>
						<div class="text-sm opacity-80">Workers Online</div>
						<div class="mt-2 text-5xl font-semibold">{workersOnline}</div>
						<div class="mt-1 text-sm opacity-70">heartbeats = 12s</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/70 shadow-xl backdrop-blur">
				<div class="card-body">
					<div>
						<div class="text-sm opacity-80">Buckets</div>
						<div class="mt-2 text-5xl font-semibold">{buckets}</div>
						<div class="mt-1 text-sm opacity-70">/ 0 lost, 1 draining</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Search -->
		<div class="mt-6">
			<label class="input input-bordered flex items-center gap-2 bg-base-200/60 backdrop-blur">
				<input class="grow" placeholder="Search jobs..." bind:value={query} />
			</label>
		</div>

		<!-- Table -->
		<div class="mt-4 card bg-base-200/50 shadow-xl backdrop-blur">
			<div class="overflow-x-auto">
				<table class="table">
					<thead class="opacity-70">
					<tr>
						<th>Job ID</th>
						<th>Type</th>
						<th>Priority</th>
						<th>Status</th>
						<th>Worker</th>
						<th class="text-right">Scheduled</th>
					</tr>
					</thead>

					<tbody>
					{#each filtered as j (j.id)}
						<tr class="hover">
							<td class="pl-6">
								<div class="flex items-center gap-2">
									<span class="font-medium">{j.id}</span>
									<!-- copy icon -->
									<button class="btn btn-ghost btn-xs btn-square opacity-60" aria-label="Copy id">
										<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
											<path d="M9 9h10v10H9z" />
											<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
										</svg>
									</button>
								</div>
							</td>

							<td>{j.type}</td>

							<td>
								<span class={`badge badge-sm ${statusClasses[j.status] ?? "badge-ghost"}`}>
									{j.status}
								</span>
							</td>


							<td>
								<span class={`badge badge-sm ${priorityClasses[j.priority] ?? "badge-ghost"}`}>
									{j.priority}
								</span>
							</td>


							<td>
								<div>
									<div class="font-medium">{j.worker}</div>
									<div class="text-xs opacity-60">• {j.host}</div>
								</div>
							</td>

							<td class="text-right opacity-80">{j.scheduled}</td>
						</tr>
					{/each}
					</tbody>
				</table>
			</div>

			<!-- Pagination -->
			<div class="flex items-center justify-between px-6 py-4 opacity-80">
				<div class="flex gap-2">
					<button class="btn btn-ghost btn-sm">‹</button>
					<button class="btn btn-ghost btn-sm">›</button>
				</div>
				<div class="text-sm">Page 1 of 14</div>
				<div class="flex gap-2">
					<button class="btn btn-ghost btn-sm">«</button>
					<button class="btn btn-ghost btn-sm">»</button>
				</div>
			</div>
		</div>
	</div>
</div>
