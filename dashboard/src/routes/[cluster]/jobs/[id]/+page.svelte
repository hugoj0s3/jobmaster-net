<script lang="ts">
	// Mock data (troque por props / load / fetch)
	const job = {
		idShort: "8b12f…7d1a",
		id: "8b12f9e3-45c8-476f-827d-7d1a7648225",
		handler: "FetchDataHandler",
		status: "Processing",
		priority: "Critical",
		retries: { current: 2, max: 3 },
		createdAt: "Apr 24, 2024, 10:15 AM",
		startedAt: "Apr 24, 2024, 10:17 AM",
		duration: "1m 32s",
		worker: { name: "Payroll-Worker-02", host: "Host 3" },
		agentConnection: "Postgres-1",
		queue: "Queue 4",
		scheduledTime: "Apr 24, 2024, 10:15 AM",
		lastHeartbeat: "4 seconds ago",
		data: {
			query: "SELECT * FROM users WHERE active = true",
			fetchSize: 500,
			lane: "DataProcessing"
		}
	};

	const logs = [
		{ time: "10:17 AM", text: "Started job on Payroll-Worker-02", type: "start" },
		{ time: "10:18 AM", text: "Fetching data from the database...", type: "info" },
		{ time: "10:18 AM", text: "Processing record batch 1 / 3", type: "step" },
		{ time: "10:19 AM", text: "Processing record batch 2 / 3", type: "step" },
		{ time: "10:20 AM", text: "Processing record batch 3 / 3", type: "step" }
	];

	const statusBadge: Record<string, string> = {
		Processing: "badge-success",
		Queued: "badge-warning",
		SavePending: "badge-neutral",
		Failed: "badge-error",
		Succeeded: "badge-success",
		HeldOnMaster: "badge-ghost",
		AssignedToBucket: "badge-info",
		Cancelled: "badge-ghost"
	};

	const priorityBadge: Record<string, string> = {
		Critical: "badge-error",
		High: "badge-warning",
		Medium: "badge-info",
		Low: "badge-neutral",
		VeryLow: "badge-ghost"
	};

	const statusDot: Record<string, string> = {
		start: "bg-success",
		info: "bg-info",
		step: "bg-secondary",
		warn: "bg-warning",
		error: "bg-error"
	};

	let page = 1;
	const totalPages = 14;

	function cancelJob() {
		// TODO: call api
		console.log("Cancel job", job.id);
	}

	function retryJob() {
		// TODO: call api
		console.log("Retry job", job.id);
	}
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto w-full max-w-6xl px-6 py-6">
		<!-- Breadcrumb -->
		<div class="breadcrumbs text-sm opacity-80">
			<ul>
				<li>
					<a class="link link-hover">Jobs</a>
				</li>
				<li>Job Details</li>
			</ul>
		</div>

		<!-- Header -->
		<div class="mt-3 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
			<div>
				<div class="text-2xl font-semibold">
					Job ID: <span class="font-mono">{job.idShort}</span>
					<span class="opacity-60">·</span>
					<span class="font-semibold">{job.handler}</span>
				</div>

				<div class="mt-3 flex flex-wrap items-center gap-2">
          <span class={`badge badge-lg gap-2 ${priorityBadge[job.priority] ?? "badge-ghost"}`}>
            <span class="inline-block size-2 rounded-full bg-current opacity-60"></span>
						{job.priority}
          </span>

					<span class={`badge badge-lg gap-2 ${statusBadge[job.status] ?? "badge-ghost"}`}>
            <span class="inline-block size-2 rounded-full bg-current opacity-60"></span>
						{job.status}
          </span>
				</div>
			</div>

			<div class="flex flex-wrap gap-2">
				<button class="btn btn-outline btn-sm" on:click={cancelJob}>
					<!-- icon -->
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
						<path d="M12 2a10 10 0 1 0 10 10A10.011 10.011 0 0 0 12 2Zm5 11H7a1 1 0 0 1 0-2h10a1 1 0 0 1 0 2Z" />
					</svg>
					Cancel Job
				</button>

				<button class="btn btn-primary btn-sm" on:click={retryJob}>
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
						<path
							d="M12 6V3L8 7l4 4V8a4 4 0 1 1-4 4H6a6 6 0 1 0 6-6Z"
						/>
					</svg>
					Retry Job
				</button>

				<div class="dropdown dropdown-end">
					<button class="btn btn-ghost btn-sm" tabindex="0" aria-label="More">
						<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor">
							<path d="M12 7a2 2 0 1 0-2-2 2 2 0 0 0 2 2Zm0 2a2 2 0 1 0 2 2 2 2 0 0 0-2-2Zm0 6a2 2 0 1 0 2 2 2 2 0 0 0-2-2Z" />
						</svg>
					</button>
					<ul tabindex="0" class="menu dropdown-content z-[1] mt-2 w-48 rounded-box bg-base-100 p-2 shadow">
						<li><a>Copy Job ID</a></li>
						<li><a>View JSON</a></li>
						<li><a class="text-error">Delete (danger)</a></li>
					</ul>
				</div>
			</div>
		</div>

		<!-- KPI Bar -->
		<div class="mt-5 rounded-box border border-base-300 bg-base-100">
			<div class="grid grid-cols-1 gap-0 md:grid-cols-5">
				<div class="p-4">
					<div class="text-xs opacity-60">Status</div>
					<div class="mt-2">
            <span class={`badge gap-2 ${statusBadge[job.status] ?? "badge-ghost"}`}>
              <span class="inline-block size-2 rounded-full bg-current opacity-60"></span>
							{job.status}
            </span>
					</div>
				</div>

				<div class="divider divider-horizontal hidden md:flex m-0"></div>

				<div class="p-4">
					<div class="text-xs opacity-60">Retries</div>
					<div class="mt-1 text-lg font-semibold">
						{job.retries.current} <span class="opacity-60">/</span> {job.retries.max}
						<span class="ml-2 text-sm font-normal opacity-60">Attempts</span>
					</div>
				</div>

				<div class="divider divider-horizontal hidden md:flex m-0"></div>

				<div class="p-4">
					<div class="text-xs opacity-60">Created</div>
					<div class="mt-1 font-medium">{job.createdAt}</div>
				</div>

				<div class="divider divider-horizontal hidden md:flex m-0"></div>

				<div class="p-4">
					<div class="text-xs opacity-60">Started</div>
					<div class="mt-1 font-medium">{job.startedAt}</div>
				</div>

				<div class="divider divider-horizontal hidden md:flex m-0"></div>

				<div class="p-4">
					<div class="text-xs opacity-60">Duration</div>
					<div class="mt-1 text-lg font-semibold">{job.duration}</div>
				</div>
			</div>
		</div>

		<!-- Content -->
		<div class="mt-5 grid grid-cols-1 gap-4 lg:grid-cols-2">
			<!-- Job Information -->
			<div class="card border border-base-300 bg-base-100">
				<div class="card-body">
					<div class="card-title">Job Information</div>

					<div class="mt-2 overflow-x-auto">
						<table class="table table-sm">
							<tbody>
							<tr>
								<td class="w-40 opacity-60">Job ID</td>
								<td class="font-mono">{job.id}</td>
							</tr>
							<tr>
								<td class="opacity-60">Worker</td>
								<td>
									<span class="font-medium">{job.worker.name}</span>
									<span class="opacity-60">({job.worker.host})</span>
								</td>
							</tr>
							<tr>
								<td class="opacity-60">Agent Connection</td>
								<td class="font-medium">{job.agentConnection}</td>
							</tr>
							<tr>
								<td class="opacity-60">Priority</td>
								<td>
                    <span class={`badge gap-2 ${priorityBadge[job.priority] ?? "badge-ghost"}`}>
                      <span class="inline-block size-2 rounded-full bg-current opacity-60"></span>
											{job.priority}
                    </span>
								</td>
							</tr>
							<tr>
								<td class="opacity-60">Queue</td>
								<td>
                    <span class="badge badge-warning gap-2">
                      <span class="inline-block size-2 rounded-full bg-warning opacity-80"></span>
											{job.queue}
                    </span>
								</td>
							</tr>
							<tr>
								<td class="opacity-60">Scheduled Time</td>
								<td>{job.scheduledTime}</td>
							</tr>
							<tr>
								<td class="opacity-60">Last Heartbeat</td>
								<td>{job.lastHeartbeat}</td>
							</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>

			<!-- Job Data -->
			<div class="card border border-base-300 bg-base-100">
				<div class="card-body">
					<div class="card-title">Job Data</div>

					<div class="mt-2 rounded-box bg-base-200 p-4">
						<pre class="text-xs leading-relaxed"><code>{JSON.stringify(job.data, null, 2)}</code></pre>
					</div>
				</div>
			</div>

			<!-- Execution Logs (span full on lg) -->
			<div class="card border border-base-300 bg-base-100 lg:col-span-2">
				<div class="card-body">
					<div class="flex items-center justify-between gap-2">
						<div class="card-title">Execution Logs</div>

						<div class="join">
							<button
								class="btn btn-sm join-item"
								on:click={() => (page = Math.max(1, page - 1))}
								disabled={page === 1}
								aria-label="Previous page"
							>
								‹
							</button>
							<button class="btn btn-sm join-item btn-ghost pointer-events-none">
								Page {page} <span class="opacity-60">of</span> {totalPages}
							</button>
							<button
								class="btn btn-sm join-item"
								on:click={() => (page = Math.min(totalPages, page + 1))}
								disabled={page === totalPages}
								aria-label="Next page"
							>
								›
							</button>
						</div>
					</div>

					<div class="mt-3 divide-y divide-base-200 rounded-box border border-base-200">
						{#each logs as l, idx (idx)}
							<div class="flex items-center justify-between gap-3 px-4 py-3">
								<div class="flex items-center gap-3">
									<span class={`inline-block size-2 rounded-full ${statusDot[l.type] ?? "bg-base-content"}`}></span>

									<div class="flex flex-wrap items-center gap-2">
										<span class="text-sm opacity-70">{l.time}</span>
										<span class="opacity-40">|</span>
										<span class="text-sm">{l.text}</span>
									</div>
								</div>

								<button class="btn btn-ghost btn-xs" aria-label="Log options">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
										<path
											d="M12 7a2 2 0 1 0-2-2 2 2 0 0 0 2 2Zm0 2a2 2 0 1 0 2 2 2 2 0 0 0-2-2Zm0 6a2 2 0 1 0 2 2 2 2 0 0 0-2-2Z"
										/>
									</svg>
								</button>
							</div>
						{/each}
					</div>

					<div class="mt-3 flex items-center justify-end">
						<div class="join">
							<button class="btn btn-sm join-item" on:click={() => (page = 1)} disabled={page === 1}>
								«
							</button>
							<button class="btn btn-sm join-item" on:click={() => (page = Math.max(1, page - 1))} disabled={page === 1}>
								‹
							</button>
							<button class="btn btn-sm join-item btn-ghost pointer-events-none">
								{page}
							</button>
							<button
								class="btn btn-sm join-item"
								on:click={() => (page = Math.min(totalPages, page + 1))}
								disabled={page === totalPages}
							>
								›
							</button>
							<button class="btn btn-sm join-item" on:click={() => (page = totalPages)} disabled={page === totalPages}>
								»
							</button>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- bottom spacing -->
		<div class="h-6"></div>
	</div>
</div>
