<script lang="ts">
	// Mock data (replace with load() / API later)
	const worker = {
		name: "Payroll-Worker-01",
		status: "Online",
		mode: "Full mode",
		agentConnection: "Postgres-1",
		lane: "Payroll",
		assignedHost: "Host 2",
		lastSeen: "10s ago",
		createdAt: "11 hours ago"
	};

	const stats = {
		heartbeat: "10s ago",
		inflight: 4,
		processed: 6341,
		failed: 192,
		readyToDelete: 0
	};

	const config = {
		mode: "Full",
		lane: "Full",
		parallelismFactor: 2.0,
		batchSize: 1000
	};

	const connection = {
		agentConnection: "Postgres-1",
		agentType: "PostgresQL",
		footprint: "Host=localhost;DB"
	};

	const recentBuckets = [
		{ id: "********_1234", priority: "High", status: "Active", host: "Host 2" },
		{ id: "********_5678", priority: "Low", status: "Lost", host: "Host 2" },
		{ id: "********_9876", priority: "Medium", status: "Draining", host: "Host 2" }
	];

	const recentActivity = [
		{ type: "success", title: "Succeeded", msg: "Fetch Data Job", when: "54s ago" },
		{ type: "error", title: "Error", msg: "Archiving Payroll Reports", when: "14m ago" }
	];

	function badgeForStatus(status: string) {
		switch (status.toLowerCase()) {
			case "active":
				return "badge badge-success badge-outline";
			case "lost":
				return "badge badge-error badge-outline";
			case "draining":
				return "badge badge-warning badge-outline";
			default:
				return "badge badge-ghost";
		}
	}

	function activityDot(type: string) {
		return type === "success"
			? "text-success"
			: type === "error"
				? "text-error"
				: "text-info";
	}
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl px-5 py-6">
		<!-- Top row (title + refresh meta) -->
		<div class="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
			<div class="space-y-2">
				<div class="flex flex-wrap items-center gap-2">
					<h1 class="text-2xl font-semibold tracking-tight">{worker.name}</h1>
					<span class="badge badge-success gap-2">
            <span class="inline-block h-2 w-2 rounded-full bg-success"></span>
						{worker.status}
          </span>
					<span class="badge badge-ghost">{worker.mode}</span>
				</div>

				<div class="flex flex-wrap items-center gap-3 text-sm opacity-80">
					<div class="flex items-center gap-2">
						<span class="opacity-70">Agent:</span>
						<span class="font-medium">{worker.agentConnection}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Lane:</span>
						<span class="font-medium">{worker.lane}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Assigned:</span>
						<span class="font-medium">{worker.assignedHost}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Created:</span>
						<span class="font-medium">{worker.createdAt}</span>
					</div>
					<div class="flex items-center gap-2">
						<span class="opacity-70">Last seen:</span>
						<span class="font-medium">{worker.lastSeen}</span>
					</div>
				</div>

				<!-- Actions -->
				<div class="flex flex-wrap gap-2 pt-1">
					<button class="btn btn-outline btn-error btn-sm">
						<span class="text-base">⬇</span>
						Drain
					</button>

					<div class="dropdown dropdown-bottom">
						<label tabindex="0" class="btn btn-outline btn-sm">
							Restart
							<span class="ml-1 opacity-70">▾</span>
						</label>
						<ul
							tabindex="0"
							class="dropdown-content menu mt-2 w-44 rounded-box bg-base-100 p-2 shadow"
						>
							<li><button>Soft restart</button></li>
							<li><button>Hard restart</button></li>
						</ul>
					</div>

					<button class="btn btn-outline btn-sm" disabled>
						Disable
						<span class="ml-1 opacity-70">▾</span>
					</button>
				</div>
			</div>

			<div class="flex items-center justify-between gap-3 rounded-box bg-base-100 px-4 py-3 shadow md:min-w-[280px]">
				<div class="text-sm">
					<div class="opacity-70">Last updated</div>
					<div class="font-medium">12s ago</div>
				</div>
				<button class="btn btn-ghost btn-sm">Refresh</button>
			</div>
		</div>

		<!-- Main grid -->
		<div class="mt-6 grid grid-cols-1 gap-4 lg:grid-cols-2">
			<!-- Left column -->
			<div class="space-y-4">
				<!-- Statistics (big) -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Statistics</h2>
							<button class="btn btn-ghost btn-xs">•••</button>
						</div>

						<div class="mt-1 space-y-3">
							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-info">⏱</span>
									<span>Heartbeat Time</span>
								</div>
								<span class="font-medium">{stats.heartbeat}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-info">↗</span>
									<span>In-flight Jobs</span>
								</div>
								<span class="font-medium">{stats.inflight}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-success">▲</span>
									<span>Processed Jobs</span>
								</div>
								<span class="font-medium">{stats.processed.toLocaleString()}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-warning">⚡</span>
									<span>Failed Jobs</span>
								</div>
								<span class="font-medium">{stats.failed.toLocaleString()}</span>
							</div>

							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2 opacity-80">
									<span class="text-error">🩸</span>
									<span>ReadyToDelete</span>
								</div>
								<span class="font-medium">{stats.readyToDelete}</span>
							</div>
						</div>
					</div>
				</div>

				<!-- Recent Buckets -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Recent Buckets</h2>
							<a class="link link-hover text-sm opacity-70" href="/buckets">View all →</a>
						</div>

						<div class="overflow-x-auto">
							<table class="table table-sm">
								<thead>
								<tr>
									<th>ID</th>
									<th>Priority</th>
									<th>Status</th>
									<th>Host</th>
								</tr>
								</thead>
								<tbody>
								{#each recentBuckets as b}
									<tr>
										<td class="font-mono text-xs">{b.id}</td>
										<td>{b.priority}</td>
										<td><span class={badgeForStatus(b.status)}>{b.status}</span></td>
										<td>{b.host}</td>
									</tr>
								{/each}
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>

			<!-- Right column -->
			<div class="space-y-4">
				<!-- Compact statistics (right top) -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Statistics</h2>
							<button class="btn btn-ghost btn-xs">•••</button>
						</div>

						<div class="grid grid-cols-1 gap-3 md:grid-cols-2">
							<div class="rounded-box bg-base-200 p-3">
								<div class="text-xs opacity-70">Heartbeat Time</div>
								<div class="mt-1 font-semibold">{stats.heartbeat}</div>
							</div>

							<div class="rounded-box bg-base-200 p-3">
								<div class="text-xs opacity-70">In-flight Jobs</div>
								<div class="mt-1 flex items-center justify-between">
									<span class="text-info">↗</span>
									<span class="font-semibold">{stats.inflight}</span>
								</div>
							</div>

							<div class="rounded-box bg-base-200 p-3">
								<div class="text-xs opacity-70">Processed Jobs</div>
								<div class="mt-1 flex items-center justify-between">
									<span class="text-success">▲</span>
									<span class="font-semibold">{stats.processed.toLocaleString()}</span>
								</div>
							</div>

							<div class="rounded-box bg-base-200 p-3">
								<div class="text-xs opacity-70">Failed Jobs</div>
								<div class="mt-1 flex items-center justify-between">
									<span class="text-warning">⚡</span>
									<span class="font-semibold">{stats.failed.toLocaleString()}</span>
								</div>
							</div>
						</div>
					</div>
				</div>

				<!-- Configuration -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Configuration</h2>
							<button class="btn btn-ghost btn-xs">•••</button>
						</div>

						<div class="grid grid-cols-1 gap-3 md:grid-cols-2">
							<div class="space-y-3">
								<div class="flex items-center justify-between">
									<div class="opacity-80">Mode</div>
									<div class="font-medium">{config.mode}</div>
								</div>
								<div class="flex items-center justify-between">
									<div class="opacity-80">Lane</div>
									<div class="font-medium">{worker.lane}</div>
								</div>
							</div>

							<div class="space-y-3">
								<div class="flex items-center justify-between">
									<div class="opacity-80">Parallelism Factor</div>
									<div class="font-medium">{config.parallelismFactor.toFixed(1)}</div>
								</div>
								<div class="flex items-center justify-between">
									<div class="opacity-80">Batch Size</div>
									<div class="font-medium">{config.batchSize.toLocaleString()}</div>
								</div>
							</div>
						</div>
					</div>
				</div>

				<!-- Connection -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Connection</h2>
							<a class="link link-hover text-sm opacity-70" href="/agent-connections">View all →</a>
						</div>

						<div class="space-y-3">
							<div class="flex items-center justify-between">
								<div class="opacity-80">Agent Connection</div>
								<div class="font-medium">{connection.agentConnection}</div>
							</div>
							<div class="flex items-center justify-between">
								<div class="opacity-80">Agent Type</div>
								<div class="font-medium">{connection.agentType}</div>
							</div>
							<div class="flex items-center justify-between">
								<div class="opacity-80">Footprint</div>
								<div class="font-medium">{connection.footprint}</div>
							</div>
						</div>
					</div>
				</div>

				<!-- Recent Activity -->
				<div class="card bg-base-100 shadow">
					<div class="card-body">
						<div class="flex items-center justify-between">
							<h2 class="card-title text-base">Recent Activity</h2>
							<a class="link link-hover text-sm opacity-70" href="/activity">View all →</a>
						</div>

						<div class="space-y-3">
							{#each recentActivity as a}
								<div class="flex items-center justify-between rounded-box bg-base-200 p-3">
									<div class="flex items-center gap-2">
                    <span class={activityDot(a.type)}>
                      {a.type === "success" ? "✔" : a.type === "error" ? "✖" : "ℹ"}
                    </span>
										<div class="text-sm">
											<span class="font-semibold">{a.title}</span>
											<span class="opacity-70"> {a.msg}</span>
										</div>
									</div>
									<div class="text-xs opacity-70">{a.when}</div>
								</div>
							{/each}
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
