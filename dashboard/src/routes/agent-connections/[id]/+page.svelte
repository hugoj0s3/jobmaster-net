<!-- +page.svelte (SvelteKit) -->
<script lang="ts">
	type WorkerRow = {
		name: string;
		lane: string;
		laneBadge: "healthy" | "warning" | "useful";
		statusText: string;
		statusBadge: "warning" | "neutral";
		cpu: number;
		mem: number;
		heartbeat: string;
		mode: "Coordinator" | "Full" | "Execution";
	};

	const agentName = "Payroll-Worker-02";
	const clusterName = "QA - Testing";
	const updatedAgo = "15s ago";

	const health = { label: "Warning", tone: "warning" as const };

	const buckets = { used: 14, total: 15, draining: 1 };

	const version = {
		engine: "PostgreSQL",
		ver: "v1.20.0",
		host: "10.0.2.12",
		port: 5432,
		database: "jm_agent_payroll"
	};

	const connectionInfo = {
		worker: "Worker",
		agentId: "PostgreSQL",
		host: "10.0.2.12",
		boundCluster: clusterName,
		state: "Active",
		agentName
	};

	// Less workers attached (compact preview on this page)
	const workers: WorkerRow[] = [
		{
			name: "Payroll-Worker-01",
			lane: "Warning",
			laneBadge: "warning",
			statusText: "—",
			statusBadge: "neutral",
			cpu: 80,
			mem: 85,
			heartbeat: "12s ago",
			mode: "Full"
		},
		{
			name: "Payroll-Worker-03",
			lane: "Useful",
			laneBadge: "useful",
			statusText: "—",
			statusBadge: "neutral",
			cpu: 93,
			mem: 96,
			heartbeat: "11s ago",
			mode: "Execution"
		}
	];

	const workersBound = workers.length;

	const events = [
		{ icon: "⚠️", text: "High memory usage detected", ago: "2m ago" },
		{ icon: "⚠️", text: "H.o.1. Warning · Payroll-Worker-03", ago: "5m ago" }
	];

	function badgeClass(kind: WorkerRow["laneBadge"]) {
		if (kind === "healthy") return "badge badge-success badge-outline";
		if (kind === "warning") return "badge badge-warning badge-outline";
		return "badge badge-info badge-outline";
	}

	function statusBadgeClass(kind: WorkerRow["statusBadge"]) {
		if (kind === "warning") return "badge badge-warning badge-outline";
		return "badge badge-ghost";
	}

	function barValue(n: number) {
		return Math.max(0, Math.min(100, n));
	}
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl px-6 py-6">
		<!-- Header -->
		<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
			<div class="flex flex-col gap-2">
				<div class="text-sm breadcrumbs opacity-70">
					<ul>
						<li><a class="link link-hover">Agent Connections</a></li>
						<li>{agentName}</li>
					</ul>
				</div>

				<div class="flex items-center gap-3">
					<h1 class="text-3xl font-semibold">{agentName}</h1>
				</div>

				<div class="flex flex-wrap items-center gap-2">
					<div class="badge badge-neutral badge-outline">Cluster: {clusterName}</div>
					<div class="badge badge-success badge-outline">ACTIVE</div>
					<div class="badge badge-success">● Connected</div>
				</div>
			</div>

			<div class="flex items-center gap-2">
				<div class="text-sm opacity-70 mr-2">Updated {updatedAgo}</div>
				<button class="btn btn-sm btn-ghost">
					<span class="mr-1">⟲</span> Refresh
				</button>

				<div class="join">
					<button class="btn btn-sm btn-outline join-item">Drain Connection</button>
					<button class="btn btn-sm btn-outline join-item">View Logs</button>
				</div>
			</div>
		</div>

		<!-- Summary cards -->
		<div class="mt-6 grid grid-cols-1 gap-4 md:grid-cols-12">
			<div class="card bg-base-100 shadow md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Health</div>
						<div class="badge badge-warning badge-outline">⚠</div>
					</div>
					<div class="text-2xl font-semibold">{health.label}</div>
					<div class="text-xs opacity-60">Agent reports degraded state</div>
				</div>
			</div>

			<div class="card bg-base-100 shadow md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Workers Bound</div>
						<div class="badge badge-neutral badge-outline">⛭</div>
					</div>
					<div class="text-2xl font-semibold">
						{workersBound} <span class="text-base font-normal opacity-70">workers</span>
					</div>
					<div class="text-xs opacity-60">Attached to this agent</div>
				</div>
			</div>

			<div class="card bg-base-100 shadow md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Buckets</div>
						<div class="badge badge-neutral badge-outline">☰</div>
					</div>
					<div class="text-2xl font-semibold">
						{buckets.used} / {buckets.total}
						<span class="ml-2 text-sm font-normal opacity-70">{buckets.draining} draining</span>
					</div>
					<progress class="progress progress-warning w-full" value={buckets.used} max={buckets.total}></progress>
				</div>
			</div>

			<div class="card bg-base-100 shadow md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Version</div>
						<div class="badge badge-neutral badge-outline">⛁</div>
					</div>
					<div class="flex items-baseline gap-2">
						<div class="text-xl font-semibold">{version.engine}</div>
						<div class="text-sm opacity-70">{version.ver}</div>
					</div>
					<div class="mt-2 space-y-1 text-sm opacity-80">
						<div class="flex justify-between">
							<span class="opacity-70">Host</span><span class="font-mono">{version.host}</span>
						</div>
						<div class="flex justify-between">
							<span class="opacity-70">Port</span><span class="font-mono">{version.port}</span>
						</div>
						<div class="flex justify-between">
							<span class="opacity-70">Database</span><span class="font-mono">{version.database}</span>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Connection info (side-by-side) -->
		<div class="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
			<div class="card bg-base-100 shadow h-full">
				<div class="card-body py-4">
					<div class="card-title text-base opacity-80">Connection Info</div>
					<div class="divider my-2"></div>

					<div class="grid grid-cols-2 gap-3 text-sm">
						<div class="opacity-70">Worker</div>
						<div class="font-medium">{connectionInfo.worker}</div>

						<div class="opacity-70">Agent ID</div>
						<div class="font-medium">{connectionInfo.agentId}</div>

						<div class="opacity-70">Host</div>
						<div class="font-mono">{connectionInfo.host}</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-100 shadow h-full">
				<div class="card-body py-4">
					<div class="card-title text-base opacity-80">Bound Cluster</div>
					<div class="divider my-2"></div>

					<div class="grid grid-cols-2 gap-3 text-sm">
						<div class="opacity-70">Bound Cluster</div>
						<div class="font-medium">{connectionInfo.boundCluster}</div>

						<div class="opacity-70">State</div>
						<div class="font-medium">{connectionInfo.state}</div>

						<div class="opacity-70">Agent Name</div>
						<div class="font-medium">{connectionInfo.agentName}</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Workers attached (compact preview cards, side-by-side) -->
		<div class="mt-6 card bg-base-100 shadow">
			<div class="card-body">
				<div class="flex items-center justify-between">
					<div class="card-title">Workers Attached</div>
					<a class="link link-primary text-sm">View all</a>
				</div>

				<div class="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
					{#each workers.slice(0, 3) as w}
						<div class="rounded-xl border border-base-300 p-4 bg-base-200">
							<div class="flex items-center justify-between gap-3">
								<div class="font-medium truncate">{w.name}</div>
								<span class={badgeClass(w.laneBadge)}>{w.lane}</span>
							</div>

							<div class="mt-2 text-sm opacity-80 flex justify-between">
								<span>Mode</span>
								<span>{w.mode}</span>
							</div>

							<div class="mt-3 space-y-2">
								<div class="flex items-center gap-2">
									<span class="text-xs w-12 opacity-60">CPU</span>
									<progress
										class="progress progress-warning w-full"
										value={barValue(w.cpu)}
										max="100"
									></progress>
									<span class="text-xs w-10 text-right opacity-70">{w.cpu}%</span>
								</div>

								<div class="flex items-center gap-2">
									<span class="text-xs w-12 opacity-60">MEM</span>
									<progress
										class="progress progress-warning w-full"
										value={barValue(w.mem)}
										max="100"
									></progress>
									<span class="text-xs w-10 text-right opacity-70">{w.mem}%</span>
								</div>
							</div>

							<div class="mt-3 flex items-center justify-between text-xs opacity-60">
								<span>Heartbeat</span>
								<span>{w.heartbeat}</span>
							</div>

							<div class="mt-2">
								<span class={statusBadgeClass(w.statusBadge)}>{w.statusText}</span>
							</div>
						</div>
					{/each}
				</div>

				{#if workers.length > 3}
					<div class="mt-3 text-sm opacity-60">+ {workers.length - 3} more workers</div>
				{/if}
			</div>
		</div>

		<!-- Recent events -->
		<div class="mt-6 card bg-base-100 shadow">
			<div class="card-body">
				<div class="card-title">Recent Events</div>
				<div class="divider my-2"></div>

				<div class="space-y-2">
					{#each events as e}
						<div class="flex items-center justify-between rounded-xl bg-base-200 px-4 py-3">
							<div class="flex items-center gap-3">
								<span class="text-lg">{e.icon}</span>
								<span class="opacity-90">{e.text}</span>
							</div>
							<div class="text-sm opacity-70">{e.ago}</div>
						</div>
					{/each}
				</div>
			</div>
		</div>
	</div>
</div>
