<script lang="ts">
	type WorkerStatus = "Online" | "Offline" | "Failed";

	type WorkerRow = {
		name: string;
		status: WorkerStatus;
		role: string;
		lane: string;
		ip?: string;
		cpu: number; // %
		ram: number; // %
		ramText: string; // e.g. "64% (3.2 GB)"
		jobs: number;
		uptime: string;
	};

	let lastUpdated = "12s ago";

	const stats = {
		online: 4,
		offline: 0,
		failed: 1,
		avgCpu: 23,
		avgMem: 70
	};

	type Tab = "All" | "Online" | "Offline" | "Failed";
	let tab: Tab = "All";
	let query = "";
	let sortBy: "Host" | "CPU" | "Memory" | "Jobs" = "Host";
	let asc = true;

	const rows: WorkerRow[] = [
		{
			name: "Deployment-Worker-01",
			status: "Online",
			role: "Full",
			lane: "Default",
			ip: "192.168.1.21",
			cpu: 12,
			ram: 44,
			ramText: "44% (1.8 GB)",
			jobs: 1,
			uptime: "24h 1m"
		},
		{
			name: "DNS-Worker-01",
			status: "Online",
			role: "Full",
			lane: "Default",
			ip: "192.168.1.10",
			cpu: 5,
			ram: 74,
			ramText: "74% (3.7 GB)",
			jobs: 2,
			uptime: "5d 19h"
		},
		{
			name: "DNS-Worker-02",
			status: "Failed",
			role: "Payroll",
			lane: "Payroll",
			ip: "192.168.1.20",
			cpu: 0,
			ram: 0,
			ramText: "—",
			jobs: 0,
			uptime: "Offline"
		},
		{
			name: "FileImport-Worker-01",
			status: "Online",
			role: "FileImport",
			lane: "FileImport",
			ip: "192.168.1.11",
			cpu: 8,
			ram: 81,
			ramText: "81% (4 GB)",
			jobs: 3,
			uptime: "2d 12h"
		}
	];

	const tabCount = (t: Tab) => {
		if (t === "All") return rows.length;
		return rows.filter((r) => r.status === t).length;
	};

	const statusDot = (s: WorkerStatus) => {
		if (s === "Online") return "bg-success";
		if (s === "Offline") return "bg-warning";
		return "bg-error";
	};

	const statusPill = (s: WorkerStatus) => {
		if (s === "Online") return "badge badge-outline badge-success rounded-full px-4 py-3";
		if (s === "Offline") return "badge badge-outline badge-warning rounded-full px-4 py-3";
		return "badge badge-outline badge-error rounded-full px-4 py-3";
	};

	const cpuBarClass = (s: WorkerStatus) => {
		if (s === "Online") return "progress progress-success";
		return "progress";
	};

	const memBarClass = (s: WorkerStatus) => {
		if (s === "Online") return "progress progress-info";
		return "progress";
	};

	$: filtered = rows
		.filter((r) => {
			if (tab !== "All" && r.status !== tab) return false;
			const q = query.trim().toLowerCase();
			if (!q) return true;
			return (
				r.name.toLowerCase().includes(q) ||
				r.role.toLowerCase().includes(q) ||
				r.lane.toLowerCase().includes(q) ||
				(r.ip ?? "").toLowerCase().includes(q)
			);
		})
		.sort((a, b) => {
			const dir = asc ? 1 : -1;
			const cmpStr = (x: string, y: string) => x.localeCompare(y) * dir;
			const cmpNum = (x: number, y: number) => (x - y) * dir;

			if (sortBy === "Host") return cmpStr(a.name, b.name);
			if (sortBy === "CPU") return cmpNum(a.cpu, b.cpu);
			if (sortBy === "Memory") return cmpNum(a.ram, b.ram);
			return cmpNum(a.jobs, b.jobs);
		});

	function refresh() {
		lastUpdated = "just now";
	}
</script>

<div class="min-h-screen w-full bg-base-100 text-base-content">
	<div class="pointer-events-none fixed inset-0 opacity-60" />

	<main class="relative mx-auto max-w-6xl px-8 py-10">
		<div class="flex items-start justify-between gap-4">
			<h1 class="text-5xl font-bold tracking-tight text-base-content">Workers</h1>

			<div class="flex items-center gap-4 text-sm text-base-content/60">
				<span>Last updated: <span class="text-base-content/80">{lastUpdated}</span></span>
				<button
					class="btn btn-ghost btn-sm text-base-content/80 hover:text-base-content"
					on:click={refresh}
				>
					⟳ <span class="ml-1 font-semibold">Refresh</span>
				</button>
				<button
					class="btn btn-ghost btn-sm text-base-content/80 hover:text-base-content"
					aria-label="Settings"
				>
					⚙
				</button>
			</div>
		</div>

		<section class="mt-10 grid grid-cols-1 gap-6 md:grid-cols-4">
			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Workers Online</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{stats.online}</p>
							<p class="mt-2 text-base-content/40">Workers Online</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-success/20 grid place-items-center text-success text-2xl">
							✓
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Workers Offline</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{stats.offline}</p>
							<p class="mt-2 text-base-content/40">Workers Offline</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-error/20 grid place-items-center text-error text-2xl">
							⨯
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Avg. CPU Usage</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{stats.avgCpu}%</p>
							<p class="mt-2 text-base-content/40">Avg. CPU Usage</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-secondary/20 grid place-items-center text-secondary text-2xl">
							▮
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<p class="text-base-content/70">Avg. Memory Usage</p>
							<p class="mt-2 text-5xl font-extrabold text-base-content">{stats.avgMem}%</p>
							<p class="mt-2 text-base-content/40">Avg. Memory Usage</p>
						</div>
						<div class="h-14 w-14 rounded-2xl bg-info/20 grid place-items-center text-info text-2xl">
							▦
						</div>
					</div>
				</div>
			</div>
		</section>

		<section class="mt-8 card bg-base-200/60 border border-base-300/60 rounded-2xl shadow-lg">
			<div class="card-body">
				<div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
					<div class="flex items-center gap-10">
						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "All"}
							on:click={() => (tab = "All")}
						>
							All
							<span class="ml-3 badge rounded-full bg-base-300/50 border-base-300/60 text-base-content/80"
							>{tabCount("All")}</span
							>
						</button>

						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "Online"}
							on:click={() => (tab = "Online")}
						>
							Online
							<span class="ml-3 badge rounded-full bg-success text-black border-0">{tabCount("Online")}</span>
						</button>

						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "Offline"}
							on:click={() => (tab = "Offline")}
						>
							Offline
							<span class="ml-3 badge rounded-full bg-error text-black border-0">{tabCount("Offline")}</span>
						</button>

						<button
							class="btn btn-ghost px-0 text-base-content/70 hover:text-base-content"
							class:font-semibold={tab === "Failed"}
							on:click={() => (tab = "Failed")}
						>
							Failed
							<span class="ml-3 badge rounded-full bg-error text-black border-0">{tabCount("Failed")}</span>
						</button>
					</div>

					<div class="flex w-full flex-col gap-3 sm:flex-row sm:items-center sm:justify-end">
						<label
							class="input input-bordered bg-transparent border-base-300/60 text-base-content w-full sm:w-[420px] rounded-xl"
						>
							<span class="opacity-60">🔎</span>
							<input class="placeholder:text-base-content/40" placeholder="Search Workers" bind:value={query} />
						</label>

						<div class="join">
							<button class="btn join-item bg-transparent border-base-300/60 text-base-content/80 rounded-xl">
								Sort: {sortBy}
							</button>

							<details class="dropdown dropdown-end join-item">
								<summary class="btn bg-transparent border-base-300/60 text-base-content/80 rounded-xl">▾</summary>
								<ul class="menu dropdown-content mt-2 w-44 rounded-xl bg-base-200 border border-base-300 shadow">
									<li><button on:click={() => (sortBy = "Host")}>Host</button></li>
									<li><button on:click={() => (sortBy = "CPU")}>CPU</button></li>
									<li><button on:click={() => (sortBy = "Memory")}>Memory</button></li>
									<li><button on:click={() => (sortBy = "Jobs")}>Jobs</button></li>
								</ul>
							</details>
						</div>

						<button
							class="btn bg-transparent border-base-300/60 text-base-content/80 rounded-xl"
							on:click={() => (asc = !asc)}
							title="Toggle sort direction"
						>
							{asc ? "A→Z" : "Z→A"}
						</button>
					</div>
				</div>

				<div class="divider my-3 opacity-30" />

				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/60">
							<th>Status</th>
							<th>Worker</th>
							<th>IP Address</th>
							<th>CPU Load</th>
							<th>Memory Usage</th>
							<th class="text-right">Jobs</th>
							<th>Uptime</th>
						</tr>
						</thead>

						<tbody>
						{#each filtered as r (r.name)}
							<tr class="hover:bg-base-300/30">
								<td>
									<div class="flex items-center gap-3">
										<span class={"h-2.5 w-2.5 rounded-full " + statusDot(r.status)} />
										<span class={statusPill(r.status)}>{r.status}</span>
									</div>
								</td>

								<td class="text-base-content font-medium">{r.name}</td>

								<td class="text-base-content/70">{r.ip ?? "—"}</td>

								<td>
									{#if r.status === "Online"}
										<div class="flex items-center gap-4">
											<progress class={cpuBarClass(r.status)} value={r.cpu} max="100" />
											<span class="w-12 text-base-content/80">{r.cpu}%</span>
										</div>
									{:else}
										<span class="text-base-content/40">—</span>
									{/if}
								</td>

								<td>
									{#if r.status === "Online"}
										<div class="flex items-center gap-4">
											<progress class={memBarClass(r.status)} value={r.ram} max="100" />
											<span class="text-base-content/80">{r.ramText}</span>
										</div>
									{:else}
										<span class="text-base-content/40">—</span>
									{/if}
								</td>

								<td class="text-right text-base-content/80">{r.jobs}</td>
								<td class="text-base-content/70">{r.uptime}</td>
							</tr>
						{/each}

						{#if filtered.length === 0}
							<tr>
								<td colspan="7" class="py-10 text-base-content/60">No workers found.</td>
							</tr>
						{/if}
						</tbody>
					</table>
				</div>
			</div>
		</section>
	</main>
</div>