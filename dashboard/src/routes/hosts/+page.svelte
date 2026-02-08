<script lang="ts">
	type HostStatus = "Online" | "Offline" | "Warning";

	type HostRow = {
		status: HostStatus;
		host: string;
		ip: string;
		cpu: number; // %
		memPercent?: number; // %
		memGb?: number; // GB
		workers?: number;
		uptime?: string;
	};

	const rows: HostRow[] = [
		{ status: "Online", host: "DNS-Worker-01",        ip: "192.168.1.10", cpu: 5,  memPercent: 74, memGb: 3.7, workers: 2, uptime: "5d 19h" },
		{ status: "Online", host: "FileImport-Worker-01", ip: "192.168.1.11", cpu: 8,  memPercent: 81, memGb: 4.0, workers: 3, uptime: "2d 12h" },
		{ status: "Online", host: "FileImport-Worker-02", ip: "192.168.1.12", cpu: 17, memPercent: 66, memGb: 3.2, workers: 3, uptime: "12d 4h" },
		{ status: "Online", host: "Payroll-Worker-01",    ip: "192.168.1.13", cpu: 53, memPercent: 65, memGb: 2.8, workers: 2, uptime: "15h 21m" },
		{ status: "Online", host: "Payroll-Worker-02",    ip: "192.168.1.14", cpu: 59, memPercent: 69, memGb: 3.0, workers: 2, uptime: "8d 17h" },
		{ status: "Online", host: "Deployment-Worker-01", ip: "192.168.1.21", cpu: 12, memPercent: 44, memGb: 1.8, workers: 1, uptime: "24h 1m" },
		{ status: "Online", host: "Log-Worker-01",        ip: "192.168.1.22", cpu: 6,  memPercent: 88, memGb: 3.5, workers: 2, uptime: "—" },
		// Offline (failed)
		{ status: "Offline", host: "DNS-Worker-02",       ip: "192.168.1.20", cpu: 0 },
	];

	let activeTab: "All" | "Online" | "Offline" = "All";
	let q = "";
	let sortBy: "host" | "cpu" | "mem" = "host";
	let sortDir: "asc" | "desc" = "asc";

	const onlineCount = rows.filter(r => r.status === "Online").length;
	const offlineCount = rows.filter(r => r.status === "Offline").length;

	$: avgCpu =
		Math.round(
			rows.filter(r => r.status !== "Offline").reduce((acc, r) => acc + (r.cpu ?? 0), 0) /
			Math.max(1, rows.filter(r => r.status !== "Offline").length)
		);

	$: avgMem =
		Math.round(
			rows
				.filter(r => r.status !== "Offline" && typeof r.memPercent === "number")
				.reduce((acc, r) => acc + (r.memPercent ?? 0), 0) /
			Math.max(1, rows.filter(r => r.status !== "Offline" && typeof r.memPercent === "number").length)
		);

	function tabFilter(r: HostRow) {
		if (activeTab === "All") return true;
		return r.status === activeTab;
	}

	function textFilter(r: HostRow) {
		const s = `${r.host} ${r.ip} ${r.status}`.toLowerCase();
		return s.includes(q.trim().toLowerCase());
	}

	function sortValue(r: HostRow) {
		if (sortBy === "host") return r.host.toLowerCase();
		if (sortBy === "cpu") return r.cpu ?? 0;
		// mem
		return r.memPercent ?? -1;
	}

	$: filtered = rows
		.filter(tabFilter)
		.filter(textFilter)
		.sort((a, b) => {
			const av = sortValue(a);
			const bv = sortValue(b);
			const cmp = av < bv ? -1 : av > bv ? 1 : 0;
			return sortDir === "asc" ? cmp : -cmp;
		});

	// Fake "Last updated"
	let lastUpdated = "10s ago";
	function refresh() {
		lastUpdated = "just now";
		setTimeout(() => (lastUpdated = "10s ago"), 1200);
	}

	function badgeColor(status: HostStatus) {
		if (status === "Online") return "badge-success";
		if (status === "Warning") return "badge-warning";
		return "badge-error";
	}

	function dotClass(status: HostStatus) {
		if (status === "Online") return "bg-success";
		if (status === "Warning") return "bg-warning";
		return "bg-error";
	}
</script>

<div class="min-h-screen bg-base-300 text-base-content">
	<div class="max-w-7xl mx-auto px-6 py-8">
		<!-- Header -->
		<div class="flex items-start justify-between gap-4">
			<div>
				<h1 class="text-3xl font-semibold tracking-tight">Hosts</h1>
			</div>

			<div class="flex items-center gap-3">
				<div class="text-sm opacity-70">Last updated: <span class="font-medium opacity-100">{lastUpdated}</span></div>
				<button class="btn btn-ghost btn-sm" on:click={refresh} title="Refresh">
          <span class="inline-flex items-center gap-2">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M21 12a9 9 0 1 1-3-6.7"/><path d="M21 3v7h-7"/>
            </svg>
            Refresh
          </span>
				</button>
				<button class="btn btn-ghost btn-sm" title="Settings">
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<path d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7z"/>
						<path d="M19.4 15a7.97 7.97 0 0 0 .1-2l2-1.5-2-3.5-2.4.5a7.8 7.8 0 0 0-1.7-1L14.8 3h-5.6L8.6 7.5a7.8 7.8 0 0 0-1.7 1L4.5 8l-2 3.5 2 1.5a7.97 7.97 0 0 0 .1 2l-2 1.5 2 3.5 2.4-.5a7.8 7.8 0 0 0 1.7 1L9.2 21h5.6l.6-4.5a7.8 7.8 0 0 0 1.7-1l2.4.5 2-3.5-2-1.5z"/>
					</svg>
				</button>
			</div>
		</div>

		<!-- KPI Cards -->
		<div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mt-6">
			<div class="card bg-base-200 shadow">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<div class="text-sm opacity-70">Hosts Online</div>
							<div class="text-4xl font-semibold mt-1">{onlineCount}</div>
							<div class="text-sm opacity-70 mt-1">Hosts Online</div>
						</div>
						<div class="rounded-2xl bg-success/15 p-3 text-success">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M20 6 9 17l-5-5"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200 shadow">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<div class="text-sm opacity-70">Hosts Offline</div>
							<div class="text-4xl font-semibold mt-1">{offlineCount}</div>
							<div class="text-sm opacity-70 mt-1">Hosts Offline</div>
						</div>

						<!-- FAILED icon (no lightning) -->
						<div class="rounded-2xl bg-error/15 p-3 text-error" title="Failed/Offline">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<circle cx="12" cy="12" r="9"/>
								<path d="M15 9l-6 6M9 9l6 6"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200 shadow">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<div class="text-sm opacity-70">Avg. CPU Usage</div>
							<div class="text-4xl font-semibold mt-1">{avgCpu}%</div>
							<div class="text-sm opacity-70 mt-1">Avg. CPU Usage</div>
						</div>
						<div class="rounded-2xl bg-secondary/15 p-3 text-secondary">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M8 2h8v4H8z"/><path d="M6 6h12v16H6z"/><path d="M9 10h6M9 14h6M9 18h6"/>
							</svg>
						</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200 shadow">
				<div class="card-body">
					<div class="flex items-start justify-between">
						<div>
							<div class="text-sm opacity-70">Avg. Memory Usage</div>
							<div class="text-4xl font-semibold mt-1">{avgMem}%</div>
							<div class="text-sm opacity-70 mt-1">Avg. Memory Usage</div>
						</div>
						<div class="rounded-2xl bg-info/15 p-3 text-info">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-7 w-7" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M7 7h10v10H7z"/><path d="M4 10h3M17 10h3M4 14h3M17 14h3M10 4v3M14 4v3M10 17v3M14 17v3"/>
							</svg>
						</div>
					</div>
				</div>
			</div>
		</div>

		<!-- Filters row -->
		<div class="card bg-base-200 shadow mt-6">
			<div class="card-body gap-4">
				<div class="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
					<!-- Tabs -->
					<div class="tabs tabs-bordered">
						<button class:tab-active={activeTab === "All"} class="tab" on:click={() => (activeTab = "All")}>
							All <span class="ml-2 badge badge-ghost">{rows.length}</span>
						</button>
						<button class:tab-active={activeTab === "Online"} class="tab" on:click={() => (activeTab = "Online")}>
							Online <span class="ml-2 badge badge-success">{onlineCount}</span>
						</button>
						<button class:tab-active={activeTab === "Offline"} class="tab" on:click={() => (activeTab = "Offline")}>
							Offline <span class="ml-2 badge badge-error">{offlineCount}</span>
						</button>
					</div>

					<!-- Search + Sort -->
					<div class="flex flex-col sm:flex-row gap-3 sm:items-center sm:justify-end w-full lg:w-auto">
						<label class="input input-bordered flex items-center gap-2 w-full sm:w-[340px]">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-60" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>
							</svg>
							<input class="grow" placeholder="Search Hosts" bind:value={q} />
						</label>

						<div class="join">
							<select class="select select-bordered join-item" bind:value={sortBy} aria-label="Sort field">
								<option value="host">Sort: Host</option>
								<option value="cpu">Sort: CPU</option>
								<option value="mem">Sort: Memory</option>
							</select>
							<button
								class="btn btn-bordered join-item"
								on:click={() => (sortDir = sortDir === "asc" ? "desc" : "asc")}
								title="Toggle sort direction"
							>
								{sortDir === "asc" ? "A→Z" : "Z→A"}
							</button>
						</div>
					</div>
				</div>

				<!-- Table -->
				<div class="overflow-x-auto">
					<table class="table table-zebra">
						<thead>
						<tr>
							<th>Status</th>
							<th>Host</th>
							<th>IP Address</th>
							<th>CPU Load</th>
							<th>Memory Usage</th>
							<th>Workers</th>
							<th>Uptime</th>
						</tr>
						</thead>
						<tbody>
						{#each filtered as r (r.host)}
							<tr class={r.status === "Offline" ? "opacity-80" : ""}>
								<td>
									<div class="flex items-center gap-2">
										<span class={`inline-block h-2.5 w-2.5 rounded-full ${dotClass(r.status)}`} />
										<span class={`badge badge-outline ${badgeColor(r.status)}`}>{r.status}</span>
									</div>
								</td>

								<td class="font-medium">{r.host}</td>
								<td class="opacity-80">{r.ip}</td>

								<td>
									{#if r.status === "Offline"}
										<span class="opacity-60">—</span>
									{:else}
										<div class="flex items-center gap-3">
											<progress class="progress progress-success w-28" value={r.cpu} max="100" />
											<span class="tabular-nums">{r.cpu}%</span>
										</div>
									{/if}
								</td>

								<td>
									{#if r.status === "Offline" || r.memPercent == null}
										<span class="opacity-60">—</span>
									{:else}
										<div class="flex items-center gap-3">
											<progress class="progress progress-info w-28" value={r.memPercent} max="100" />
											<span class="tabular-nums">{r.memPercent}% ({r.memGb} GB)</span>
										</div>
									{/if}
								</td>

								<td class="tabular-nums">
									{#if r.status === "Offline"}
										<span class="badge badge-ghost">Offline</span>
									{:else}
										{r.workers}
									{/if}
								</td>

								<td class="tabular-nums">
									{#if r.status === "Offline"}
										<span class="opacity-60">Offline</span>
									{:else}
										{r.uptime}
									{/if}
								</td>
							</tr>
						{/each}
						</tbody>
					</table>
				</div>

				<!-- Footer -->
				<div class="flex flex-col md:flex-row md:items-center md:justify-between gap-3 pt-2">
					<div class="flex items-center gap-6 text-sm opacity-80">
						<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-success"></span> Online</div>
						<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-warning"></span> Warning</div>
						<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full bg-error"></span> Offline</div>
					</div>

					<div class="flex items-center justify-between md:justify-end gap-4">
						<div class="text-sm opacity-70">Displaying {filtered.length} hosts</div>

						<div class="join">
							<button class="btn btn-sm join-item">Previous</button>
							<button class="btn btn-sm btn-active join-item">1</button>
							<button class="btn btn-sm join-item">Next</button>
						</div>

						<select class="select select-bordered select-sm">
							<option>10 rows</option>
							<option>25 rows</option>
							<option>50 rows</option>
						</select>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
