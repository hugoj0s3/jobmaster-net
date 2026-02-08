<!-- src/routes/hosts/[id]/+page.svelte -->
<script lang="ts">
	// Mock data (replace with your API data)
	const hostName = "Payroll-Worker-01";
	const ip = "192.168.1.13";
	const ram = "8 GB";
	const lastUpdated = "7s ago";

	const status = {
		label: "Offline",
		dotClass: "bg-error",
		badgeClass: "badge badge-error badge-outline",
		sub: "Last Heartbeat",
		value: "1m 34s ago"
	};

	const kpis = [
		{
			title: "CPU Load",
			value: "52%",
			sub: "3.2 cores / 4",
			class: "bg-base-200/60 border-base-300"
		},
		{
			title: "Memory Usage",
			value: "66%",
			sub: "5.3 GB / 8GB",
			class: "bg-base-200/60 border-base-300"
		},
		{
			title: "Load Average",
			value: "3.5",
			sub: "3.5 (1m) / 3.2 (5m) / 1.6 (15m)",
			class: "bg-base-200/60 border-base-300"
		}
	];

	const workers = [
		{
			name: "DNS-Worker-01",
			id: "b3...516",
			lastHeartbeat: "18s ago",
			pf: 1.0,
			lane: "Full",
			status: "Offline"
		},
		{
			name: "Payroll-Worker-02",
			id: "b1...2b4",
			lastHeartbeat: "1m 34s ago",
			pf: 1.0,
			lane: "Payroll",
			status: "Offline"
		},
		{
			name: "Log-Worker-01",
			id: "37...aa8",
			lastHeartbeat: "1m 4s ago",
			pf: 1.0,
			lane: "Fulfill",
			status: "Offline"
		}
	];

	let tab: "metrics" | "workers" = "metrics";
	const ranges = ["1m", "15m", "1h", "8h", "24h"];
	let selectedRange = "1h";

	let autoRefresh = false;
</script>

<!-- Page background -->
<div class="min-h-screen w-full bg-base-100">
	<!-- Top spacing container (no navbar/sidebar) -->
	<div class="mx-auto w-full max-w-6xl px-6 py-8">
		<!-- Header row -->
		<div class="flex flex-col gap-4">
			<div class="flex items-start justify-between gap-4">
				<div class="space-y-2">
					<div class="text-sm breadcrumbs">
						<ul>
							<li><a class="link link-hover">Hosts</a></li>
							<li>{hostName}</li>
						</ul>
					</div>

					<h1 class="text-3xl font-semibold tracking-tight">
						Host Details - {hostName}
					</h1>

					<div class="flex flex-wrap items-center gap-3 text-sm text-base-content/70">
            <span class="inline-flex items-center gap-2">
              <span class="inline-block h-2 w-2 rounded-full {status.dotClass}"></span>
              <span class={status.badgeClass}>{status.label}</span>
            </span>
						<span class="opacity-60">•</span>
						<span>{ip}</span>
						<span class="opacity-60">•</span>
						<span>{ram}</span>
					</div>
				</div>

				<div class="flex flex-col items-end gap-2">
					<div class="flex items-center gap-2 text-sm text-base-content/60">
						<span>Last updated: {lastUpdated}</span>
						<button class="btn btn-ghost btn-sm">Refresh</button>
						<button class="btn btn-ghost btn-sm btn-square" aria-label="settings">
							<!-- gear -->
							<svg width="18" height="18" viewBox="0 0 24 24" fill="none" class="opacity-80">
								<path
									d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"
									stroke="currentColor"
									stroke-width="1.5"
								/>
								<path
									d="M19.4 15a8.4 8.4 0 0 0 .1-2l2-1.2-2-3.4-2.2.7a7.9 7.9 0 0 0-1.7-1l-.3-2.3H9.7L9.4 8a7.9 7.9 0 0 0-1.7 1l-2.2-.7-2 3.4 2 1.2a8.4 8.4 0 0 0 0 2l-2 1.2 2 3.4 2.2-.7c.5.4 1.1.7 1.7 1l.3 2.3h5.6l.3-2.3c.6-.3 1.2-.6 1.7-1l2.2.7 2-3.4-2-1.2Z"
									stroke="currentColor"
									stroke-width="1.5"
									stroke-linejoin="round"
								/>
							</svg>
						</button>
					</div>

					<label class="label cursor-pointer gap-3">
						<span class="label-text text-sm text-base-content/60">Auto-refresh</span>
						<input type="checkbox" class="toggle toggle-sm" bind:checked={autoRefresh} />
					</label>
				</div>
			</div>

			<!-- KPI cards row -->
			<div class="grid grid-cols-1 gap-4 md:grid-cols-4">
				<!-- Status card -->
				<div class="card border border-base-300 bg-base-200/60">
					<div class="card-body gap-3">
						<div class="flex items-center justify-between">
							<div class="text-sm font-semibold text-base-content/80">{status.label}</div>
							<div class="text-error">
								<!-- lightning -->
								<svg width="26" height="26" viewBox="0 0 24 24" fill="none">
									<path
										d="M13 2 3 14h7l-1 8 12-14h-7l-1-6Z"
										stroke="currentColor"
										stroke-width="1.5"
										stroke-linejoin="round"
									/>
								</svg>
							</div>
						</div>

						<div class="text-3xl font-semibold">{status.value}</div>
						<div class="text-sm text-base-content/60">{status.sub}</div>
					</div>
				</div>

				{#each kpis as k}
					<div class={"card border " + k.class}>
						<div class="card-body gap-3">
							<div class="flex items-center justify-between">
								<div class="text-sm font-semibold text-base-content/80">{k.title}</div>
								<div class="opacity-70">
									<!-- simple icon -->
									<svg width="26" height="26" viewBox="0 0 24 24" fill="none">
										<path
											d="M4 7h16M4 12h16M4 17h16"
											stroke="currentColor"
											stroke-width="1.5"
											stroke-linecap="round"
										/>
									</svg>
								</div>
							</div>

							<div class="text-3xl font-semibold">{k.value}</div>
							<div class="text-sm text-base-content/60">{k.sub}</div>
						</div>
					</div>
				{/each}
			</div>

			<!-- Tabs + actions -->
			<div class="flex flex-col gap-3 pt-2 md:flex-row md:items-center md:justify-between">
				<div role="tablist" class="tabs tabs-bordered">
					<button
						role="tab"
						class={"tab " + (tab === "metrics" ? "tab-active" : "")}
						on:click={() => (tab = "metrics")}
					>
						Metrics
					</button>
					<button
						role="tab"
						class={"tab " + (tab === "workers" ? "tab-active" : "")}
						on:click={() => (tab = "workers")}
					>
						Workers Assigned
					</button>
				</div>

				<div class="flex items-center gap-2">
					<details class="dropdown dropdown-end">
						<summary class="btn btn-outline btn-sm">Actions</summary>
						<ul class="menu dropdown-content z-[1] mt-2 w-52 rounded-box bg-base-200 p-2 shadow">
							<li><a>Restart host</a></li>
							<li><a>Mark as drained</a></li>
							<li><a>View logs</a></li>
						</ul>
					</details>

					<button class="btn btn-ghost btn-sm btn-square" aria-label="panel settings">
						<svg width="18" height="18" viewBox="0 0 24 24" fill="none" class="opacity-80">
							<path
								d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z"
								stroke="currentColor"
								stroke-width="1.5"
							/>
							<path
								d="M19.4 15a8.4 8.4 0 0 0 .1-2l2-1.2-2-3.4-2.2.7a7.9 7.9 0 0 0-1.7-1l-.3-2.3H9.7L9.4 8a7.9 7.9 0 0 0-1.7 1l-2.2-.7-2 3.4 2 1.2a8.4 8.4 0 0 0 0 2l-2 1.2 2 3.4 2.2-.7c.5.4 1.1.7 1.7 1l.3 2.3h5.6l.3-2.3c.6-.3 1.2-.6 1.7-1l2.2.7 2-3.4-2-1.2Z"
								stroke="currentColor"
								stroke-width="1.5"
								stroke-linejoin="round"
							/>
						</svg>
					</button>
				</div>
			</div>
		</div>

		<!-- Content -->
		{#if tab === "metrics"}
			<div class="mt-6 space-y-6">
				<!-- Charts row -->
				<div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
					<div class="card border border-base-300 bg-base-200/40">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<h2 class="card-title text-base">CPU Usage</h2>

								<div class="join">
									{#each ranges as r}
										<button
											class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
											on:click={() => (selectedRange = r)}
										>
											{r}
										</button>
									{/each}
								</div>
							</div>

							<!-- Placeholder chart -->
							<div class="mt-4 h-40 rounded-xl border border-base-300 bg-base-100/40">
								<div class="flex h-full items-center justify-center text-sm text-base-content/50">
									Chart placeholder ({selectedRange})
								</div>
							</div>

							<div class="mt-4 flex flex-wrap gap-4 text-sm">
                <span class="inline-flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-success"></span> Online
                </span>
								<span class="inline-flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-warning"></span> Warning
                </span>
								<span class="inline-flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-error"></span> Offline
                </span>
							</div>
						</div>
					</div>

					<div class="card border border-base-300 bg-base-200/40">
						<div class="card-body">
							<div class="flex items-center justify-between">
								<h2 class="card-title text-base">Memory Usage</h2>

								<div class="join">
									{#each ranges as r}
										<button
											class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
											on:click={() => (selectedRange = r)}
										>
											{r}
										</button>
									{/each}
								</div>
							</div>

							<!-- Placeholder chart -->
							<div class="mt-4 h-40 rounded-xl border border-base-300 bg-base-100/40">
								<div class="flex h-full items-center justify-center text-sm text-base-content/50">
									Chart placeholder ({selectedRange})
								</div>
							</div>

							<div class="mt-4 flex flex-wrap gap-4 text-sm text-base-content/70">
                <span class="inline-flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-warning"></span> File I/O
                </span>
								<span class="inline-flex items-center gap-2">
                  <span class="h-2 w-2 rounded-full bg-info"></span> Disk I/O (KB/s)
                </span>
							</div>
						</div>
					</div>
				</div>

				<!-- Workers table -->
				<div class="card border border-base-300 bg-base-200/40">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
							<button class="btn btn-link btn-sm">View all Workers →</button>
						</div>

						<div class="overflow-x-auto">
							<table class="table">
								<thead>
								<tr class="text-base-content/70">
									<th>Worker</th>
									<th>ID</th>
									<th>Last Heartbeat</th>
									<th>Parallelism Factor</th>
									<th>Lane</th>
									<th class="text-right">Status</th>
								</tr>
								</thead>
								<tbody>
								{#each workers as w}
									<tr>
										<td class="font-medium">
											<div class="flex items-center gap-3">
												<span class="h-2 w-2 rounded-full bg-success opacity-80"></span>
												{w.name}
											</div>
										</td>
										<td class="font-mono text-sm opacity-80">{w.id}</td>
										<td class="opacity-80">{w.lastHeartbeat}</td>
										<td>
											<div class="flex items-center gap-3">
												<span class="opacity-80">{w.pf.toFixed(1)}</span>
												<progress class="progress progress-primary w-28" value="15" max="100"></progress>
											</div>
										</td>
										<td class="opacity-80">{w.lane}</td>
										<td class="text-right">
											<span class="badge badge-error badge-outline">{w.status}</span>
										</td>
									</tr>
								{/each}
								</tbody>
							</table>
						</div>

						<div class="mt-2 text-sm text-base-content/60">
							<button class="btn btn-ghost btn-sm">View all Workers</button>
						</div>
					</div>
				</div>
			</div>
		{:else}
			<!-- Workers tab (simple view) -->
			<div class="mt-6">
				<div class="card border border-base-300 bg-base-200/40">
					<div class="card-body">
						<div class="flex items-center justify-between gap-3">
							<h2 class="card-title text-base">Workers Assigned</h2>
							<div class="join">
								{#each ranges as r}
									<button
										class={"btn btn-xs join-item " + (selectedRange === r ? "btn-active" : "btn-ghost")}
										on:click={() => (selectedRange = r)}
									>
										{r}
									</button>
								{/each}
							</div>
						</div>

						<div class="overflow-x-auto">
							<table class="table">
								<thead>
								<tr class="text-base-content/70">
									<th>Worker</th>
									<th>ID</th>
									<th>Last Heartbeat</th>
									<th>Parallelism Factor</th>
									<th>Lane</th>
									<th class="text-right">Status</th>
								</tr>
								</thead>
								<tbody>
								{#each workers as w}
									<tr>
										<td class="font-medium">{w.name}</td>
										<td class="font-mono text-sm opacity-80">{w.id}</td>
										<td class="opacity-80">{w.lastHeartbeat}</td>
										<td class="opacity-80">{w.pf.toFixed(1)}</td>
										<td class="opacity-80">{w.lane}</td>
										<td class="text-right">
											<span class="badge badge-error badge-outline">{w.status}</span>
										</td>
									</tr>
								{/each}
								</tbody>
							</table>
						</div>

						<div class="mt-2 flex justify-end">
							<button class="btn btn-link btn-sm">View all Workers →</button>
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>

<style>
    /* Optional: subtle "glow" vibe similar to the screenshot */
    :global(body) {
        background: radial-gradient(1200px 600px at 70% 0%, rgba(120, 90, 255, 0.18), transparent 60%),
        radial-gradient(900px 500px at 20% 20%, rgba(0, 180, 255, 0.12), transparent 55%),
        radial-gradient(900px 600px at 80% 60%, rgba(255, 120, 180, 0.10), transparent 55%),
        hsl(var(--b1));
    }
</style>
