<script lang="ts">
	type ScheduleStatus = "Succeeded" | "Failed" | "Paused";

	type RecurringScheduleRow = {
		jobType: string;
		handler: string;
		description: string;
		frequency: string;
		tz?: string;
		nextRun: string;
		lastStatus: ScheduleStatus;
		lastStatusAgo: string;
	};

	// ---- table data ----
	const rows: RecurringScheduleRow[] = [
		{
			jobType: "RenewalJob",
			handler: "Handler",
			description: "Renew subscriptions",
			frequency: "Daily at 3:00 AM",
			tz: "America/New_York",
			nextRun: "In 4 hours",
			lastStatus: "Succeeded",
			lastStatusAgo: "4 hours ago"
		},
		{
			jobType: "CleanupOld",
			handler: "ReportsHandler",
			description: "Clean up old reports",
			frequency: "Every 2 hours",
			nextRun: "In 1 hour",
			lastStatus: "Succeeded",
			lastStatusAgo: "50 minutes ago"
		},
		{
			jobType: "Backup",
			handler: "DatabaseHandler",
			description: "Database backup",
			frequency: "Every 6 hours",
			nextRun: "In 43 min",
			lastStatus: "Succeeded",
			lastStatusAgo: "5 hours ago"
		},
		{
			jobType: "HelloJob",
			handler: "Handler",
			description: "Greeting job",
			frequency: "Every minute",
			nextRun: "In 54 sec",
			lastStatus: "Failed",
			lastStatusAgo: "1 minute ago"
		},
		{
			jobType: "Invoice",
			handler: "ProcessingHandler",
			description: "Process invoices",
			frequency: "Every Monday at 12:00 PM",
			tz: "Europe/London",
			nextRun: "1 day ago",
			lastStatus: "Paused",
			lastStatusAgo: "6 days ago"
		}
	];

	let query = "";
	let statusFilter: "All Statuses" | ScheduleStatus = "All Statuses";
	let typeFilter = "All Job Types";

	$: filtered = rows.filter((r) => {
		const q = query.trim().toLowerCase();
		const matchesQuery =
			!q ||
			`${r.jobType} ${r.handler} ${r.description} ${r.frequency} ${r.tz ?? ""}`
				.toLowerCase()
				.includes(q);

		const matchesStatus = statusFilter === "All Statuses" ? true : r.lastStatus === statusFilter;

		const matchesType = typeFilter === "All Job Types" ? true : r.jobType === typeFilter;

		return matchesQuery && matchesStatus && matchesType;
	});

	function clearFilters() {
		query = "";
		statusFilter = "All Statuses";
		typeFilter = "All Job Types";
	}

	function statusBadge(s: ScheduleStatus) {
		if (s === "Succeeded") return "badge badge-success";
		if (s === "Failed") return "badge badge-error";
		return "badge badge-warning";
	}

	// ---- modal state ----
	let createOpen = false;

	// modal form
	let jobName = "";
	let description = "";
	let handler = "";
	let frequency = "";
	let startPaused = false;

	const handlers = [
		"RenewalJobHandler",
		"CleanupOldReportsHandler",
		"BackupDatabaseHandler",
		"HelloJobHandler",
		"InvoiceProcessingHandler"
	];

	const frequencies = [
		"Every minute",
		"Every 5 minutes",
		"Every hour",
		"Every 2 hours",
		"Every 6 hours",
		"Daily at 03:00 AM",
		"Every Monday at 12:00 PM"
	];

	function openCreate() {
		createOpen = true;
	}

	function closeCreate() {
		createOpen = false;
	}

	function submitCreate() {
		// TODO wire to API
		console.log({ jobName, description, handler, frequency, startPaused });

		// reset (optional)
		jobName = "";
		description = "";
		handler = "";
		frequency = "";
		startPaused = false;

		closeCreate();
	}
</script>

<!-- Background similar to screenshot: dark + subtle gradients -->
<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl p-6">
		<!-- Header row -->
		<div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
			<div>
				<h1 class="text-2xl font-semibold">Recurring Schedules</h1>
			</div>

			<div class="flex flex-wrap items-center gap-3">
				<div class="text-sm opacity-70">
					Last updated: <span class="font-medium">12s ago</span>
				</div>

				<button class="btn btn-ghost btn-sm">
					<!-- refresh icon -->
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<path d="M21 12a9 9 0 1 1-2.64-6.36" />
						<path d="M21 3v6h-6" />
					</svg>
					Refresh
				</button>

				<button class="btn btn-ghost btn-sm" aria-label="Settings">
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z" />
						<path d="M19.4 15a7.8 7.8 0 0 0 .1-1 7.8 7.8 0 0 0-.1-1l2-1.5-2-3.5-2.4 1a8 8 0 0 0-1.7-1l-.4-2.6H9.1L8.7 7a8 8 0 0 0-1.7 1l-2.4-1-2 3.5L4.6 12a7.8 7.8 0 0 0-.1 1 7.8 7.8 0 0 0 .1 1l-2 1.5 2 3.5 2.4-1a8 8 0 0 0 1.7 1l.4 2.6h5.8l.4-2.6a8 8 0 0 0 1.7-1l2.4 1 2-3.5-2-1.5Z" />
					</svg>
				</button>

				<button class="btn btn-primary btn-sm" on:click={openCreate}>
					<span class="text-lg leading-none">＋</span> New Schedule
				</button>
			</div>
		</div>

		<!-- Card -->
		<div class="mt-6 rounded-2xl bg-base-100/60 shadow-xl backdrop-blur">
			<!-- Search -->
			<div class="p-4">
				<label class="input input-bordered flex items-center gap-2 w-full">
					<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-70" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<circle cx="11" cy="11" r="7"></circle>
						<path d="M21 21l-4.3-4.3"></path>
					</svg>
					<input class="grow" type="text" placeholder="Search schedules..." bind:value={query} />
				</label>
			</div>

			<!-- Filters row -->
			<div class="flex flex-col gap-3 border-t border-base-300/60 px-4 py-3 md:flex-row md:items-center md:justify-between">
				<div class="flex flex-wrap items-center gap-3">
					<select class="select select-bordered select-sm" bind:value={statusFilter}>
						<option>All Statuses</option>
						<option>Succeeded</option>
						<option>Failed</option>
						<option>Paused</option>
					</select>

					<select class="select select-bordered select-sm" bind:value={typeFilter}>
						<option>All Job Types</option>
						{#each Array.from(new Set(rows.map((r) => r.jobType))) as jt}
							<option value={jt}>{jt}</option>
						{/each}
					</select>

					<button class="btn btn-ghost btn-sm" on:click={clearFilters}>
						Clear filters
						<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-70" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
							<path d="M21 12a9 9 0 1 1-2.64-6.36" />
							<path d="M21 3v6h-6" />
						</svg>
					</button>
				</div>

				<div class="flex items-center gap-2 opacity-70">
					<button class="btn btn-ghost btn-sm" aria-label="View toggle">
						<span class="inline-flex items-center gap-2">
							<span class="h-3 w-3 rounded-full bg-current opacity-40"></span>
							<span class="h-3 w-3 rounded-sm border border-current opacity-40"></span>
						</span>
					</button>
				</div>
			</div>

			<!-- Table -->
			<div class="overflow-x-auto">
				<table class="table">
					<thead>
					<tr class="text-sm">
						<th class="w-[28%]">Job Type</th>
						<th class="w-[28%]">Description</th>
						<th class="w-[18%]">Frequency</th>
						<th class="w-[13%]">Next Run</th>
						<th class="w-[13%]">Last Status</th>
						<th class="w-[1%]"></th>
					</tr>
					</thead>

					<tbody>
					{#each filtered as r}
						<tr class="hover">
							<td>
								<div class="flex items-center gap-3">
									<!-- Icon tile -->
									<div class="h-10 w-10 rounded-xl bg-base-300/60 grid place-items-center">
										<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 opacity-80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
											<path d="M21 8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v1Z" />
											<path d="M21 16a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-1a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2v1Z" />
										</svg>
									</div>

									<div class="leading-tight">
										<div class="font-medium">{r.jobType}</div>
										<div class="text-xs opacity-60">{r.handler}</div>
									</div>
								</div>
							</td>

							<td>
								<div class="leading-tight">
									<div class="font-medium opacity-90">{r.description}</div>
									<div class="text-xs opacity-60">
										{#if r.tz}{r.tz}{:else}&nbsp;{/if}
									</div>
								</div>
							</td>

							<td>
								<div class="leading-tight">
									<div class="font-medium">{r.frequency}</div>
									{#if r.tz}
										<div class="text-xs opacity-60">{r.tz}</div>
									{:else}
										<div class="text-xs opacity-60">&nbsp;</div>
									{/if}
								</div>
							</td>

							<td class="opacity-90">{r.nextRun}</td>

							<td>
								<div class="flex items-center gap-2">
									<span class={statusBadge(r.lastStatus)}>{r.lastStatus}</span>
									<span class="text-xs opacity-60">{r.lastStatusAgo}</span>
								</div>
							</td>

							<td>
								<button class="btn btn-ghost btn-sm" aria-label="Row actions">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="M12 20a1 1 0 0 0 1-1v-1.1a7.9 7.9 0 0 0 2.2-1.3l.9.5a1 1 0 0 0 1.3-.4l1-1.7a1 1 0 0 0-.3-1.3l-.9-.6a8.3 8.3 0 0 0 0-2.6l.9-.6a1 1 0 0 0 .3-1.3l-1-1.7a1 1 0 0 0-1.3-.4l-.9.5A7.9 7.9 0 0 0 13 6.1V5a1 1 0 0 0-2 0v1.1a7.9 7.9 0 0 0-2.2 1.3l-.9-.5a1 1 0 0 0-1.3.4l-1 1.7a1 1 0 0 0 .3 1.3l.9.6a8.3 8.3 0 0 0 0 2.6l-.9.6a1 1 0 0 0-.3 1.3l1 1.7a1 1 0 0 0 1.3.4l.9-.5A7.9 7.9 0 0 0 11 17.9V19a1 1 0 0 0 1 1Z" />
										<path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7Z" />
									</svg>
								</button>
							</td>
						</tr>
					{/each}

					{#if filtered.length === 0}
						<tr>
							<td colspan="6">
								<div class="py-8 text-center opacity-70">No schedules found.</div>
							</td>
						</tr>
					{/if}
					</tbody>
				</table>
			</div>

			<!-- Footer / pagination -->
			<div class="flex items-center justify-between border-t border-base-300/60 px-4 py-3">
				<div class="text-sm opacity-70">{Math.min(filtered.length, 5)} of {rows.length}</div>

				<div class="join">
					<button class="btn btn-sm join-item" disabled aria-label="Previous page">‹</button>
					<button class="btn btn-sm join-item" disabled aria-label="Next page">›</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===== Create Schedule Modal ===== -->
	{#if createOpen}
		<div class="fixed inset-0 z-50">
			<!-- backdrop -->
			<div class="absolute inset-0 bg-black/60 backdrop-blur-sm" on:click={closeCreate} />

			<!-- modal shell -->
			<div class="absolute inset-0 flex items-center justify-center p-4">
				<div
					class="w-full max-w-3xl rounded-2xl border border-white/10 bg-[#2b2f43]/80 shadow-2xl backdrop-blur"
					on:click|stopPropagation
					role="dialog"
					aria-modal="true"
					aria-label="New Recurring Schedule"
				>
					<!-- header -->
					<div class="flex items-center justify-between px-8 pt-7">
						<h2 class="text-2xl font-semibold text-white/90">New Recurring Schedule</h2>

						<button
							class="btn btn-ghost btn-sm h-9 min-h-9 w-9 rounded-xl bg-white/10 hover:bg-white/15"
							aria-label="Close"
							on:click={closeCreate}
						>
							<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-white/80" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
								<path d="M18 6 6 18" />
								<path d="M6 6l12 12" />
							</svg>
						</button>
					</div>

					<!-- body -->
					<div class="px-8 pb-6 pt-6 space-y-6">
						<!-- Job Name -->
						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Job Name <span class="text-pink-400">*</span>
								</span>
							</label>
							<input
								class="input input-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white placeholder:text-white/35"
								placeholder="Enter job name"
								bind:value={jobName}
							/>
						</div>

						<!-- Description -->
						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">Description</span>
							</label>
							<textarea
								class="textarea textarea-bordered w-full min-h-[92px] bg-white/5 border-white/15 focus:border-white/30 text-white placeholder:text-white/35"
								placeholder="Enter description (optional)"
								bind:value={description}
							/>
						</div>

						<!-- Select Handler -->
						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Select Handler <span class="text-pink-400">*</span>
								</span>
							</label>
							<div class="relative">
								<select
									class="select select-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white"
									bind:value={handler}
								>
									<option value="" disabled selected>Select handler type</option>
									{#each handlers as h}
										<option value={h}>{h}</option>
									{/each}
								</select>

								<div class="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white/60">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</div>
							</div>
						</div>

						<!-- Frequency -->
						<div class="form-control">
							<label class="label py-0 mb-2">
								<span class="label-text text-white/70 font-medium">
									Frequency <span class="text-pink-400">*</span>
								</span>
							</label>

							<div class="relative">
								<select
									class="select select-bordered w-full bg-white/5 border-white/15 focus:border-white/30 text-white"
									bind:value={frequency}
								>
									<option value="" disabled selected>Select frequency</option>
									{#each frequencies as f}
										<option value={f}>{f}</option>
									{/each}
								</select>

								<div class="pointer-events-none absolute right-4 top-1/2 -translate-y-1/2 text-white/60">
									<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</div>
							</div>
						</div>

						<!-- Start paused toggle -->
						<div class="flex items-center gap-3 pt-1">
							<input class="toggle toggle-lg" type="checkbox" bind:checked={startPaused} />
							<span class="text-white/70">Start Paused</span>
						</div>
					</div>

					<!-- footer -->
					<div class="flex items-center justify-end gap-3 px-8 pb-7">
						<button class="btn btn-ghost bg-white/10 hover:bg-white/15 text-white/80" on:click={closeCreate}>
							Cancel
						</button>

						<button
							class="btn border-0 bg-fuchsia-500 hover:bg-fuchsia-400 text-white"
							on:click={submitCreate}
							disabled={!jobName || !handler || !frequency}
							class:opacity-60={!jobName || !handler || !frequency}
						>
							Create Schedule
						</button>
					</div>
				</div>
			</div>
		</div>
	{/if}
</div>
