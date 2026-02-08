<script lang="ts">
	type Activity = {
		status: "Succeeded" | "Failed" | "Paused";
		message: string;
		when: string;
		duration: string;
	};

	type UpcomingRun = {
		scheduledAt: string;
		timezoneA: string;
		timezoneB: string;
	};

	const scheduleName = "RenewalJobHandler";

	const details = {
		handler: "RenewalJobHandler",
		description: "Renew subscriptions",
		schedule: "Daily at 03:00 AM",
		lastResult: "Succeeded",
		debued: true,
		nextRun: "In 23 hours at Today, 03:00 AM",
		lastRunStatus: "Succeeded",
		lastRunAgo: "3 hours ago",
		status: "Active"
	};

	const recent: Activity[] = [
		{ status: "Succeeded", message: "Renew subscriptions,", when: "3 hours ago", duration: "9.7s" },
		{ status: "Succeeded", message: "Renew subscriptions,", when: "1 day ago", duration: "10.3s" },
		{ status: "Succeeded", message: "Renew subscriptions,", when: "3 days ago", duration: "10.1s" },
		{ status: "Succeeded", message: "Renew subscriptions,", when: "5 days ago", duration: "10.5s" },
		{ status: "Succeeded", message: "Renew subscriptions,", when: "6 days ago", duration: "10.2s" }
	];

	const upcoming: UpcomingRun[] = [
		{ scheduledAt: "Apr 25, 2024, 03:00 AM", timezoneA: "America/Sao_Paulo", timezoneB: "America/Sao_Paulo" },
		{ scheduledAt: "Apr 26, 2024, 03:00 AM", timezoneA: "America/Sao_Paulo", timezoneB: "America/Sao_Paulo" },
		{ scheduledAt: "Apr 27, 2024, 03:00 AM", timezoneA: "America/Sao_Paulo", timezoneB: "America/Sao_Paulo" }
	];

	const badgeClass = (s: Activity["status"]) => {
		if (s === "Succeeded") return "badge badge-success";
		if (s === "Failed") return "badge badge-error";
		return "badge badge-warning";
	};
</script>

<div class="min-h-screen bg-base-200">
	<div class="mx-auto max-w-6xl p-6">
		<!-- Top: back + title + actions -->
		<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
			<div class="space-y-2">
				<button class="btn btn-ghost btn-sm w-fit">
					<span class="text-lg leading-none">‹</span>
					Back
				</button>

				<h1 class="text-3xl font-semibold tracking-tight">{scheduleName}</h1>
			</div>

			<div class="flex flex-wrap items-center gap-3">
				<button class="btn btn-primary btn-sm">
          <span class="inline-flex items-center gap-2">
            <!-- play -->
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
              <path d="M8 5v14l11-7z" />
            </svg>
            Run Now
          </span>
				</button>

				<button class="btn btn-warning btn-sm">
          <span class="inline-flex items-center gap-2">
            <!-- pause -->
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="currentColor">
              <path d="M6 5h4v14H6zM14 5h4v14h-4z" />
            </svg>
            Abort
          </span>
				</button>

				<button class="btn btn-error btn-outline btn-sm">
          <span class="inline-flex items-center gap-2">
            <!-- trash -->
            <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 6h18" />
              <path d="M8 6V4h8v2" />
              <path d="M19 6l-1 14H6L5 6" />
              <path d="M10 11v6M14 11v6" />
            </svg>
           	Cancel
          </span>
				</button>
			</div>
		</div>

		<!-- Content grid -->
		<div class="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
			<!-- Left column -->
			<div class="lg:col-span-2 space-y-6">
				<!-- Details card -->
				<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
					<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
						<h2 class="text-lg font-semibold">Details</h2>
						<span class="badge badge-neutral badge-lg">{details.status}</span>
					</div>

					<div class="p-6">
						<div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
							<div class="space-y-3">
								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Handler</div>
									<div class="font-medium">{details.handler}</div>
								</div>

								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Description</div>
									<div class="font-medium">{details.description}</div>
								</div>

								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Schedule</div>
									<div class="font-medium text-primary">{details.schedule}</div>
								</div>

								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Last Result</div>
									<div class="flex items-center gap-2">
										<span class="badge badge-success">Succeeded</span>
										{#if details.debued}
                      <span class="inline-flex items-center gap-2 text-sm opacity-70">
                        <span class="h-2 w-2 rounded-full bg-success"></span>
                        Debued
                      </span>
										{/if}
									</div>
								</div>
							</div>

							<div class="space-y-3">
								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Next Run</div>
									<div class="font-medium text-warning">{details.nextRun}</div>
								</div>

								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Last Run</div>
									<div class="flex items-center gap-2">
										<span class="badge badge-success">Succeeded</span>
										<span class="text-sm opacity-70">{details.lastRunAgo}</span>
									</div>
								</div>

								<div class="flex items-center justify-between gap-4">
									<div class="text-sm opacity-70">Status</div>
									<div class="flex items-center gap-2">
										<span class="h-2 w-2 rounded-full bg-success"></span>
										<span class="font-medium">{details.status}</span>
									</div>
								</div>
							</div>
						</div>
					</div>
				</div>

				<!-- Upcoming runs -->
				<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
					<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
						<h2 class="text-lg font-semibold">Upcoming Runs</h2>
					</div>

					<div class="overflow-x-auto">
						<table class="table">
							<thead>
							<tr class="text-sm">
								<th>Scheduled At</th>
								<th>Timezone</th>
								<th>Timezone</th>
							</tr>
							</thead>
							<tbody>
							{#each upcoming as u}
								<tr class="hover">
									<td class="font-medium">{u.scheduledAt}</td>
									<td class="opacity-80">{u.timezoneA}</td>
									<td class="opacity-80">{u.timezoneB}</td>
								</tr>
							{/each}
							</tbody>
						</table>
					</div>

					<div class="flex items-center justify-between px-6 py-3 border-t border-base-300/50">
						<div class="text-sm opacity-70">1 - 3 of 3</div>
						<div class="join">
							<button class="btn btn-sm join-item" disabled>Previous</button>
							<button class="btn btn-sm join-item btn-active">1</button>
							<button class="btn btn-sm join-item" disabled>Next</button>
						</div>
					</div>
				</div>
			</div>

			<!-- Right column: Recent activity -->
			<div class="space-y-6">
				<div class="rounded-2xl bg-base-100/60 shadow-xl backdrop-blur border border-base-300/40">
					<div class="flex items-center justify-between px-6 py-4 border-b border-base-300/50">
						<h2 class="text-lg font-semibold">Recent Activity</h2>
						<button class="btn btn-ghost btn-sm" aria-label="More">
							<svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 24 24" fill="currentColor">
								<path d="M5 10a2 2 0 1 0 0 4 2 2 0 0 0 0-4Zm7 0a2 2 0 1 0 0 4 2 2 0 0 0 0-4Zm7 0a2 2 0 1 0 0 4 2 2 0 0 0 0-4Z" />
							</svg>
						</button>
					</div>

					<div class="p-4">
						<ul class="space-y-3">
							{#each recent as a}
								<li class="rounded-xl border border-base-300/50 bg-base-100/40 px-4 py-3">
									<div class="flex items-start justify-between gap-3">
										<div class="space-y-1">
											<div class="flex items-center gap-2">
												<span class={badgeClass(a.status)}>{a.status}</span>
												<span class="font-medium opacity-90">{a.message}</span>
											</div>
											<div class="text-sm opacity-70">{a.when}</div>
										</div>

										<div class="flex items-center gap-2">
											<div class="text-sm opacity-70">{a.duration}</div>
											<button class="btn btn-ghost btn-xs" aria-label="Dismiss">
												<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
													<path d="M18 6 6 18" />
													<path d="m6 6 12 12" />
												</svg>
											</button>
										</div>
									</div>
								</li>
							{/each}
						</ul>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>
