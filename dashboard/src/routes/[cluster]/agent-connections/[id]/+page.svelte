<!-- +page.svelte (SvelteKit) -->
<script lang="ts">
	import { onMount, onDestroy } from "svelte";
	import { page } from "$app/stores";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { BucketStatus } from "$lib/api/enums";
	import { DateTimeUtil } from "$lib/helper/datetime-util";

	type ApiBucketModel = components["schemas"]["ApiBucketModel"];

	type WorkerRow = {
		name: string;
		lane: string;
		laneBadge: "healthy" | "warning" | "useful";
		statusText: string;
		statusBadge: "warning" | "neutral";
		cpu: number;
		mem: number;
		heartbeat: string;
		mode: string;
	};

	export let data: {
		agentConnection: any;
		workers: any[];
		buckets: any[];
		error: string | null;
	};

	const clusterId = () => $page.params.cluster;
	const connectionId = () => $page.params.id;

	let agentConn: any = null;
	let apiBuckets: ApiBucketModel[] = [];
	let apiWorkers: any[] = [];
	let isRefreshing = false;
	let lastUpdatedAt = new Date();
	let refreshError: string | null = null;
	let poller: number | undefined;
	const refreshIntervalSec = 15;

	$: agentName = agentConn?.displayName ?? agentConn?.name ?? agentConn?.id ?? "Unknown";
	$: clusterName = agentConn?.cluster ?? clusterId();

	$: health = deriveHealth(agentConn);

	function deriveHealth(conn: any): { label: string; tone: "success" | "warning" | "error" } {
		const raw = String(conn?.health ?? conn?.status ?? conn?.state ?? "").toLowerCase();
		if (raw === "error" || raw === "failed" || raw === "err") return { label: "Error", tone: "error" };
		if (raw === "warning" || raw === "warn" || raw === "degraded") return { label: "Warning", tone: "warning" };
		if (raw === "healthy" || raw === "active" || raw === "connected") return { label: "OK", tone: "success" };
		if (!conn) return { label: "—", tone: "warning" };
		return { label: "OK", tone: "success" };
	}

	$: totalBuckets = apiBuckets.length;
	$: drainingBuckets = apiBuckets.filter(
		(b) => b.status === BucketStatus.Draining || b.status === BucketStatus.ReadyToDrain
	).length;
	$: activeBuckets = apiBuckets.filter(
		(b) => b.status === BucketStatus.Active || b.status === BucketStatus.Completing
	).length;

	$: workers = mapWorkers(apiWorkers);
	$: workersBound = workers.length;

	function mapWorkerMode(mode: number | null | undefined): string {
		switch (mode) {
			case 1: return "Coordinator";
			case 2: return "Full";
			case 3: return "Execution";
			case 4: return "Idle";
			default: return "—";
		}
	}

	function mapWorkerLaneBadge(status: number | null | undefined): WorkerRow["laneBadge"] {
		switch (status) {
			case 0: return "healthy";
			case 1: return "warning";
			case 2: return "useful";
			default: return "healthy";
		}
	}

	function mapWorkerStatusLabel(status: number | null | undefined): string {
		switch (status) {
			case 0: return "Idle";
			case 1: return "Active";
			case 2: return "Stopped";
			default: return "—";
		}
	}

	function mapWorkers(raw: any[]): WorkerRow[] {
		return raw.map((w) => ({
			name: String(w?.displayName ?? w?.name ?? w?.id ?? "Unknown"),
			lane: mapWorkerStatusLabel(w?.status),
			laneBadge: mapWorkerLaneBadge(w?.status),
			statusText: mapWorkerStatusLabel(w?.status),
			statusBadge: (w?.status === 2 ? "warning" : "neutral") as WorkerRow["statusBadge"],
			cpu: Number(w?.cpuUsagePercent ?? 0),
			mem: Number(w?.memoryUsagePercent ?? 0),
			heartbeat: w?.lastHeartbeatAt
				? DateTimeUtil.formatAgeShort(Date.now() - new Date(w.lastHeartbeatAt).getTime()) + " ago"
				: "—",
			mode: mapWorkerMode(w?.mode)
		}));
	}

	$: connectionInfo = {
		agentId: agentConn?.id ?? "—",
		host: agentConn?.host ?? agentConn?.hostDisplayName ?? "—",
		boundCluster: clusterName,
		state: agentConn?.state ?? agentConn?.status ?? "—",
		agentName
	};

	$: version = {
		engine: agentConn?.repositoryTypeId ?? "—",
		ver: agentConn?.version ?? "—",
		host: agentConn?.host ?? "—",
		port: agentConn?.port ?? "—",
		database: agentConn?.database ?? "—"
	};

	$: lastUpdated = DateTimeUtil.formatDateTime(lastUpdatedAt);

	$: updatedAgo = DateTimeUtil.lastUpdatedAgo(new Date(), lastUpdatedAt);

	async function refreshNow() {
		isRefreshing = true;
		refreshError = null;
		try {
			const cid = clusterId();
			const connId = connectionId();
			if (!cid || !connId) return;

			const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const [connResponse, workersResponse, bucketsResponse] = await Promise.all([
				jmApi.GET("/{clusterId}/agent-connections/{agentConnectionId}", {
					params: { path: { clusterId: cid, agentConnectionId: connId } }
				}),
				jmApi.GET("/{clusterId}/workers", {
					params: {
						path: { clusterId: cid },
						query: { AgentConnectionId: connId }
					}
				}),
				jmApi.GET("/{clusterId}/buckets", {
					params: {
						path: { clusterId: cid },
						query: { AgentConnectionId: connId }
					}
				})
			]);

			if (connResponse.error) {
				refreshError = String(connResponse.error);
				console.error("API error (agent-connection):", connResponse.error);
				return;
			}

			agentConn = connResponse.data ?? null;
			apiWorkers = (workersResponse.data ?? []) as any[];
			apiBuckets = (bucketsResponse.data ?? []) as ApiBucketModel[];
			lastUpdatedAt = new Date();
		} catch (error) {
			refreshError = error instanceof Error ? error.message : String(error);
			console.error("Failed to fetch agent connection:", error);
		} finally {
			isRefreshing = false;
		}
	}

	function restartPoller() {
		if (poller) window.clearInterval(poller);
		poller = window.setInterval(() => {
			refreshNow();
		}, refreshIntervalSec * 1000);
	}

	onMount(() => {
		agentConn = data.agentConnection;
		apiWorkers = data.workers ?? [];
		apiBuckets = (data.buckets ?? []) as ApiBucketModel[];
		refreshError = data.error;
		lastUpdatedAt = new Date();

		refreshNow();
		restartPoller();
	});

	onDestroy(() => {
		if (poller) window.clearInterval(poller);
	});

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

<div class="min-h-screen bg-base-100">
	<div class="w-full px-6 py-6">
		<div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
			<div class="flex flex-col gap-2">
				<div class="text-sm breadcrumbs opacity-70">
					<ul>
						<li><a href="/{clusterId()}/agent-connections" class="link link-hover">Agent Connections</a></li>
						<li>{agentName}</li>
					</ul>
				</div>

				<div class="flex items-center gap-3">
					<h1 class="text-3xl font-semibold">{agentName}</h1>
				</div>

				<div class="flex flex-wrap items-center gap-2">
					<div class="badge badge-neutral badge-outline">Cluster: {clusterName}</div>
				</div>
			</div>

			<div class="flex items-center gap-3 text-sm opacity-80">
				<span>Last Refresh: {lastUpdated}</span>
				<button
					class="btn btn-ghost btn-sm btn-square"
					aria-label="Refresh now"
					on:click={refreshNow}
					disabled={isRefreshing}
				>
					<svg
						xmlns="http://www.w3.org/2000/svg"
						class={"h-4 w-4 " + (isRefreshing ? "animate-spin" : "")}
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<path d="M21 12a9 9 0 1 1-3-6.7" />
						<path d="M21 3v6h-6" />
					</svg>
				</button>
			</div>
		</div>

		{#if refreshError}
			<div class="alert alert-error mt-4 text-sm">
				<span>{refreshError}</span>
			</div>
		{/if}

		<!-- Summary cards -->
		<div class="mt-6 grid grid-cols-1 gap-4 md:grid-cols-12">
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Health</div>
						<div class="badge badge-{health.tone} badge-outline">
							{#if health.tone === 'success'}✓{:else if health.tone === 'warning'}⚠{:else}⛔{/if}
						</div>
					</div>
					<div class="text-2xl font-semibold">{health.label}</div>
					<div class="text-xs opacity-60">Agent connection health</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg md:col-span-3">
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

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg md:col-span-3">
				<div class="card-body">
					<div class="flex items-center justify-between">
						<div class="text-sm opacity-70">Buckets</div>
						<div class="badge badge-neutral badge-outline">☰</div>
					</div>
					<div class="text-2xl font-semibold">
						{activeBuckets} / {totalBuckets}
						<span class="ml-2 text-sm font-normal opacity-70">{drainingBuckets} draining</span>
					</div>
					<progress class="progress progress-warning w-full" value={activeBuckets} max={totalBuckets || 1}></progress>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg md:col-span-3">
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
			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg h-full">
				<div class="card-body py-4">
					<div class="card-title text-base opacity-80">Connection Info</div>
					<div class="divider my-2"></div>

					<div class="grid grid-cols-2 gap-3 text-sm">
						<div class="opacity-70">Agent ID</div>
						<div class="font-medium">{connectionInfo.agentId}</div>

						<div class="opacity-70">Host</div>
						<div class="font-mono">{connectionInfo.host}</div>
					</div>
				</div>
			</div>

			<div class="card bg-base-200/60 border border-base-300/60 shadow-lg h-full">
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
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
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

		<!-- Buckets -->
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body">
				<div class="flex items-center justify-between">
					<div class="card-title">Buckets</div>
					<a href="/{clusterId()}/buckets" class="link link-primary text-sm">View all</a>
				</div>
				<div class="divider my-2"></div>

				{#if apiBuckets.length === 0}
					<div class="text-sm opacity-60">No buckets found for this agent connection.</div>
				{:else}
					<div class="space-y-2">
						{#each apiBuckets.slice(0, 5) as b}
							<a
								href="/{clusterId()}/buckets/{b.id}"
								class="flex items-center justify-between rounded-xl bg-base-200 px-4 py-3 hover:bg-base-300 transition cursor-pointer"
							>
								<div class="flex items-center gap-3">
									<span class="font-medium">{b.name ?? b.id ?? '—'}</span>
									{#if b.workerLane}
										<span class="badge badge-ghost badge-sm">{b.workerLane}</span>
									{/if}
								</div>
								<div class="flex items-center gap-2">
									{#if b.status === BucketStatus.Active}
										<span class="badge badge-success badge-sm">Active</span>
									{:else if b.status === BucketStatus.Draining}
										<span class="badge badge-warning badge-sm">Draining</span>
									{:else if b.status === BucketStatus.ReadyToDrain}
										<span class="badge badge-warning badge-outline badge-sm">Ready to Drain</span>
									{:else if b.status === BucketStatus.Completing}
										<span class="badge badge-info badge-sm">Completing</span>
									{:else if b.status === BucketStatus.Lost}
										<span class="badge badge-error badge-sm">Lost</span>
									{:else}
										<span class="badge badge-ghost badge-sm">—</span>
									{/if}
								</div>
							</a>
						{/each}
					</div>
					{#if apiBuckets.length > 5}
						<div class="mt-3 text-sm opacity-60">+ {apiBuckets.length - 5} more buckets</div>
					{/if}
				{/if}
			</div>
		</div>
	</div>
</div>
