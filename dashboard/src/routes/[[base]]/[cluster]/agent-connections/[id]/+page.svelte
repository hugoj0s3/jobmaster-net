<script lang="ts">
	import { onMount } from "svelte";
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { ApiClientUtil } from "$lib/api/api-client-util";
	import type { components } from "$lib/api/schema";
	import { DateDisplayUtil } from "$lib/helper/date-display-util";
	import Pager from "$lib/components/Pager.svelte";
	import { workerModeLabel } from "$lib/api/enums";

	type ApiAgentWorker = components["schemas"]["ApiAgentWorker"];

	const clusterId = () => $page.params.cluster;
	const connectionId = () => $page.params.id;

	let agentConn: any = null;
	let workers: ApiAgentWorker[] = [];
	let workersPage = 0;
	let workersPageSize = 10;
	let isRefreshing = false;
	let notFound = false;

	$: connName = agentConn?.name ?? agentConn?.id ?? "Unknown";
	$: isAlive = agentConn?.isAlive === true;
	$: pagedWorkers = workers.slice(workersPage * workersPageSize, (workersPage + 1) * workersPageSize);

	async function refreshNow() {
		isRefreshing = true;
		try {
			const cid = clusterId();
			const connId = connectionId();
			if (!cid || !connId) return;

			const jm = await ApiClientUtil.CreateApiClientFromConfig(fetch);

			const [connResp, workersResp] = await Promise.all([
				(jm as any).GET("/{clusterId}/agent-connections/{agentConnectionId}", {
					params: { path: { clusterId: cid, agentConnectionId: connId } }
				}),
				jm.GET("/{clusterId}/workers", {
					params: { path: { clusterId: cid }, query: { AgentConnectionId: connId } }
				})
			]);

			if (connResp.error) {
				if (connResp.response?.status === 404) { notFound = true; return; }
				return;
			}
			agentConn = connResp.data ?? null;
			if (!agentConn) { notFound = true; return; }

			workers = (workersResp.data ?? []) as ApiAgentWorker[];
		} catch (e) {
			console.error("Failed to fetch agent connection:", e);
		} finally {
			isRefreshing = false;
		}
	}

	onMount(() => { refreshNow(); });
</script>

<div class="min-h-screen bg-base-100">
	<div class="mx-auto max-w-full px-6 py-6">

		<div class="space-y-2">
			<div class="text-sm breadcrumbs opacity-70">
				<ul>
					<li><a href="/{clusterId()}/agent-connections" class="link link-hover">Agent Connections</a></li>
					<li>{connName}</li>
				</ul>
			</div>
			<h1 class="text-3xl font-semibold tracking-tight">{connName}</h1>
			<div class="flex items-center gap-2">
				<span class="inline-block h-2 w-2 rounded-full {isAlive ? 'bg-success' : 'bg-error'}"></span>
				<span class={"badge badge-sm badge-outline " + (isAlive ? "badge-success" : "badge-error")}>{isAlive ? "OK" : "Offline"}</span>
			</div>
		</div>

		{#if notFound}
			<div class="alert alert-error text-sm mt-4"><span>Agent Connection not found.</span></div>
		{:else if agentConn}

		<!-- Info card -->
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body">
				<h2 class="card-title text-base">Details</h2>
				<div class="divider my-2"></div>
				<div class="grid grid-cols-1 gap-3 sm:grid-cols-2 text-sm">
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Repository Type</span>
						<span class="font-medium">{agentConn.repositoryTypeId ?? "—"}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Last Heartbeat</span>
						<span class="font-medium">{DateDisplayUtil.formatRelativeOrDate(agentConn.lastHeartbeat)}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Created At</span>
						<span class="font-medium">{DateDisplayUtil.formatRelativeOrDate(agentConn.createdAt)}</span>
					</div>
					<div class="flex items-center justify-between gap-4">
						<span class="opacity-70">Protect Changes</span>
						<span class="font-medium">{agentConn.protectConnectionChanges ? "Yes" : "No"}</span>
					</div>
				</div>
			</div>
		</div>

		<!-- Workers -->
		<div class="mt-6 card bg-base-200/60 border border-base-300/60 shadow-lg">
			<div class="card-body">
				<div class="flex items-center justify-between gap-3">
					<h2 class="card-title text-base">Workers</h2>
					<Pager
						bind:pageIndex={workersPage}
						bind:pageSize={workersPageSize}
						totalCount={workers.length}
						currentCount={pagedWorkers.length}
						showPageSize={true}
					/>
				</div>
				<div class="divider my-2"></div>
				<div class="overflow-x-auto">
					<table class="table">
						<thead>
						<tr class="text-base-content/70">
							<th>Worker</th>
							<th>Mode</th>
							<th>Worker Lane</th>
							<th>Status</th>
							<th>Last Heartbeat</th>
						</tr>
						</thead>
						<tbody>
						{#each pagedWorkers as w (w.id)}
							<tr class="hover cursor-pointer" on:click={() => goto(`/${clusterId()}/workers/${w.id}`)}>
								<td class="font-medium">{w.name ?? w.id ?? "—"}</td>
								<td><span class="badge badge-ghost badge-sm">{workerModeLabel(w.mode)}</span></td>
								<td class="opacity-70">{w.workerLane ?? "—"}</td>
								<td>
									<span class={"badge badge-sm badge-outline " + (w.isAlive ? "badge-success" : "badge-error")}>
										{w.isAlive ? "Online" : "Offline"}
									</span>
								</td>
								<td class="opacity-70 whitespace-nowrap">{DateDisplayUtil.formatRelativeOrDate(w.lastHeartbeat)}</td>
							</tr>
						{:else}
							<tr><td colspan="5" class="text-center opacity-60 py-6">No workers.</td></tr>
						{/each}
						</tbody>
					</table>
				</div>
			</div>
		</div>

		{/if}
	</div>
</div>
