import type { PageLoad } from "./$types";
import { ApiClientUtil } from "$lib/api/api-client-util";

export const load: PageLoad = async ({ fetch, params }) => {
	try {
		const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

		// Try to get the individual agent connection first
		const connResponse = await jmApi.GET("/{clusterId}/agent-connections/{agentConnectionId}", {
			params: { path: { clusterId: params.cluster, agentConnectionId: params.id } }
		});

		let agentConnection = connResponse.data ?? null;
		let error = connResponse.error ? String(connResponse.error) : null;

		// If individual endpoint fails (404), try to get from list as fallback
		if (!agentConnection && connResponse.error) {
			try {
				const listResponse = await jmApi.GET("/{clusterId}/agent-connections", {
					params: { path: { clusterId: params.cluster } }
				});
				
				if (!listResponse.error && listResponse.data) {
					const allConnections = listResponse.data as any[];
					agentConnection = allConnections.find(conn => conn.id === params.id) || null;
					if (agentConnection) {
						error = null; // Clear error since we found the connection
					}
				}
			} catch (fallbackError) {
				console.error("Fallback fetch failed:", fallbackError);
			}
		}

		// Get workers and buckets (these should work even if individual endpoint fails)
		const [workersResponse, bucketsResponse] = await Promise.all([
			jmApi.GET("/{clusterId}/workers", {
				params: {
					path: { clusterId: params.cluster },
					query: { AgentConnectionId: params.id }
				}
			}),
			jmApi.GET("/{clusterId}/buckets", {
				params: {
					path: { clusterId: params.cluster },
					query: { AgentConnectionId: params.id }
				}
			})
		]);

		return {
			agentConnection,
			workers: (workersResponse.data ?? []) as any[],
			buckets: (bucketsResponse.data ?? []) as any[],
			error
		};
	} catch (e) {
		return {
			agentConnection: null as any,
			workers: [] as any[],
			buckets: [] as any[],
			error: e instanceof Error ? e.message : String(e)
		};
	}
};
