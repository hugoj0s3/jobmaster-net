import type { PageLoad } from "./$types";
import { ApiClientUtil } from "$lib/api/api-client-util";

export const load: PageLoad = async ({ fetch, params }) => {
	try {
		const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

		const [connResponse, workersResponse, bucketsResponse] = await Promise.all([
			jmApi.GET("/{clusterId}/agent-connections/{agentConnectionId}", {
				params: { path: { clusterId: params.cluster, agentConnectionId: params.id } }
			}),
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
			agentConnection: (connResponse.data ?? null) as any,
			workers: (workersResponse.data ?? []) as any[],
			buckets: (bucketsResponse.data ?? []) as any[],
			error: connResponse.error ? String(connResponse.error) : null
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
