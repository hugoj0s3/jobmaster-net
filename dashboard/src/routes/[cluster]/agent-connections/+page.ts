import type { PageLoad } from "./$types";
import { ApiClientUtil } from "$lib/api/api-client-util";

export const load: PageLoad = async ({ fetch, params }) => {
	try {
		const jmApi = await ApiClientUtil.CreateApiClientFromConfig(fetch);

		const response = await jmApi.GET("/{clusterId}/agent-connections", {
			params: { path: { clusterId: params.cluster } }
		});

		if (response.error) {
			return {
				agentConnections: [],
				error: String(response.error)
			};
		}

		return {
			agentConnections: (response.data ?? []) as any[],
			error: null as string | null
		};
	} catch (e) {
		return {
			agentConnections: [],
			error: e instanceof Error ? e.message : String(e)
		};
	}
};
