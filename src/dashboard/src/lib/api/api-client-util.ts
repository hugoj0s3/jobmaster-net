import createClient from "openapi-fetch";
import type { paths } from "$lib/api/schema";

import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";
import { AuthRetentionUtil } from '$lib/api/auth-retention-util';
import type { Credentials } from '$lib/api/credentials';

export class ApiClientUtil {
	static async CreateApiClientFromConfig(fetchFn: typeof fetch, clusterId?: string) {
		await JobMasterConfigUtil.loadConfig();
		const apiUrl = JobMasterConfigUtil.getClusterApiUrl(clusterId);
		return ApiClientUtil.CreateApiClient(apiUrl, fetchFn);
	}

	static async ValidateCredentials(credentials: Credentials, fetchFn: typeof fetch, clusterId?: string): Promise<boolean> {
		await JobMasterConfigUtil.loadConfig();
		const apiUrl = JobMasterConfigUtil.getClusterApiUrl(clusterId);
		const apiClient = ApiClientUtil.CreateApiClient(apiUrl, fetchFn, credentials);

		const response = await apiClient.GET("/clusters/ids");

		return !response.error && response.response.ok;
	}

	private static async buildAuthHeaders(credentials?: Credentials): Promise<Record<string, string>> {
		credentials ??= await AuthRetentionUtil.getCredentials() ?? undefined;
		if (!credentials) {
			return {};
		}

		const config = await JobMasterConfigUtil.loadConfig();
		if (!config) {
			return {};
		}

		const authProvider = config.auth
			?.providers
			.find(p => p.type == credentials.type);
		if (!authProvider) {
			return {};
		}

		const headers: Record<string, string> = {};
		if (authProvider.type == "USER_PASSWORD") {
			headers[authProvider.userHeaderName!] = credentials.userName!;
			headers[authProvider.passwordHeaderName!] = credentials.userPassword!;
		} else {
			const headerName = authProvider.headerName || "Authorization";
			const scheme = authProvider.scheme;
			const value = scheme ? `${scheme} ${credentials.secretValue!}` : credentials.secretValue!;
			headers[headerName] = value;
		}

		return headers;
	}

	private static normalizeBaseUrl(apiBaseUrl: string | null | undefined): string {
		const raw = (apiBaseUrl ?? "").trim();
		if (!raw) return "";

		// Full URL: keep origin + any path prefix.
		try {
			const u = new URL(raw);
			const prefix = u.pathname.replace(/\/+$/g, "");
			return `${u.origin}${prefix}`;
		} catch {
			// Not a full URL
		}

		// Relative base path (same origin). Ensure leading slash.
		const prefix = raw.replace(/\/+$/g, "");
		if (!prefix) return "";
		return prefix.startsWith("/") ? prefix : `/${prefix}`;
	}

	private static CreateApiClient(
		apiBaseUrl: string | null | undefined,
		fetchFn: typeof fetch,
		credentials?: Credentials
	) {
		const baseUrl = ApiClientUtil.normalizeBaseUrl(apiBaseUrl);

		return createClient<paths>({
			baseUrl,
			fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
				const authHeaders = await ApiClientUtil.buildAuthHeaders(credentials);
				const mergedHeaders: HeadersInit = {
					...(init?.headers ?? {}),
					...authHeaders
				};

				return fetchFn(input, {
					...init,
					headers: mergedHeaders
				});
			}
		});
	}
}