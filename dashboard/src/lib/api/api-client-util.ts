import createClient from "openapi-fetch";
import type { paths } from "$lib/api/schema";
import { getAllSecrets, type SecretAuthProvider } from "$lib/secret/secrets";

import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";

export class ApiClientUtil {
    
    private static async buildAuthHeaders(): Promise<Record<string, string>> {
        const headers: Record<string, string> = {};
        const { apiKey, user, pwd, jwt, authProvider } = await getAllSecrets();
        const provider = authProvider as SecretAuthProvider | null;

        if (apiKey) {
            const headerName =
                provider?.type === "API_KEY" && provider.headerName
                    ? provider.headerName
                    : "X-JobMaster-Key";
            headers[headerName] = apiKey;
        }

        if (user && pwd) {
            const userHeaderName =
                provider?.type === "USER_PASSWORD" && provider.userHeaderName
                    ? provider.userHeaderName
                    : "X-JobMaster-User";
            const passwordHeaderName =
                provider?.type === "USER_PASSWORD" && provider.passwordHeaderName
                    ? provider.passwordHeaderName
                    : "X-JobMaster-Pwd";
            headers[userHeaderName] = user;
            headers[passwordHeaderName] = pwd;
        }

        if (jwt) {
            let transportHeader: string;
            let scheme: string | undefined;
            if (provider?.type === "JWT_SIMPLE") {
                transportHeader = provider.headerName ?? "Authorization";
                scheme = provider.scheme;
            } else {
                transportHeader = (provider as any)?.transport?.header ?? "Authorization";
                scheme = (provider as any)?.transport?.scheme;
            }
            headers[transportHeader] = scheme ? `${scheme} ${jwt}` : jwt;
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

    private static CreateApiClient(apiBaseUrl: string | null | undefined, fetchFn: typeof fetch) {
        const baseUrl = ApiClientUtil.normalizeBaseUrl(apiBaseUrl);

        return createClient<paths>({
            baseUrl,
            fetch: async (input: RequestInfo | URL, init?: RequestInit) => {
                const authHeaders = await ApiClientUtil.buildAuthHeaders();
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

    static async CreateApiClientFromConfig(fetchFn: typeof fetch, clusterId?: string) {
        await JobMasterConfigUtil.loadConfig(fetchFn);
        const apiUrl = JobMasterConfigUtil.getClusterApiUrl(clusterId);
        return ApiClientUtil.CreateApiClient(apiUrl, fetchFn);
    }
}
