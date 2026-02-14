import createClient from "openapi-fetch";
import type { paths } from "$lib/api/schema";
import { getAllSecrets } from "$lib/secret/secrets";

type AuthProvider =
    | { type: "API_KEY"; headerName?: string }
    | { type: "USER_PASSWORD"; userHeaderName?: string; passwordHeaderName?: string }
    | { type: "JWT_CUSTOM_FORM"; transport?: { header?: string; scheme?: string } }
    | { type: "JWT_SSO"; transport?: { header?: string; scheme?: string } };

type JobmasterConfig = {
    apiBaseUrl: string;
};

export class ApiClientUtil {
    private static configCache: JobmasterConfig | null = null;

    private static async loadJobmasterConfig(fetchFn: typeof fetch): Promise<JobmasterConfig> {
        if (ApiClientUtil.configCache) return ApiClientUtil.configCache;

        const res = await fetchFn("/jobmaster-config.json");
        if (!res.ok) throw new Error(`${res.status} ${res.statusText} - /jobmaster-config.json`);
        const cfg = (await res.json()) as JobmasterConfig;
        ApiClientUtil.configCache = cfg;
        return cfg;
    }
    
    private static async buildAuthHeaders(): Promise<Record<string, string>> {
        const headers: Record<string, string> = {};
        const { apiKey, user, pwd, jwt, authProvider } = await getAllSecrets();
        const provider = authProvider as AuthProvider | null;

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
            const transportHeader = provider?.transport?.header ?? "Authorization";
            const scheme = provider?.transport?.scheme;
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
            fetch: async (input, init) => {
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

    static async CreateApiClientFromConfig(fetchFn: typeof fetch) {
        const cfg = await ApiClientUtil.loadJobmasterConfig(fetchFn);
        return ApiClientUtil.CreateApiClient(cfg.apiBaseUrl, fetchFn);
    }
}
