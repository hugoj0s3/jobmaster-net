import createClient from "openapi-fetch";
import type { paths } from "$lib/api/schema";
import { getAllSecrets } from "$lib/secret/secrets";

type AuthProvider =
    | { type: "API_KEY"; headerName?: string }
    | { type: "USER_PASSWORD"; userHeaderName?: string; passwordHeaderName?: string }
    | { type: "JWT_CUSTOM_FORM"; transport?: { header?: string; scheme?: string } }
    | { type: "JWT_SSO"; transport?: { header?: string; scheme?: string } };

export class ApiClientUtil {
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
        const base = (apiBaseUrl ?? "").trim();
        if (!base) return "";

        return base.replace(/\/?jm-api\/?$/i, "");
    }

    static CreateApiClient(apiBaseUrl: string | null | undefined, fetchFn: typeof fetch) {
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
}
