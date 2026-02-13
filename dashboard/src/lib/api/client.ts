import createClient from "openapi-fetch";
import type { paths } from "$lib/api/schema";
import { getAllSecrets } from "$lib/secret/secrets";

type AuthProvider =
    | { type: "API_KEY"; headerName?: string }
    | { type: "USER_PASSWORD"; userHeaderName?: string; passwordHeaderName?: string }
    | { type: "JWT_CUSTOM_FORM"; transport?: { header?: string; scheme?: string } }
    | { type: "JWT_SSO"; transport?: { header?: string; scheme?: string } };

async function buildAuthHeaders(): Promise<Record<string, string>> {
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

function normalizeBaseUrl(apiBaseUrl: string | null | undefined): string {
    const base = (apiBaseUrl ?? "").trim();
    if (!base) return "";

    // OpenAPI paths in schema.d.ts already include /jm-api.
    // If the config points to that segment, strip it to avoid duplicating.
    return base.replace(/\/?jm-api\/?$/i, "");
}

export function createJobMasterClient(apiBaseUrl: string | null | undefined, fetchFn: typeof fetch) {
    const baseUrl = normalizeBaseUrl(apiBaseUrl);

    return createClient<paths>({
        baseUrl,
        fetch: async (input, init) => {
            const authHeaders = await buildAuthHeaders();
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
