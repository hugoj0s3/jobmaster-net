// TODO we shouldn't store secrets in session storage. we will refactor this later.

export type SecretAuthProvider =
    | { type: "API_KEY"; headerName?: string }
    | { type: "USER_PASSWORD"; userHeaderName?: string; passwordHeaderName?: string }
    | { type: "JWT_SIMPLE"; headerName?: string; scheme?: string }
    | { type: "JWT_CUSTOM_FORM"; transport?: { header?: string; scheme?: string } };

export type SecretKeys = {
    apiKey: string | null;
    user: string | null;
    pwd: string | null;
    jwt: string | null;
    authProvider: SecretAuthProvider | null;
};

function safeGetSessionItem(key: string): string | null {
    try {
        if (typeof sessionStorage === "undefined") return null;
        return sessionStorage.getItem(key);
    } catch {
        return null;
    }
}

function safeGetAuthProvider(): SecretAuthProvider | null {
    const raw = safeGetSessionItem("jm_auth_provider");
    if (!raw) return null;

    try {
        return JSON.parse(raw) as SecretAuthProvider;
    } catch {
        return null;
    }
}

export async function getSecretApiKey(): Promise<string | null> {
    return safeGetSessionItem("jm_api_key");
}

export async function getSecretUser(): Promise<string | null> {
    return safeGetSessionItem("jm_user");
}

export async function getSecretPwd(): Promise<string | null> {
    return safeGetSessionItem("jm_pwd");
}

export async function getSecretJwt(): Promise<string | null> {
    return safeGetSessionItem("jm_jwt");
}

export async function getSecretAuthProvider(): Promise<SecretAuthProvider | null> {
    return safeGetAuthProvider();
}

export async function getAllSecrets(): Promise<SecretKeys> {
    const [apiKey, user, pwd, jwt] = await Promise.all([
        getSecretApiKey(),
        getSecretUser(),
        getSecretPwd(),
        getSecretJwt()
    ]);

    return {
        apiKey,
        user,
        pwd,
        jwt,
        authProvider: safeGetAuthProvider()
    };
}
