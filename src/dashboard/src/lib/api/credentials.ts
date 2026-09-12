export type Credentials = {
	type: "API_KEY" | "JWT_SIMPLE" | "JWT_CUSTOM_FORM" | "OAUTH" | "USER_PASSWORD";
	secretValue?: string;
	userName?: string;
	userPassword?: string;
	// A friendly name for "Signed in as ..." in the app shell: USER_PASSWORD is the typed
	// username, API_KEY comes from GET /whoami, and JWT_SIMPLE/JWT_CUSTOM_FORM/OAUTH all
	// decode the "sub" claim of the JWT the browser actually holds.
	displayName?: string;
};

/**
 * Decodes a JWT's payload and returns its "sub" claim, without verifying the signature —
 * this only drives a display label, never an auth decision.
 */
export function decodeJwtSubject(token: string): string | undefined {
	try {
		const payload = token.split(".")[1];
		if (!payload) return undefined;
		const base64 = payload.replace(/-/g, "+").replace(/_/g, "/");
		const json = decodeURIComponent(
			atob(base64)
				.split("")
				.map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0"))
				.join("")
		);
		const claims = JSON.parse(json);
		return typeof claims.sub === "string" ? claims.sub : undefined;
	} catch {
		return undefined;
	}
}
