type ApiCluster = {
	clusterId: string;
	transientThreshold: string; // vem como string de TimeSpan (ex: "00:10:00")
};

type ApiJob = {
	id: string;
	jobDefinitionId: string;
	status: number;
	createdAt?: string;
	scheduledAt?: string;
	processingStartedAt?: string;
	succeedExecutedAt?: string;
};


export async function fetchJson<T>(url: string): Promise<T> {
	const headers: Record<string, string> = {};

	const apiKey = sessionStorage.getItem("jm_api_key");
	if (apiKey) {
		headers["X-JobMaster-Key"] = apiKey;
		headers["x-api-key"] = apiKey;
	}

	const user = sessionStorage.getItem("jm_user");
	const pwd = sessionStorage.getItem("jm_pwd");
	if (user && pwd) {
		headers["X-JobMaster-User"] = user;
		headers["X-JobMaster-Pwd"] = pwd;

		headers["X-User-Name"] = user;
		headers["X-Password"] = pwd;
	}

	const jwt = sessionStorage.getItem("jm_jwt");
	if (jwt) headers["Authorization"] = `Bearer ${jwt}`;

	const res = await fetch(url, { headers });
	if (!res.ok) throw new Error(`${res.status} ${res.statusText} - ${url}`);
	return (await res.json()) as T;
}