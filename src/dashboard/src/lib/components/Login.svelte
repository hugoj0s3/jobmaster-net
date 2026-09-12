<script lang="ts">
    import AppLogo from "$lib/components/AppLogo.svelte";
    import { AuthRetentionUtil } from "$lib/api/auth-retention-util";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";
    import { decodeJwtSubject, type Credentials } from "$lib/api/credentials";

    let { auth, onLogin } = $props();

    // Every OAuth provider collapses into one "OAuth" tab showing all of them stacked together,
    // rather than each provider getting its own tab — keeps the tab strip from growing unbounded
    // as more providers are added, and matches the conventional social-login pattern of showing
    // every "Sign in with X" option at once.
    let oauthProviders = $derived((auth.providers ?? []).filter((p: any) => p.type === "OAUTH"));
    let otherProviders = $derived((auth.providers ?? []).filter((p: any) => p.type !== "OAUTH"));
    let tabs = $derived([
        ...otherProviders,
        ...(oauthProviders.length > 0 ? [{ type: "OAUTH_GROUP", displayName: auth.oAuthTabLabel ?? "OAuth", providers: oauthProviders }] : [])
    ]);

    let selectedProvider = $state(tabs[0]);

    let apiKey = $state("");
    let user = $state("");
    let pwd = $state("");
    let jwtToken = $state("");

    let jwtFieldValues = $state<Record<string, string>>({});

    let loginError = $state<string | null>(null);

    async function validateCredentials(credentials: Credentials) {
        const isValid = await ApiClientUtil.ValidateCredentials(credentials, fetch);
        if (!isValid) {
            throw new Error("Invalid credentials");
        }
    }

    async function storeSecretCredential(secretValue: string): Promise<Credentials | undefined> {
        if (!selectedProvider) return undefined;

        const credentials: Credentials = {
            type: selectedProvider.type,
            secretValue
        };

        if (selectedProvider.type === "JWT_SIMPLE" || selectedProvider.type === "JWT_CUSTOM_FORM") {
            credentials.displayName = decodeJwtSubject(secretValue);
        }

        await validateCredentials(credentials);

        if (selectedProvider.type === "API_KEY") {
            credentials.displayName = await ApiClientUtil.GetWhoAmI(credentials, fetch);
        }

        await AuthRetentionUtil.storeCredentials(credentials);
        return credentials;
    }

    let isSubmitting = $state(false);
    let oauthError = $state<string | null>(null);

    async function startOAuthLogin(provider: { key: string }) {
        oauthError = null;
        try {
            const res = await fetch(`${JobMasterConfigUtil.getBasePath()}/oauth/${provider.key}`);
            if (!res.ok) throw new Error(`Failed to start login (${res.status})`);
            const { url } = await res.json();
            window.location.href = url;
        } catch (err) {
            oauthError = err instanceof Error ? err.message : "Login failed";
        }
    }

    async function handleSubmit(e: SubmitEvent) {
        e.preventDefault();
        loginError = null;
        isSubmitting = true;

        AuthRetentionUtil.clear();

        let loggedInCredentials: Credentials | undefined;

        try {
            if (selectedProvider?.type === "API_KEY") {
                loggedInCredentials = await storeSecretCredential(apiKey);

            } else if (selectedProvider?.type === "JWT_SIMPLE") {
                loggedInCredentials = await storeSecretCredential(jwtToken);

            } else if (selectedProvider?.type === "USER_PASSWORD") {
                const credentials: Credentials = {
                    type: "USER_PASSWORD",
                    userName: user,
                    userPassword: pwd,
                    displayName: user
                };

                await validateCredentials(credentials);
                await AuthRetentionUtil.storeCredentials(credentials);
                loggedInCredentials = credentials;

            } else if (selectedProvider?.type === "JWT_CUSTOM_FORM") {
                const body = new URLSearchParams();
                for (const [k, v] of Object.entries(jwtFieldValues)) body.append(k, v ?? "");

                const res = await fetch(selectedProvider.tokenUrl!, {
                    method: "POST",
                    headers: { "Content-Type": "application/x-www-form-urlencoded" },
                    body: body.toString()
                });

                if (!res.ok) throw new Error(`Login failed (${res.status})`);

                const data = await res.json();
                const token: string = data.token ?? data.access_token ?? data.jwt;

                if (!token) throw new Error("No token in response");

                loggedInCredentials = await storeSecretCredential(token);
            }

            // Explicitly pass back whatever this login used (or nothing), rather than leaving
            // the parent's "signed in as" state at whatever it happened to be from a previous
            // login in this same page session (e.g. a prior OAuth login) — every non-OAuth type
            // has no displayName, so this always correctly resets it.
            onLogin(loggedInCredentials?.displayName ?? null);
        } catch (err) {
            loginError = err instanceof Error ? err.message : "Login failed";
        } finally {
            isSubmitting = false;
        }
    }
</script>

<main class="flex min-h-screen items-center justify-center bg-base-200 text-base-content">
    <div class="mx-auto w-full max-w-md px-6">
        <div class="flex flex-col items-center text-center">
            <div class="flex items-center gap-3">
                <h1 class="text-3xl tracking-tight leading-none flex items-baseline">
                    <span class="font-light text-base-content">Job</span><span class="font-extrabold text-base-content">Master</span>
                </h1>
                <AppLogo class="h-11 w-11 -rotate-12" />
            </div>
            <p class="mt-2 text-sm text-base-content/60">Sign in to continue</p>
        </div>

        <div class="divider mt-8 mb-6"></div>

        {#if tabs.length > 1}
            <div class="tabs tabs-boxed mb-6 flex">
                {#each tabs as tab}
                    <button
                        type="button"
                        class="tab flex-1 {selectedProvider === tab ? 'tab-active' : ''}"
                        onclick={() => { selectedProvider = tab; loginError = null; oauthError = null; }}
                    >
                        {tab.displayName ?? tab.type}
                    </button>
                {/each}
            </div>
        {/if}

        <form onsubmit={handleSubmit} class="space-y-4">
            {#if selectedProvider?.type === "API_KEY"}
                <label class="form-control w-full">
                    <div class="label"><span class="label-text">API Key</span></div>
                    <input
                        type="password"
                        class="input input-bordered w-full"
                        placeholder="Enter your API key"
                        bind:value={apiKey}
                        required
                    />
                </label>

            {:else if selectedProvider?.type === "JWT_SIMPLE"}
                <label class="form-control w-full">
                    <div class="label"><span class="label-text">JWT Token</span></div>
                    <textarea
                        class="textarea textarea-bordered w-full h-24 font-mono text-xs"
                        placeholder="Paste your JWT token here..."
                        bind:value={jwtToken}
                        required
                    ></textarea>
                </label>

            {:else if selectedProvider?.type === "USER_PASSWORD"}
                <label class="form-control w-full">
                    <div class="label"><span class="label-text">Username</span></div>
                    <input
                        type="text"
                        class="input input-bordered w-full"
                        placeholder="Username"
                        bind:value={user}
                        required
                    />
                </label>
                <label class="form-control w-full">
                    <div class="label"><span class="label-text">Password</span></div>
                    <input
                        type="password"
                        class="input input-bordered w-full"
                        placeholder="Password"
                        bind:value={pwd}
                        required
                    />
                </label>

            {:else if selectedProvider?.type === "JWT_CUSTOM_FORM"}
                {#each selectedProvider.fields ?? [] as field (field.id)}
                    <label class="form-control w-full">
                        <div class="label"><span class="label-text">{field.label}</span></div>
                        <input
                            type={field.type}
                            class="input input-bordered w-full"
                            placeholder={field.label}
                            value={jwtFieldValues[field.id] ?? field.defaultValue ?? ""}
                            oninput={(e) => { jwtFieldValues[field.id] = (e.currentTarget as HTMLInputElement).value; }}
                            required={field.isRequired}
                            disabled={field.disabled}
                        />
                    </label>
                {/each}
            {/if}

            {#if loginError}
                <div class="alert alert-error text-sm py-2">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <span>{loginError}</span>
                </div>
            {/if}

            {#if selectedProvider?.type !== "OAUTH_GROUP"}
                <button type="submit" class="btn btn-primary btn-block mt-2" disabled={isSubmitting}>
                    {#if isSubmitting}
                        <span class="loading loading-spinner loading-sm"></span>
                    {/if}
                    Sign In
                </button>
            {/if}
        </form>

        {#if selectedProvider?.type === "OAUTH_GROUP"}
            <div class="space-y-2">
                {#each selectedProvider.providers as provider (provider.key)}
                    <button
                        type="button"
                        class="btn btn-block"
                        style={`${provider.backgroundColor ? `background-color:${provider.backgroundColor};` : ""}${provider.foregroundColor ? `color:${provider.foregroundColor};` : ""}`}
                        onclick={() => startOAuthLogin(provider)}
                    >
                        {#if provider.icon}
                            <img src={provider.icon} alt="" class="h-5 w-5" />
                        {/if}
                        {provider.displayName ?? `Sign in with ${provider.key}`}
                    </button>
                {/each}
            </div>

            {#if oauthError}
                <div class="alert alert-error text-sm py-2 mt-2">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                    <span>{oauthError}</span>
                </div>
            {/if}
        {/if}
    </div>
</main>