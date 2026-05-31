<script lang="ts">
    import AppLogo from "$lib/components/AppLogo.svelte";
    import { AuthRetentionUtil } from "$lib/api/auth-retention-util";
    import { ApiClientUtil } from "$lib/api/api-client-util";
    import type { Credentials } from "$lib/api/credentials";

    let { auth, onLogin } = $props();

    let selectedProvider = $state(auth.providers?.[0]);

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

    async function storeSecretCredential(secretValue: string) {
        if (!selectedProvider) return;

        const credentials: Credentials = {
            type: selectedProvider.type,
            secretValue
        };

        await validateCredentials(credentials);
        await AuthRetentionUtil.storeCredentials(credentials);
    }

    let isSubmitting = $state(false);

    async function handleSubmit(e: SubmitEvent) {
        e.preventDefault();
        loginError = null;
        isSubmitting = true;

        AuthRetentionUtil.clear();

        try {
            if (selectedProvider?.type === "API_KEY") {
                await storeSecretCredential(apiKey);

            } else if (selectedProvider?.type === "JWT_SIMPLE") {
                await storeSecretCredential(jwtToken);

            } else if (selectedProvider?.type === "USER_PASSWORD") {
                const credentials: Credentials = {
                    type: "USER_PASSWORD",
                    userName: user,
                    userPassword: pwd
                };

                await validateCredentials(credentials);
                await AuthRetentionUtil.storeCredentials(credentials);

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

                await storeSecretCredential(token);
            }

            onLogin();
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
            <AppLogo class="mb-6 h-20 w-20" />
            <h1 class="text-3xl tracking-tight leading-none flex items-baseline">
                <span class="font-light text-base-content">Job</span><span class="font-extrabold text-base-content">Master</span>
            </h1>
            <p class="mt-2 text-sm text-base-content/60">Sign in to continue</p>
        </div>

        <div class="divider mt-8 mb-6"></div>

        {#if (auth.providers?.length ?? 0) > 1}
            <div class="tabs tabs-boxed mb-6 flex">
                {#each auth.providers as provider}
                    <button
                        type="button"
                        class="tab flex-1 {selectedProvider === provider ? 'tab-active' : ''}"
                        onclick={() => { selectedProvider = provider; loginError = null; }}
                    >
                        {provider.displayName ?? provider.type}
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

            <button type="submit" class="btn btn-primary btn-block mt-2" disabled={isSubmitting}>
                {#if isSubmitting}
                    <span class="loading loading-spinner loading-sm"></span>
                {/if}
                Sign In
            </button>
        </form>
    </div>
</main>