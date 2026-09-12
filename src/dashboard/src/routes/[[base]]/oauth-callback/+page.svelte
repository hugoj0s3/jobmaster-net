<script lang="ts">
    import { onMount } from "svelte";
    import { AuthRetentionUtil } from "$lib/api/auth-retention-util";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";
    import { decodeJwtSubject, type Credentials } from "$lib/api/credentials";

    let error = $state<string | null>(null);

    onMount(async () => {
        const url = new URL(window.location.href);
        const code = url.searchParams.get("code");
        const state = url.searchParams.get("state");
        const idpError = url.searchParams.get("error");

        if (idpError) {
            error = `Sign-in was cancelled or failed: ${idpError}`;
            return;
        }
        if (!code) {
            error = "Missing authorization code.";
            return;
        }

        try {
            await JobMasterConfigUtil.loadConfig();
            const basePath = JobMasterConfigUtil.getBasePath();

            const res = await fetch(`${basePath}/oauth/confirm`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ code, state })
            });

            if (!res.ok) {
                const body = await res.json().catch(() => null);
                throw new Error(body?.error ?? `Sign-in failed (${res.status})`);
            }

            const { token } = await res.json();
            if (!token) throw new Error("No token returned");

            const credentials: Credentials = { type: "OAUTH", secretValue: token, displayName: decodeJwtSubject(token) };
            await AuthRetentionUtil.storeCredentials(credentials);

            window.location.href = JobMasterConfigUtil.resolveHref("/");
        } catch (err) {
            error = err instanceof Error ? err.message : "Sign-in failed";
        }
    });
</script>

<main class="flex min-h-screen items-center justify-center bg-base-200 text-base-content">
    <div class="mx-auto w-full max-w-md px-6 text-center">
        {#if error}
            <div class="alert alert-error text-sm py-2 mb-4">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <span>{error}</span>
            </div>
            <a href={JobMasterConfigUtil.resolveHref("/")} class="btn btn-primary">Back to sign in</a>
        {:else}
            <span class="loading loading-infinity loading-lg text-primary"></span>
            <p class="mt-2 text-sm text-base-content/60">Signing you in...</p>
        {/if}
    </div>
</main>
