<script lang="ts">
    import { onMount } from "svelte";
    import logoSvg from "$lib/assets/jobmaster-logo.svg";
    
    interface ClusterConfig {
        clusters: { id: string; environmentName: string }[];
    }

    let config = $state<ClusterConfig | null>(null);

    onMount(async () => {
        const res = await fetch("/jobmaster-config.json");
        config = await res.json();
    });
</script>

{#if !config}
    <div class="flex h-screen items-center justify-center bg-base-200">
        <span class="loading loading-infinity loading-lg"></span>
    </div>
{:else}
    <div class="flex min-h-screen items-center justify-center bg-base-200">
        <div class="mx-auto w-full max-w-md px-6">
            <div class="flex flex-col items-center text-center">
                <img src={logoSvg} alt="JobMaster" class="mb-6 h-20 w-20" />
                <h1 class="text-3xl font-bold tracking-tight">
                    Job<span class="text-primary">Master</span>
                </h1>
                <p class="mt-2 text-sm text-base-content/60">Select a cluster to continue</p>
            </div>

            <div class="divider mt-8 mb-6"></div>

            <div class="space-y-3">
                {#each config.clusters as cluster (cluster.id)}
                    <a
                            href="/{cluster.id}/dashboard"
                            class="btn btn-block justify-between"
                    >
                        <div class="flex flex-col items-start gap-0.5">
                            <div class="font-medium">{cluster.id}</div>
                            <div class="text-xs opacity-60">{cluster.environmentName}</div>
                        </div>
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7"/>
                        </svg>
                    </a>
                {/each}
            </div>

            <p class="mt-10 text-center text-sm text-base-content/60">
                {config.clusters.length} cluster{config.clusters.length !== 1 ? 's' : ''} available
            </p>
        </div>
    </div>
{/if}
