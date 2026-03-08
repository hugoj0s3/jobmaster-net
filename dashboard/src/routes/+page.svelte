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
    <div class="flex h-screen items-center justify-center bg-base-200 text-base-content">
        <span class="loading loading-infinity loading-lg text-primary"></span>
    </div>
{:else}
    <div class="flex min-h-screen items-center justify-center bg-base-200 text-base-content">
        <div class="mx-auto w-full max-w-md px-6">
            <div class="flex flex-col items-center text-center">
                <img src={logoSvg} alt="JobMaster" class="h-20 w-20 drop-shadow-lg" />
                <h1 class="mt-4 text-3xl font-extrabold tracking-tight">
                    Job<span class="text-primary">Master</span>
                </h1>
                <p class="mt-1 text-sm opacity-50 font-mono">Dashboard</p>
            </div>

            <div class="divider mt-6 mb-4 text-xs opacity-40">SELECT A CLUSTER</div>

            <div class="space-y-2">
                {#each config.clusters as cluster (cluster.id)}
                    <a
                            href="/{cluster.id}/dashboard"
                            class="group btn btn-block justify-between border border-base-300 bg-base-100 hover:border-primary/40 hover:bg-base-100 transition-all duration-150"
                    >
                        <div class="flex flex-col items-start gap-0.5">
                            <div class="font-mono font-bold text-sm">{cluster.id}</div>
                            <div class="text-xs opacity-50">{cluster.environmentName}</div>
                        </div>
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-0 group-hover:opacity-60 transition-opacity" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7"/>
                        </svg>
                    </a>
                {/each}
            </div>

            <p class="mt-8 text-center text-[11px] opacity-30 font-mono">
                {config.clusters.length} cluster{config.clusters.length !== 1 ? 's' : ''} available
            </p>
        </div>
    </div>
{/if}
