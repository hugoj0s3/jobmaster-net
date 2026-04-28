<script lang="ts">
    import { onMount } from "svelte";
    import { goto } from "$app/navigation";
    
    let config = $state<any>(null);

    onMount(async () => {
        const res = await fetch("/jobmaster-config.json");
        config = await res.json();
    });

    function selectCluster(id: string) {
        return goto(`/${id}/dashboard`, { keepFocus: true, noScroll: true });
    }
</script>

{#if !config}
    <div class="flex h-screen items-center justify-center bg-base-200 text-base-content">
        <span class="loading loading-infinity loading-lg text-primary"></span>
    </div>
{:else}
    <div class="min-h-screen bg-base-200 text-base-content">
        <div class="mx-auto w-full max-w-xl px-6 py-10">
            <h1 class="text-2xl font-semibold">Select a cluster</h1>
            <p class="mt-2 opacity-70">Choose the environment you want to browse.</p>

            <div class="mt-6 space-y-2">
                {#each config.clusters as cluster}
                    <button
                            class="btn btn-block justify-start border border-base-300 bg-base-100 hover:bg-base-300"
                            onclick={() => selectCluster(cluster.id)}
                    >
                        <div class="flex flex-col items-start">
                            <div class="font-mono font-bold text-sm">{cluster.id}</div>
                            <div class="text-xs opacity-70">{cluster.environmentName}</div>
                        </div>
                    </button>
                {/each}
            </div>
        </div>
    </div>
{/if}
