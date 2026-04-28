<script lang="ts">
    let { config, currentCluster, onSelectCluster } = $props();
</script>

<header
        class="
        h-14
        navbar
        bg-base-100
        border-b border-base-300
        px-4
        text-base-content
    "
>
    <!-- Left -->
    <div class="flex-1 gap-3">
        <label for="main-drawer" class="btn btn-ghost btn-sm lg:hidden">
            <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" fill="none"
                 viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                      d="M4 6h16M4 12h16M4 18h7" />
            </svg>
        </label>

        <div class="flex flex-col leading-tight">
            <span class="text-[10px] font-bold opacity-40 tracking-wide">
                Cluster Context
            </span>
            <span class="text-sm font-mono font-bold text-primary">
                {currentCluster?.environmentName}
            </span>
        </div>
    </div>

    <!-- Right -->
    <div class="flex-none">
        <div class="dropdown dropdown-end">
            <button
                    tabindex="0"
                    class="
                    btn btn-sm btn-ghost
                    border border-base-300
                    bg-base-100
                    hover:bg-base-200
                "
            >
                Switch Cluster
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 opacity-50"
                     fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M19 9l-7 7-7-7" />
                </svg>
            </button>

            <ul
                    tabindex="0"
                    class="
                    dropdown-content
                    menu
                    p-2
                    mt-2
                    w-64
                    rounded-box
                    shadow-xl
                    bg-base-200
                    border border-base-300
                    z-[100]
                "
            >
                <li class="menu-title text-[11px] opacity-40">
                    Available Environments
                </li>

                {#each config.clusters as cluster}
                    <li>
                        <button
                                class="
                                flex justify-between items-center
                                {currentCluster.id === cluster.id ? 'active' : ''}
                            "
                                onclick={() => onSelectCluster(cluster.id)}
                        >
                            <span class="font-mono text-sm">
                                {cluster.environmentName}
                            </span>

                            {#if currentCluster.id === cluster.id}
                                <span class="badge badge-xs badge-primary"></span>
                            {/if}
                        </button>
                    </li>
                {/each}
            </ul>
        </div>
    </div>
</header>
