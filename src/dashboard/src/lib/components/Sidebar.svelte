<script lang="ts">
    import { page } from "$app/state";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";
    import AppLogo from "$lib/components/AppLogo.svelte";

    let isHovered = $state(false);

    const menuItems = [
        { name: "Dashboard", path: "/", iconClass: "fa-solid fa-gauge-high" },
        { name: "Jobs", path: "/jobs", iconClass: "fa-solid fa-gears" },
        { name: "Recurring Schedules", path: "/recurring-schedules", iconClass: "fa-solid fa-clock" },
        { name: "Hosts", path: "/hosts", iconClass: "fa-solid fa-server" },
        { name: "Workers", path: "/workers", iconClass: "fa-solid fa-microchip" },
        { name: "Buckets", path: "/buckets", iconClass: "fa-solid fa-layer-group" },
        { name: "Agent Connections", path: "/agent-connections", iconClass: "fa-solid fa-plug" },
    ];

    function resolveHref(path: string) {
        const cluster = page.params.cluster;
        return JobMasterConfigUtil.resolveHref(path, cluster);
    }
</script>

<aside
        class="
        h-screen
        bg-base-200
        border-r border-base-300
        transition-all duration-300
        z-30
        flex flex-col
        shrink-0
        {isHovered ? 'w-64' : 'w-20'}
    "
        onmouseenter={() => (isHovered = true)}
        onmouseleave={() => (isHovered = false)}
>
    <!-- Logo -->
    <a href={resolveHref("/")} class="h-20 flex items-center px-4 overflow-hidden border-b border-base-300 cursor-pointer">
        <div class="flex items-center min-w-[50px]">
            <AppLogo class="h-10 w-10 shrink-0 transition-transform duration-300 {isHovered ? '-rotate-12' : ''}" />
        </div>

        {#if isHovered}
            <div class="ml-3 animate-in fade-in slide-in-from-left-2 duration-300">
                <h1 class="text-xl tracking-tighter leading-none flex items-baseline">
                    <span class="font-light text-base-content">Job</span>
                    <span class="font-extrabold text-base-content">Master</span>
                </h1>
                <p class="text-[9px] font-mono opacity-40 tracking-widest mt-1 font-bold">
                    .net orchestrator
                </p>
            </div>
        {/if}
    </a>

    <!-- Navigation -->
    <nav class="flex-1 px-3 space-y-1.5 mt-6">
        {#each menuItems as item}
            <a
                    href={resolveHref(item.path)}
                    class="
                    flex items-center
                    h-12 px-3
                    rounded-xl
                    hover:bg-base-300
                    active:bg-primary
                    group
                    transition-colors
                "
            >
                <div
                        class="
                        min-w-[32px]
                        flex justify-center
                        text-base-content/50
                        group-hover:text-primary
                        transition-colors
                    "
                >
                    <i class="{item.iconClass} text-lg"></i>
                </div>

                {#if isHovered}
                    <span
                            class="
                            ml-4
                            font-mono font-bold text-[11px]
                            tracking-wider
                            whitespace-nowrap
                            animate-in fade-in
                            text-base-content/80
                            group-hover:text-base-content
                        "
                    >
                        {item.name}
                    </span>
                {/if}
            </a>
        {/each}
    </nav>
</aside>

<!-- Layout spacer -->
<div class="transition-all duration-300 {isHovered ? 'w-64' : 'w-20'}"></div>
