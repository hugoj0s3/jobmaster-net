<script lang="ts">
    import { page } from "$app/stores";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";

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
        const cluster = $page.params.cluster;
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
            <svg viewBox="0 0 200 200" class="h-10 w-10 shrink-0 transition-transform duration-300 {isHovered ? '-rotate-12' : ''}" xmlns="http://www.w3.org/2000/svg">
                    <rect x="10" y="10" width="180" height="180" rx="40" ry="40" style="fill: var(--color-primary)" />
                    <g style="stroke: var(--color-primary-content); stroke-opacity: 0.12; stroke-width: 1.2" fill="none">
                        <line x1="60" y1="30" x2="60" y2="170"/>
                        <line x1="100" y1="30" x2="100" y2="170"/>
                        <line x1="140" y1="30" x2="140" y2="170"/>
                        <line x1="30" y1="60" x2="170" y2="60"/>
                        <line x1="30" y1="100" x2="170" y2="100"/>
                        <line x1="30" y1="140" x2="170" y2="140"/>
                    </g>
                    <g style="fill: var(--color-primary-content); opacity: 0.25">
                        <circle cx="60" cy="60" r="3"/><circle cx="100" cy="60" r="3"/><circle cx="140" cy="60" r="3"/>
                        <circle cx="60" cy="100" r="3"/><circle cx="140" cy="100" r="3"/>
                        <circle cx="60" cy="140" r="3"/><circle cx="100" cy="140" r="3"/><circle cx="140" cy="140" r="3"/>
                    </g>
                    <text x="100" y="118" text-anchor="middle" font-family="'Inter','Segoe UI',system-ui,sans-serif" font-weight="800" font-size="72" letter-spacing="-2" style="fill: var(--color-primary-content)">JM</text>
                </svg>
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
