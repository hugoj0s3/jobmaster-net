<script lang="ts">
    let isHovered = $state(false);

    const menuItems = [
        { name: "Dashboard", path: "/", iconClass: "fa-solid fa-gauge-high" },
        { name: "Workers", path: "/workers", iconClass: "fa-solid fa-microchip" },
        { name: "Jobs", path: "/jobs", iconClass: "fa-solid fa-list-check" },
        { name: "Buckets", path: "/buckets", iconClass: "fa-solid fa-database" },
    ];
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
    <div class="h-20 flex items-center px-4 overflow-hidden border-b border-base-300">
        <div class="flex items-center min-w-[50px]">
            <div
                    class="w-10 h-10 bg-accent text-accent-content rounded-xl flex items-center justify-center font-black text-lg transition-transform duration-500 shadow-lg shadow-accent/20"
                    style="transform: rotate({isHovered ? '-6deg' : '0deg'}); font-family: 'JetBrains Mono'; letter-spacing: -1px;"
            >
                JM
            </div>

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
    </div>

    <!-- Navigation -->
    <nav class="flex-1 px-3 space-y-1.5 mt-6">
        {#each menuItems as item}
            <a
                    href={item.path}
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
