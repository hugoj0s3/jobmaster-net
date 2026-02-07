<!-- src/routes/+layout.svelte -->
<script lang="ts">
    import "../app.css";
    import { onMount } from "svelte";
    import Login from "$lib/components/Login.svelte";
    import Sidebar from "$lib/components/Sidebar.svelte";

    import { resolveThemeId, setStoredTheme } from "$lib/theme-helper";

    let { children } = $props();

    let config = $state<any>(null);
    let isLoggedIn = $state(false);
    let currentCluster = $state<any>(null);
    let currentTheme = $state<any>(null);

    onMount(async () => {
        const res = await fetch("/jobmaster-config.json");
        config = await res.json();

        if (sessionStorage.getItem("jm_auth") === "true") isLoggedIn = true;
        if (config?.clusters?.length > 0) handleClusterChange(config.clusters[0].id);
    });

    function handleClusterChange(id: string) {
        currentCluster = config.clusters.find((c: any) => c.id === id);
        const themeId = resolveThemeId(id, config);
        applyTheme(themeId);
    }

    // 1. Definimos o mapeamento fora para ser usado na limpeza e na aplicação
    const themeVarMap: Record<string, string> = {
        primary: "--color-primary",
        primaryContent: "--color-primary-content",
        secondary: "--color-secondary",
        secondaryContent: "--color-secondary-content",
        accent: "--color-accent",
        accentContent: "--color-accent-content",
        base100: "--color-base-100",
        base200: "--color-base-200",
        base300: "--color-base-300",
        baseContent: "--color-base-content",
    };

    function applyTheme(themeId: string, persistForCluster = false) {
        const theme = config?.themes?.find((t: any) => t.id === themeId);
        if (!theme) return;

        currentTheme = theme;
        const base = theme.baseTheme ?? "jobmaster-light";

        // 2. Aplicar o atributo do DaisyUI
        document.documentElement.setAttribute("data-theme", base);

        // 3. Ajustar o esquema de cores do browser
        document.documentElement.style.colorScheme =
            base.includes("dark") || base === "night" || base === "business" || base === "dracula" ? "dark" : "light";

        const root = document.documentElement.style;

        Object.values(themeVarMap).forEach(cssVar => root.removeProperty(cssVar));

        if (theme.overrides) {
            for (const [key, value] of Object.entries(theme.overrides)) {
                const cssVar = themeVarMap[key];
                if (cssVar) {
                    root.setProperty(cssVar, value as string);
                    
                    console.log(`Override aplicado: ${cssVar} = ${value}`);
                }
            }
        }
    }

    function logout() {
        sessionStorage.removeItem("jm_auth");
        isLoggedIn = false;
    }
</script>

{#if !config}
    <div class="flex h-screen items-center justify-center bg-base-200 text-base-content">
        <span class="loading loading-infinity loading-lg text-primary"></span>
    </div>

{:else if !isLoggedIn}
    <Login auth={config.auth} onLogin={() => (isLoggedIn = true)} />

{:else}
    <div class="flex h-screen overflow-hidden bg-base-100 text-base-content">
        <Sidebar />

        <div class="flex-1 flex flex-col min-w-0">
            <header class="h-14 border-b border-base-300 bg-base-100 flex items-center justify-center px-4 shrink-0">
                <div class="dropdown dropdown-end">
                    <button
                            tabindex="0"
                            class="btn btn-ghost btn-sm h-9 px-3 flex items-center gap-3 border border-base-300 bg-base-100 hover:bg-base-200"
                    >
                        <div class="flex items-center gap-2">
                            <span class="text-[12px] font-mono opacity-50">ADMIN</span>
                            <span class="text-[14px] font-mono font-bold px-1.5 py-0.5 rounded leading-none">
                {currentCluster?.id} {currentCluster?.environmentName}
              </span>
                        </div>

                        <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 opacity-30" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" />
                        </svg>
                    </button>

                    <ul
                            tabindex="0"
                            class="dropdown-content menu p-2 shadow-2xl bg-base-200 rounded-box w-72 mt-2 border border-base-300 z-[100] space-y-2"
                    >
                        <li class="menu-title text-[12px] font-black opacity-40">Cluster</li>
                        {#each config.clusters as cluster}
                            <li>
                                <button
                                        class="flex flex-col items-start py-2 {currentCluster.id === cluster.id ? 'active' : ''}"
                                        onclick={() => handleClusterChange(cluster.id)}
                                >
                  <span class="text-[12px] font-mono font-bold">
                    {cluster.id} {cluster.environmentName}
                  </span>
                                </button>
                            </li>
                        {/each}

                        <div class="divider my-0"></div>

                        <li class="menu-title text-[12px] font-black opacity-40">Appearance</li>
                        <div class="grid grid-cols-2 gap-1 p-2">
                            {#each config.themes as theme}
                                <button
                                        class="btn btn-xs font-mono text-[12px] {currentTheme.id === theme.id ? 'btn-primary' : 'btn-ghost border-base-300'}"
                                        onclick={() => applyTheme(theme.id, true)}
                                >
                                    {theme.displayName}
                                </button>
                            {/each}
                        </div>

                        <div class="divider my-0"></div>

                        <li>
                            <button class="text-error font-bold text-[12px] justify-center" onclick={logout}>
                                Logout Session
                            </button>
                        </li>
                    </ul>
                </div>
            </header>

            <main class="flex-1 overflow-y-auto p-8 bg-base-100">
                <div class="max-w-[1600px] mx-auto">
                    {@render children()}
                </div>
            </main>
        </div>
    </div>
{/if}
