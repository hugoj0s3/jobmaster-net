<!-- src/routes/+layout.svelte -->
<script lang="ts">
    import "../app.css";
    import {onMount} from "svelte";
    import { page } from "$app/stores";
    import { goto } from "$app/navigation";
    import Login from "$lib/components/Login.svelte";
    import Sidebar from "$lib/components/Sidebar.svelte";

    import {resolveThemeId, setStoredTheme} from "$lib/theme-helper";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";

    let {children} = $props();

    let config = $state<any>(null);
    let isLoggedIn = $state(false);
    let currentCluster = $state<any>(null);
    let currentTheme = $state<any>(null);

    onMount(async () => {
        config = await JobMasterConfigUtil.loadConfig(fetch);
        if (sessionStorage.getItem("jm_auth") === "true" || config.auth?.enabled != true) isLoggedIn = true;
    });

    function getUrlClusterIdFromPathname(pathname: string) {
        const parts = pathname.split("/").filter(Boolean);
        for (const part of parts) {
            const cluster = config?.clusters?.find((c: any) => c.id.toLowerCase() === part.toLowerCase());
            if (cluster) return cluster.id;
        }
        return null;
    }

    function getPathAfterCluster() {
        const base = JobMasterConfigUtil.getBasePath();
        const baseParts = base.split("/").filter(Boolean);
        const parts = $page.url.pathname.split("/").filter(Boolean);
        
        let filtered = parts;
        if (baseParts.length > 0 && parts.slice(0, baseParts.length).every((p, idx) => p.toLowerCase() === baseParts[idx].toLowerCase())) {
            filtered = parts.slice(baseParts.length);
        }
        
        if (filtered.length > 0) {
            const first = filtered[0];
            const isCluster = config?.clusters?.some((c: any) => c.id.toLowerCase() === first.toLowerCase()) === true;
            if (isCluster) {
                filtered = filtered.slice(1);
            }
        }
        
        return `/${filtered.join("/")}`;
    }

    function handleClusterChange(id: string) {
        const after = getPathAfterCluster();
        const next = after === "/" ? "/dashboard" : after;
        window.location.href = JobMasterConfigUtil.resolveHref(next, id);
    }

    $effect(() => {
        if (!config?.clusters?.length) return;

        const urlClusterId = getUrlClusterIdFromPathname($page.url.pathname);
        
        let clusterFound = null;
        if (urlClusterId) {
            const urlClusterIdLower = urlClusterId.toLowerCase();
            clusterFound = config.clusters.find((c: any) => (c.id ?? "").toLowerCase() === urlClusterIdLower);
        }

        if (!urlClusterId || !clusterFound) {
            const parts = $page.url.pathname.split("/").filter(Boolean);

            if (parts.length > 0) {
                void goto("/", { replaceState: true, keepFocus: true, noScroll: true });
            }

            currentCluster = null;

            const defaultThemeId = config?.themeConfigs?.primaryThemeId ?? "jobmaster-light";
            if (currentTheme?.id !== defaultThemeId) {
                applyTheme(defaultThemeId, false);
            }
            return;
        }
        
        const nextCluster = clusterFound ?? config.clusters[0] ?? null;
        if (!nextCluster) return;

        if (currentCluster?.id !== nextCluster.id) {
            currentCluster = nextCluster;
            const themeId = resolveThemeId(nextCluster.id, config);
            applyTheme(themeId);
        }

        if (!clusterFound) {
            const after = getPathAfterCluster();
            const next = after === "/" ? "/dashboard" : after;
            const target = JobMasterConfigUtil.resolveHref(next, nextCluster.id);

            if ($page.url.pathname !== target) {
                void goto(target, { replaceState: true, keepFocus: true, noScroll: true });
            }
        }
    });

    const themeVarMap: Record<string, string> = {
        primary: "--color-primary",
        primaryContent: "--color-primary-content",
        secondary: "--color-secondary",
        secondaryContent: "--color-secondary-content",
        accent: "--color-accent",
        accentContent: "--color-accent-content",
        neutral: "--color-neutral",
        neutralContent: "--color-neutral-content",
        base100: "--color-base-100",
        base200: "--color-base-200",
        base300: "--color-base-300",
        baseContent: "--color-base-content",
        info: "--color-info",
        infoContent: "--color-info-content",
        success: "--color-success",
        successContent: "--color-success-content",
        warning: "--color-warning",
        warningContent: "--color-warning-content",
        error: "--color-error",
        errorContent: "--color-error-content",
    };

    const styleVarList = [
        "--font-sans", "--font-mono", "--font-serif",
        "--rounded-box", "--rounded-btn", "--rounded-badge",
        "--tab-radius", "--border-btn", "--tab-border",
        "--animation-btn", "--animation-input", "--btn-focus-scale",
    ];

    const darkBaseThemes = new Set([
        "dark",
        "business",
        "night",
        "dracula",
        "synthwave",
        "jobmaster-dark",
    ]);

    function applyTheme(themeId: string, persistForCluster = false) {
        const themes = config?.themeConfigs?.themes;
        let theme = themes?.find((t: any) => t.id === themeId);

        if (!theme) {
            const fallbackId = config?.themeConfigs?.primaryThemeId ?? "jobmaster-light";
            theme = themes?.find((t: any) => t.id === fallbackId) ?? themes?.[0];
        }

        if (!theme) return;

        currentTheme = theme;
        const base = theme.baseTheme ?? "jobmaster-light";

        const root = document.documentElement.style;
        Object.values(themeVarMap).forEach(cssVar => root.removeProperty(cssVar));
        styleVarList.forEach(cssVar => root.removeProperty(cssVar));

        document.documentElement.setAttribute("data-theme", base);
        document.documentElement.style.colorScheme = darkBaseThemes.has(base) ? "dark" : "light";

        if (theme.colorOverrides) {
            for (const [key, value] of Object.entries(theme.colorOverrides)) {
                if (!value) continue;
                const cssVar = themeVarMap[key];
                if (cssVar) root.setProperty(cssVar, value as string);
            }
        }

        if (theme.styleOverrides) {
            const so = theme.styleOverrides;
            if (so.fontFamily?.length)     root.setProperty("--font-sans",          so.fontFamily.join(", "));
            if (so.fontFamilyMono?.length)  root.setProperty("--font-mono",         so.fontFamilyMono.join(", "));
            if (so.fontFamilySerif?.length) root.setProperty("--font-serif",        so.fontFamilySerif.join(", "));
            if (so.borderRadiusBox)         root.setProperty("--rounded-box",       so.borderRadiusBox);
            if (so.borderRadiusBtn)         root.setProperty("--rounded-btn",       so.borderRadiusBtn);
            if (so.borderRadiusBadge)       root.setProperty("--rounded-badge",     so.borderRadiusBadge);
            if (so.tabRadius)               root.setProperty("--tab-radius",        so.tabRadius);
            if (so.borderWidthBtn)          root.setProperty("--border-btn",        so.borderWidthBtn);
            if (so.tabBorderWidth)          root.setProperty("--tab-border",        so.tabBorderWidth);
            if (so.animationBtn)            root.setProperty("--animation-btn",     so.animationBtn);
            if (so.animationInput)          root.setProperty("--animation-input",   so.animationInput);
            if (so.btnFocusScale)           root.setProperty("--btn-focus-scale",   so.btnFocusScale);
        }

        if (persistForCluster && currentCluster?.id) {
            setStoredTheme(currentCluster.id, themeId);
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
    <Login auth={config.auth} onLogin={() => (isLoggedIn = true)}/>

{:else}
    {#if currentCluster}
        <div class="flex h-screen overflow-hidden bg-base-100 text-base-content">
            <Sidebar/>

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

                            <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 opacity-30" fill="none"
                                 viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>
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
                                            class="flex flex-col items-start py-2 {currentCluster?.id === cluster.id ? 'active' : ''}"
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
                                {#each config.themeConfigs.themes as theme}
                                    <button
                                            class="btn btn-xs font-mono text-[12px] {currentTheme?.id === theme.id ? 'btn-primary' : 'btn-ghost border-base-300'}"
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

                <main class="flex-1 overflow-y-auto pr-2 py-8 bg-base-100">
                    <div class="max-w-[1600px]">
                        {@render children()}
                    </div>
                </main>
            </div>
        </div>
    {:else}
        <main class="min-h-screen bg-base-200 text-base-content">
            {@render children()}
        </main>
    {/if}
{/if}
