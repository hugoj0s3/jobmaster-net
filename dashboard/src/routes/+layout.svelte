<!-- src/routes/+layout.svelte -->
<script lang="ts">
    import "../app.css";
    import {onMount} from "svelte";
    import { page } from "$app/stores";
    import { goto } from "$app/navigation";
    import Login from "$lib/components/Login.svelte";
    import Sidebar from "$lib/components/Sidebar.svelte";
    import AppLogo from "$lib/components/AppLogo.svelte";

    import {resolveThemeId, setStoredTheme} from "$lib/theme-helper";
    import { AuthRetentionUtil } from "$lib/api/auth-retention-util";
    import { JobMasterConfigUtil } from "$lib/api/job-master-config-util";
    import { ApiClientUtil } from '$lib/api/api-client-util';

    let {children} = $props();

    let config = $state<any>(null);
    let isLoggedIn = $state(false);
    let currentCluster = $state<any>(null);
    let currentTheme = $state<any>(null);

    onMount(async () => {
        config = await JobMasterConfigUtil.loadConfig();
        if (config.auth?.enabled != true) isLoggedIn = true;

        const credentials = await AuthRetentionUtil.getCredentials();
        if (!credentials) {
            isLoggedIn = false;
            return;
        }

        const isValidCredentials = await ApiClientUtil.ValidateCredentials(credentials, fetch);
        if (!isValidCredentials) {
            isLoggedIn = false;
            return;
        }

        isLoggedIn = true;
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
        logo: "--color-logo",
        logoContent: "--color-logo-content",
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

    const injectedFontUrls = new Set<string>();

    function injectFontUrl(url: string) {
        if (injectedFontUrls.has(url)) return;
        const link = document.createElement('link');
        link.rel = 'stylesheet';
        link.href = url;
        document.head.appendChild(link);
        injectedFontUrls.add(url);
    }

    const styleVarList = [
        "--font-sans", "--font-mono",
        "--rounded-box", "--rounded-btn", "--rounded-badge",
        "--tab-radius", "--border-btn", "--tab-border",
        "--animation-btn", "--animation-input", "--btn-focus-scale",
    ];

    const darkBaseThemes = new Set([
        "dark", "synthwave", "halloween", "forest", "aqua", "black",
        "luxury", "dracula", "business", "night", "coffee", "dim",
        "sunset", "jobmaster-dark",
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
            if (so.fontUrlSans)             injectFontUrl(so.fontUrlSans);
            if (so.fontFamilySans?.length)  root.setProperty("--font-sans",          so.fontFamilySans.join(", "));
            if (so.fontUrlMono)             injectFontUrl(so.fontUrlMono);
            if (so.fontFamilyMono?.length)  root.setProperty("--font-mono",         so.fontFamilyMono.join(", "));
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


    async function logout() {
        AuthRetentionUtil.clear();
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
                    <div class="dropdown dropdown-center">
                        <button
                                tabindex="0"
                                class="btn btn-ghost btn-sm h-10 px-3 flex items-center gap-2.5 border border-base-300 bg-base-100 hover:bg-base-200"
                        >
                            <!-- Logo-style accent badge -->
                            <div class="h-7 w-7 rounded-xl bg-primary flex items-center justify-center shrink-0">
                                <span class="text-[11px] font-extrabold text-primary-content leading-none">
                                    {(currentCluster?.id?.[0] ?? 'C').toUpperCase()}
                                </span>
                            </div>
                            <!-- Cluster info -->
                            <div class="flex flex-col items-start leading-tight">
                                <span class="text-[12px] font-bold text-base-content">{currentCluster?.id}</span>
                                <span class="text-[10px] opacity-50 font-mono">{currentCluster?.environmentName}</span>
                            </div>

                            <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 opacity-30 ml-1" fill="none"
                                 viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>
                            </svg>
                        </button>

                        <ul
                                tabindex="0"
                                class="dropdown-content menu flex-nowrap p-2 shadow-2xl bg-base-200 rounded-box w-[28rem] mt-2 border border-base-300 z-[100] space-y-2 max-h-[32rem] overflow-y-auto"
                        >
                            <li class="menu-title flex flex-row items-center justify-between pr-2 text-[12px] font-black">
                                <span class="opacity-40">Cluster</span>
                                <button
                                        class="btn btn-ghost btn-xs text-error hover:bg-error/10 font-bold text-[11px] h-6 min-h-6 px-2"
                                        onclick={logout}
                                >
                                    Logout
                                </button>
                            </li>
                            <div class="grid grid-cols-2 gap-1 p-2">
                                {#each config.clusters as cluster}
                                    <li>
                                        <button
                                                class="flex flex-col items-start py-2 w-full {currentCluster?.id === cluster.id ? 'active' : ''}"
                                                onclick={() => handleClusterChange(cluster.id)}
                                        >
                                            <span class="text-[12px] font-bold">{cluster.id}</span>
                                            <span class="text-[10px] opacity-50 font-mono">{cluster.environmentName}</span>
                                        </button>
                                    </li>
                                {/each}
                            </div>

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
                        </ul>
                    </div>
                </header>

                <main class="flex-1 overflow-y-auto pr-2 pt-2 pb-8 bg-base-100">
                    <div class="max-w-[1600px]">
                        {@render children()}
                    </div>
                </main>
            </div>
        </div>
    {:else}
        <main class="flex min-h-screen items-center justify-center bg-base-200 text-base-content">
            <div class="mx-auto w-full max-w-md px-6">
                <div class="flex flex-col items-center text-center">
                    <AppLogo class="mb-6 h-20 w-20" />
                    <h1 class="text-3xl tracking-tight leading-none flex items-baseline">
                        <span class="font-light text-base-content">Job</span><span class="font-extrabold text-base-content">Master</span>
                    </h1>
                    <p class="mt-2 text-sm text-base-content/60">Select a cluster to continue</p>
                </div>
                <div class="divider mt-8 mb-6"></div>
                <div class="space-y-3">
                    {#each config.clusters as cluster (cluster.id)}
                        <a
                            href={JobMasterConfigUtil.resolveHref("/", cluster.id)}
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
        </main>
    {/if}
{/if}
