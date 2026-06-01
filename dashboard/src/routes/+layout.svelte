<!-- src/routes/+layout.svelte -->
<script lang="ts">
    import "../app.css";
    import {onMount} from "svelte";
    import { page } from "$app/state";
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
        const parts = page.url.pathname.split("/").filter(Boolean);
        
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

        const urlClusterId = getUrlClusterIdFromPathname(page.url.pathname);
        
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

            if (page.url.pathname !== target) {
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

    // DaisyUI v5 theme variables (renamed from v4 --rounded-* to --radius-*)
    const styleVarList = [
        "--font-sans", "--font-mono",
        "--radius-box", "--radius-selector", "--radius-field",
        "--size-selector", "--size-field", "--border", "--depth", "--noise",
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

        // Primary theme StyleOverrides are the site-wide baseline (fonts, radii, etc.).
        // Apply them first so every theme inherits them, even color-only themes.
        const primaryTheme = themes?.find((t: any) => t.id === config?.themeConfigs?.primaryThemeId);
        if (primaryTheme?.styleOverrides) applyStyleOverrides(primaryTheme.styleOverrides, root);

        if (theme.colorOverrides) {
            for (const [key, value] of Object.entries(theme.colorOverrides)) {
                if (!value) continue;
                const cssVar = themeVarMap[key];
                if (cssVar) root.setProperty(cssVar, value as string);
            }
        }

        // Current theme's StyleOverrides (if any) override the primary baseline.
        if (theme.styleOverrides && theme.id !== primaryTheme?.id) {
            applyStyleOverrides(theme.styleOverrides, root);
        }

        if (persistForCluster && currentCluster?.id) {
            setStoredTheme(currentCluster.id, themeId);
        }

        updateFavicon();
    }

    function applyStyleOverrides(so: any, root: CSSStyleDeclaration) {
        if (so.fontUrlSans)            injectFontUrl(so.fontUrlSans);
        if (so.fontFamilySans?.length) root.setProperty("--font-sans",       so.fontFamilySans.join(", "));
        if (so.fontUrlMono)            injectFontUrl(so.fontUrlMono);
        if (so.fontFamilyMono?.length) root.setProperty("--font-mono",       so.fontFamilyMono.join(", "));
        // DaisyUI v5 radius vars. Always set via inline style so every theme
        // uses the same values — per-theme DaisyUI CSS never wins over inline.
        root.setProperty("--radius-box",      so.borderRadiusBox  ?? "0.5rem");
        root.setProperty("--radius-selector", so.borderRadiusBtn  ?? "0.5rem");
        root.setProperty("--radius-field",    so.borderRadiusBadge ?? "0.25rem");
    }

    function updateFavicon() {
        const probe = document.createElement('div');
        probe.style.cssText = 'visibility:hidden;position:absolute;pointer-events:none';
        document.body.appendChild(probe);

        probe.style.backgroundColor = 'var(--color-logo)';
        const bg = getComputedStyle(probe).backgroundColor || '#111';

        probe.style.backgroundColor = 'var(--color-logo-content)';
        const fg = getComputedStyle(probe).backgroundColor || '#fff';

        document.body.removeChild(probe);

        const svg = `<svg viewBox="0 0 200 200" xmlns="http://www.w3.org/2000/svg">
  <rect x="10" y="10" width="180" height="180" rx="40" ry="40" fill="${bg}"/>
  <g stroke="${fg}" stroke-opacity="0.12" stroke-width="1.2" fill="none">
    <line x1="60" y1="30" x2="60" y2="170"/>
    <line x1="100" y1="30" x2="100" y2="170"/>
    <line x1="140" y1="30" x2="140" y2="170"/>
    <line x1="30" y1="60" x2="170" y2="60"/>
    <line x1="30" y1="100" x2="170" y2="100"/>
    <line x1="30" y1="140" x2="170" y2="140"/>
  </g>
  <g fill="${fg}" opacity="0.25">
    <circle cx="60" cy="60" r="3"/><circle cx="100" cy="60" r="3"/><circle cx="140" cy="60" r="3"/>
    <circle cx="60" cy="100" r="3"/><circle cx="140" cy="100" r="3"/>
    <circle cx="60" cy="140" r="3"/><circle cx="100" cy="140" r="3"/><circle cx="140" cy="140" r="3"/>
  </g>
  <text x="100" y="118" text-anchor="middle" font-family="system-ui,sans-serif" font-weight="800" font-size="72" letter-spacing="-2" fill="${fg}">JM</text>
</svg>`;

        let link = document.querySelector<HTMLLinkElement>('link[rel~="icon"]');
        if (!link) {
            link = document.createElement('link');
            link.rel = 'icon';
            link.type = 'image/svg+xml';
            document.head.appendChild(link);
        }
        link.href = `data:image/svg+xml,${encodeURIComponent(svg)}`;
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
                            <!-- Cluster info -->
                            <div class="flex flex-col items-start leading-tight gap-0.5">
                                <span class="text-[12px] font-bold text-base-content">{currentCluster?.id}</span>
                                {#if currentCluster?.environmentName}
                                    <span class="badge badge-primary badge-xs">{currentCluster.environmentName}</span>
                                {/if}
                            </div>

                            <svg xmlns="http://www.w3.org/2000/svg" class="h-3 w-3 opacity-30 ml-1" fill="none"
                                 viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/>
                            </svg>
                        </button>

                        <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
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
                                            {#if cluster.environmentName}
                                                <span class="badge badge-primary badge-xs mt-0.5">{cluster.environmentName}</span>
                                            {/if}
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
                    <div class="flex items-center gap-3">
                        <h1 class="text-3xl tracking-tight leading-none flex items-baseline">
                            <span class="font-light text-base-content">Job</span><span class="font-extrabold text-base-content">Master</span>
                        </h1>
                        <AppLogo class="h-11 w-11 -rotate-12" />
                    </div>
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
                                {#if cluster.environmentName}
                                    <span class="badge badge-primary badge-xs">{cluster.environmentName}</span>
                                {/if}
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
