export interface PublicColorOverrides {
    logo: string | null;
    logoContent: string | null;
    base100: string | null;
    base200: string | null;
    base300: string | null;
    baseContent: string | null;
    primary: string | null;
    primaryContent: string | null;
    secondary: string | null;
    secondaryContent: string | null;
    accent: string | null;
    accentContent: string | null;
    neutral: string | null;
    neutralContent: string | null;
    info: string | null;
    infoContent: string | null;
    success: string | null;
    successContent: string | null;
    warning: string | null;
    warningContent: string | null;
    error: string | null;
    errorContent: string | null;
}

export interface PublicStyleOverrides {
    fontFamilySans: string[] | null;
    fontUrlSans: string | null;
    fontFamilyMono: string[] | null;
    fontUrlMono: string | null;
    borderRadiusBox: string | null;
    borderRadiusBtn: string | null;
    borderRadiusBadge: string | null;
    tabRadius: string | null;
    borderWidthBtn: string | null;
    tabBorderWidth: string | null;
    animationBtn: string | null;
    animationInput: string | null;
    btnFocusScale: string | null;
}

export interface PublicThemeItemConfig {
    id: string;
    displayName: string;
    baseTheme: string;
    isPrimaryTheme: boolean;
    colorOverrides: PublicColorOverrides | null;
    styleOverrides: PublicStyleOverrides | null;
}

export interface PublicThemeConfig {
    primaryThemeId: string;
    themes: PublicThemeItemConfig[];
}

export interface PublicJwtFormFieldConfig {
    id: string;
    label: string;
    type: string;
    isRequired: boolean;
    defaultValue: string | null;
    disabled: boolean;
}

export interface PublicAuthProviderConfig {
    type: 'API_KEY' | 'USER_PASSWORD' | 'JWT_SIMPLE' | 'JWT_CUSTOM_FORM';
    displayName: string | null;
    headerName: string | null;
    scheme: string | null;
    userHeaderName: string | null;
    passwordHeaderName: string | null;
    tokenUrl: string | null;
    fields: PublicJwtFormFieldConfig[] | null;
}

export interface PublicAuthConfig {
    enabled: boolean;
    providers: PublicAuthProviderConfig[];
}

export interface PublicClusterConfig {
    id: string;
    environmentName: string;
    themeId: string | null;
}

export interface DashboardPublicConfig {
    apiBaseUrl: string;
    authRetentionMode: 'none' | 'client' | 'server';
    auth: PublicAuthConfig;
    clusters: PublicClusterConfig[];
    themeConfigs: PublicThemeConfig;
}

// Set once by loadConfig after discovering where jobmaster-config.json lives.
// This decouples base-path detection from window.location so direct URL navigation works.
let _resolvedBasePath: string | null = null;

export class JobMasterConfigUtil {
    private static configCache: DashboardPublicConfig | null = null;

    static getBasePath(): string {
        return _resolvedBasePath ?? '';
    }

    static resolveHref(path: string, clusterId?: string): string {
        const base = JobMasterConfigUtil.getBasePath();
        if (!clusterId) {
            if (path === '/') return base || '/';
            return `${base}${path}`;
        }
        if (path === '/') return `${base}/${clusterId}/dashboard`;
        return `${base}/${clusterId}${path}`;
    }

    static async loadConfig(): Promise<DashboardPublicConfig> {
        if (JobMasterConfigUtil.configCache) return JobMasterConfigUtil.configCache;

        // In production the C# middleware injects window.__JM__ into index.html,
        // giving us the base path directly with no guessing needed.
        const injected = typeof window !== 'undefined' ? (window as any).__JM__ : undefined;
        if (injected?.basePath !== undefined) {
            const prefix: string = injected.basePath;
            const res = await fetch(`${prefix}/jobmaster-config.json`);
            if (!res.ok) throw new Error(`${res.status} ${res.statusText} - jobmaster-config.json`);
            const cfg = (await res.json()) as DashboardPublicConfig;
            _resolvedBasePath = prefix;
            JobMasterConfigUtil.configCache = cfg;
            return cfg;
        }

        // Dev fallback: walk up path segments until jobmaster-config.json is found.
        const pathname = typeof window !== 'undefined' ? window.location.pathname : '';
        const parts = pathname.split('/').filter(Boolean);

        for (let i = parts.length; i >= 0; i--) {
            const prefix = i > 0 ? '/' + parts.slice(0, i).join('/') : '';
            const res = await fetch(`${prefix}/jobmaster-config.json`);
            if (!res.ok) continue;
            const ct = res.headers.get('content-type') ?? '';
            if (!ct.includes('json')) continue;
            const cfg = (await res.json()) as DashboardPublicConfig;
            _resolvedBasePath = prefix;
            JobMasterConfigUtil.configCache = cfg;
            return cfg;
        }

        throw new Error('jobmaster-config.json not found');
    }

    static getClusterApiUrl(_clusterId?: string): string {
        return JobMasterConfigUtil.configCache?.apiBaseUrl ?? '';
    }
}
