export interface PublicColorOverrides {
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
    fontFamily: string[] | null;
    fontFamilyMono: string[] | null;
    fontFamilySerif: string[] | null;
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
    transport: { header: string; scheme: string } | null;
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
    credentialsPersistenceEnabled: boolean;
    auth: PublicAuthConfig;
    clusters: PublicClusterConfig[];
    themeConfigs: PublicThemeConfig;
}

// Captured once at module load time, before any SPA navigation changes window.location
const _initialBasePath: string = (() => {
    if (typeof window === 'undefined') return '';
    const parts = window.location.pathname.split('/').filter(Boolean);
    return parts.length > 0 ? `/${parts[0]}` : '';
})();

export class JobMasterConfigUtil {
    private static configCache: DashboardPublicConfig | null = null;

    static getBasePath(): string {
        return _initialBasePath;
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

    static async loadConfig(fetchFn: typeof fetch): Promise<DashboardPublicConfig> {
        if (JobMasterConfigUtil.configCache) return JobMasterConfigUtil.configCache;

        const basePath = JobMasterConfigUtil.getBasePath();
        const res = await fetchFn(`${basePath}/jobmaster-config.json`);
        if (!res.ok) throw new Error(`${res.status} ${res.statusText} - jobmaster-config.json`);

        const cfg = (await res.json()) as DashboardPublicConfig;
        JobMasterConfigUtil.configCache = cfg;
        return cfg;
    }

    static getClusterApiUrl(_clusterId?: string): string {
        return JobMasterConfigUtil.configCache?.apiBaseUrl ?? '';
    }
}
