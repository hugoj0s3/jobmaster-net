import type { DashboardPublicConfig } from '$lib/api/job-master-config-util';

const KEY = (clusterId: string) => `jm_theme_${clusterId}`;

export function resolveThemeId(clusterId: string, config: DashboardPublicConfig): string {
    const stored = sessionStorage.getItem(KEY(clusterId));

    if (stored && config.themeConfigs?.themes?.some(t => t.id === stored)) {
        return stored;
    }

    const cluster = config.clusters?.find(c => c.id === clusterId);
    return cluster?.themeId ?? config.themeConfigs?.primaryThemeId ?? 'jobmaster-light';
}

export function setStoredTheme(clusterId: string, themeId: string) {
    sessionStorage.setItem(KEY(clusterId), themeId);
}

export function clearStoredTheme(clusterId: string) {
    sessionStorage.removeItem(KEY(clusterId));
}
