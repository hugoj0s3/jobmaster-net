const KEY = (clusterId: string) => `jm_theme_${clusterId}`;

export function resolveThemeId(clusterId: string, config: any) {
    const stored = sessionStorage.getItem(KEY(clusterId));

    // If stored theme doesn't exist in config anymore, ignore it
    if (stored && config?.themes?.some((t: any) => t.id === stored)) {
        return stored;
    }

    // else fallback to cluster default, then global default
    const cluster = config?.clusters?.find((c: any) => c.id === clusterId);
    return cluster?.defaultThemeId ?? config?.defaultThemeId ?? "jobmaster-light";
}

export function setStoredTheme(clusterId: string, themeId: string) {
    sessionStorage.setItem(KEY(clusterId), themeId);
}

export function clearStoredTheme(clusterId: string) {
    sessionStorage.removeItem(KEY(clusterId));
}
