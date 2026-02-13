const KEY = (clusterId: string) => `jm_dashboard_settings_${clusterId}`;

export type DashboardSettings = {
    nextMinutes: number;
    lastHours: number;
    refreshIntervalSec: number;
};

const DEFAULTS: DashboardSettings = {
    nextMinutes: 5,
    lastHours: 24,
    refreshIntervalSec: 20
};

function isValidPositiveInt(n: unknown): n is number {
    return typeof n === "number" && Number.isFinite(n) && n > 0 && Number.isInteger(n);
}

function clampInt(n: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, n));
}

export function resolve(clusterId: string | null | undefined): DashboardSettings {
    if (!clusterId) return { ...DEFAULTS };

    try {
        if (typeof sessionStorage === "undefined") return { ...DEFAULTS };

        const raw = sessionStorage.getItem(KEY(clusterId));
        if (!raw) return { ...DEFAULTS };

        const parsed = JSON.parse(raw) as Partial<DashboardSettings>;

        const nextMinutes = isValidPositiveInt(parsed.nextMinutes)
            ? clampInt(parsed.nextMinutes, 1, 60 * 24)
            : DEFAULTS.nextMinutes;
        const lastHours = isValidPositiveInt(parsed.lastHours)
            ? clampInt(parsed.lastHours, 1, 24 * 365)
            : DEFAULTS.lastHours;

        let refreshIntervalSec = isValidPositiveInt(parsed.refreshIntervalSec)
            ? clampInt(parsed.refreshIntervalSec, 5, 60 * 60)
            : DEFAULTS.refreshIntervalSec;
        
        if (refreshIntervalSec < 5) {
            refreshIntervalSec = 5;
        }
        
        refreshIntervalSec += 1;

        return { nextMinutes, lastHours, refreshIntervalSec };
    } catch {
        return { ...DEFAULTS };
    }
}

export function set(clusterId: string, settings: DashboardSettings) {
    const normalized: DashboardSettings = {
        nextMinutes: clampInt(settings.nextMinutes, 1, 60 * 24),
        lastHours: clampInt(settings.lastHours, 1, 24 * 365),
        refreshIntervalSec: clampInt(settings.refreshIntervalSec, 5, 60 * 60)
    };

    sessionStorage.setItem(KEY(clusterId), JSON.stringify(normalized));
}

export function clear(clusterId: string) {
    sessionStorage.removeItem(KEY(clusterId));
}

export function defaultDashboardSettings(): DashboardSettings {
    return { ...DEFAULTS };
}
