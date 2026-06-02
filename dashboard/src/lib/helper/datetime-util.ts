export class DateTimeUtil {
    static formatAgeShort(ms: number): string {
        const s = Math.max(0, Math.floor(ms / 1000));
        if (s < 60) return `${s}s`;

        const m = Math.floor(s / 60);
        if (m < 60) return `${m}m`;

        const h = Math.floor(m / 60);
        return `${h}h`;
    }

    static lastUpdatedAgo(now: Date, lastUpdatedAt: Date): string {
        return DateTimeUtil.formatAgeShort(now.getTime() - lastUpdatedAt.getTime());
    }

    static formatDateTime(d: Date): string {
        return d.toLocaleString(undefined, {
            month: "numeric",
            day: "numeric",
            year: "numeric",
            hour: "numeric",
            minute: "2-digit",
            second: "2-digit",
        });
    }
}
