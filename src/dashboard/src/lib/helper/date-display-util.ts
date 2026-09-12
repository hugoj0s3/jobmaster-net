import { DateTimeUtil } from "$lib/helper/datetime-util";

export class DateDisplayUtil {
    static toDate(value: string | Date | null | undefined): Date | null {
        if (!value) return null;

        // Treat bare datetime strings (no Z / offset) as UTC so the browser
        // timezone conversion is applied correctly when formatting.
        const normalized = typeof value === "string" && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(value) && !/[Z+\-]\d*$/.test(value)
            ? value + "Z"
            : value;
        const d = normalized instanceof Date ? normalized : new Date(normalized);
        return Number.isNaN(d.getTime()) ? null : d;
    }

    static formatDateTime(value: string | Date | null | undefined): string {
        const d = DateDisplayUtil.toDate(value);
        if (!d) return "—";

        return DateTimeUtil.formatDateTime(d);
    }

    static formatRelativeOrDate(
        value: string | Date | null | undefined,
        now: Date = new Date(),
        thresholdMs: number = 60 * 60 * 1000
    ): string {
        const d = DateDisplayUtil.toDate(value);
        if (!d) return "—";

        const diffMs = now.getTime() - d.getTime();
        const absMs = Math.abs(diffMs);

        if (absMs < thresholdMs) {
            const age = DateTimeUtil.formatAgeShort(absMs);
            return diffMs >= 0 ? `${age} ago` : `in ${age}`;
        }

        return DateTimeUtil.formatDateTime(d);
    }
}
