import { DateTimeUtil } from "$lib/helper/datetime-util";

export class DateDisplayUtil {
    static toDate(value: string | Date | null | undefined): Date | null {
        if (!value) return null;

        const d = value instanceof Date ? value : new Date(value);
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
