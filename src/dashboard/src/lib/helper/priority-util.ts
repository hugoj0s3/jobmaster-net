import { Priority as ApiPriority } from "$lib/api/enums";

export class PriorityUtil {
    static readonly Label = {
        Low: "Low",
        Medium: "Medium",
        High: "High",
        Critical: "Critical"
    } as const;

    static getLabel(priority: number | null | undefined): PriorityLabel {
        if (priority === ApiPriority.VeryLow || priority === ApiPriority.Low) return PriorityUtil.Label.Low;
        if (priority === ApiPriority.Medium) return PriorityUtil.Label.Medium;
        if (priority === ApiPriority.High) return PriorityUtil.Label.High;
        if (priority === ApiPriority.Critical) return PriorityUtil.Label.Critical;
        throw new Error(`Unknown priority: ${priority}`);
    }

    static getBadgeClass(label: PriorityLabel): string {
        if (label === PriorityUtil.Label.Critical) return "badge-error";
        if (label === PriorityUtil.Label.High) return "badge-warning";
        if (label === PriorityUtil.Label.Medium) return "badge-info";
        if (label === PriorityUtil.Label.Low) return "badge-neutral";
        throw new Error(`Unknown priority label: ${label}`);
    }
}

export type PriorityLabel = (typeof PriorityUtil.Label)[keyof typeof PriorityUtil.Label];
