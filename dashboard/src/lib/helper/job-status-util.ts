import { JobStatus as ApiJobStatus } from "$lib/api/enums";

export class JobStatusUtil {
    static readonly Label = {
        SavePending: "SavePending",
        HeldOnMaster: "HeldOnMaster",
        AssignedToBucket: "AssignedToBucket",
        Processing: "Processing",
        Succeeded: "Succeeded",
        Queued: "Queued",
        Failed: "Failed",
        Cancelled: "Cancelled"
    } as const;

    static getLabel(status: number | null | undefined): JobStatusLabel {
        if (status === ApiJobStatus.SavePending) return JobStatusUtil.Label.SavePending;
        if (status === ApiJobStatus.HeldOnMaster) return JobStatusUtil.Label.HeldOnMaster;
        if (status === ApiJobStatus.AssignedToBucket) return JobStatusUtil.Label.AssignedToBucket;
        if (status === ApiJobStatus.Processing) return JobStatusUtil.Label.Processing;
        if (status === ApiJobStatus.Succeeded) return JobStatusUtil.Label.Succeeded;
        if (status === ApiJobStatus.Queued) return JobStatusUtil.Label.Queued;
        if (status === ApiJobStatus.Failed) return JobStatusUtil.Label.Failed;
        if (status === ApiJobStatus.Cancelled) return JobStatusUtil.Label.Cancelled;
        throw new Error(`Unknown job status: ${status}`);
    }

    static getBadgeClass(label: JobStatusLabel): string {
        if (label === JobStatusUtil.Label.Succeeded) return "badge-success";
        if (label === JobStatusUtil.Label.Failed) return "badge-error";
        if (label === JobStatusUtil.Label.Cancelled) return "badge-ghost";

        if (label === JobStatusUtil.Label.Processing) return "badge-accent";
        if (label === JobStatusUtil.Label.Queued) return "badge-warning";
        if (label === JobStatusUtil.Label.HeldOnMaster) return "badge-primary";
        if (label === JobStatusUtil.Label.AssignedToBucket) return "badge-secondary";
        if (label === JobStatusUtil.Label.SavePending) return "badge-ghost";

        throw new Error(`Unknown job status label: ${label}`);
    }
}

export type JobStatusLabel = (typeof JobStatusUtil.Label)[keyof typeof JobStatusUtil.Label];
