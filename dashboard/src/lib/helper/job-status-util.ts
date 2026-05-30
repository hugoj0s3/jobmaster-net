import { JobStatus as ApiJobStatus } from "$lib/api/enums";

export class JobStatusUtil {
    static readonly Label = {
        PendingSave: "Pending Save",
        OnMaster: "On Master",
        InBucket: "In Bucket",
        Onboarded: "Onboarded",
        Queued: "Queued",
        Processing: "Processing",
        Succeeded: "Succeeded",
        Failed: "Failed",
        Cancelled: "Cancelled",
        Aborted: "Aborted"
    } as const;

    static getLabel(status: number | null | undefined): JobStatusLabel {
        if (status === ApiJobStatus.PendingSave) return JobStatusUtil.Label.PendingSave;
        if (status === ApiJobStatus.OnMaster) return JobStatusUtil.Label.OnMaster;
        if (status === ApiJobStatus.InBucket) return JobStatusUtil.Label.InBucket;
        if (status === ApiJobStatus.Onboarded) return JobStatusUtil.Label.Onboarded;
        if (status === ApiJobStatus.Queued) return JobStatusUtil.Label.Queued;
        if (status === ApiJobStatus.Processing) return JobStatusUtil.Label.Processing;
        if (status === ApiJobStatus.Succeeded) return JobStatusUtil.Label.Succeeded;
        if (status === ApiJobStatus.Failed) return JobStatusUtil.Label.Failed;
        if (status === ApiJobStatus.Cancelled) return JobStatusUtil.Label.Cancelled;
        if (status === ApiJobStatus.Aborted) return JobStatusUtil.Label.Aborted;
        throw new Error(`Unknown job status: ${status}`);
    }

    static getBadgeClass(label: JobStatusLabel): string {
        if (label === JobStatusUtil.Label.Succeeded) return "badge-success";
        if (label === JobStatusUtil.Label.Failed) return "badge-error";
        if (label === JobStatusUtil.Label.Cancelled) return "badge-ghost";
        if (label === JobStatusUtil.Label.Aborted) return "badge-error";

        if (label === JobStatusUtil.Label.Processing) return "badge-accent";
        if (label === JobStatusUtil.Label.Queued) return "badge-warning";
        if (label === JobStatusUtil.Label.Onboarded) return "badge-info";
        if (label === JobStatusUtil.Label.InBucket) return "badge-secondary";
        if (label === JobStatusUtil.Label.OnMaster) return "badge-primary";
        if (label === JobStatusUtil.Label.PendingSave) return "badge-ghost";

        throw new Error(`Unknown job status label: ${label}`);
    }
}

export type JobStatusLabel = (typeof JobStatusUtil.Label)[keyof typeof JobStatusUtil.Label];
