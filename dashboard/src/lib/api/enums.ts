import type { components } from "$lib/api/schema";

export const JobStatus = {
    SavePending: 1,
    HeldOnMaster: 2,
    AssignedToBucket: 3,
    Processing: 4,
    Succeeded: 5,
    Queued: 6,
    Failed: 7,
    Cancelled: 8
} as const satisfies Record<string, components["schemas"]["JobMasterJobStatus"]>;

export const BucketStatus = {
    Active: 1,
    Completing: 2,
    ReadyToDrain: 3,
    Draining: 4,
    Lost: 5,
    ReadyToDelete: 6
} as const satisfies Record<string, components["schemas"]["BucketStatus"]>;

export const Priority = {
    VeryLow: 1,
    Low: 2,
    Medium: 3,
    High: 4,
    Critical: 5
} as const satisfies Record<string, components["schemas"]["JobMasterPriority"]>;

export const ClusterMode = {
    Active: 1,
    Passive: 2,
    Archived: 3
} as const satisfies Record<string, components["schemas"]["ClusterMode"]>;

export const RecurrenceExpressionTypeId = {
	NaturalCron: "NaturalCron",
	TimeSpanInterval: "TimeSpanInterval",
	NeverRecurs: "Never-Recurs"
} as const;

export type RecurrenceExpressionTypeId =
	(typeof RecurrenceExpressionTypeId)[keyof typeof RecurrenceExpressionTypeId];
