import type { components } from "$lib/api/schema";

export const JobStatus = {
    PendingSave: 1,
    OnMaster: 2,
    InBucket: 3,
    Processing: 4,
    Succeeded: 5,
    Queued: 6,
    Failed: 7,
    Cancelled: 8,
    Aborted: 9,
    Onboarded: 10
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
} as const;

export const LogCategory = {
	Job: 1,
	JobExecution: 2,
	AgentWorker: 3,
	Bucket: 4,
	Cluster: 5,
	RecurringSchedule: 6,
	Api: 7
} as const satisfies Record<string, components["schemas"]["ApiJobMasterLogCategory"]>;

export const LogLevel = {
	Debug: 0,
	Info: 1,
	Warning: 2,
	Error: 3,
	Critical: 4
} as const satisfies Record<string, components["schemas"]["ApiJobMasterLogLevel"]>;

export function logLevelLabel(level: number | null | undefined): string {
	switch (level) {
		case LogLevel.Critical: return "Critical";
		case LogLevel.Error:    return "Error";
		case LogLevel.Warning:  return "Warning";
		case LogLevel.Info:     return "Info";
		case LogLevel.Debug:    return "Debug";
		default: return level != null ? "Level " + level : "?";
	}
}

export function logLevelBadgeClass(level: number | null | undefined): string {
	if (level === LogLevel.Critical || level === LogLevel.Error) return "badge-error";
	if (level === LogLevel.Warning) return "badge-warning";
	if (level === LogLevel.Info)    return "badge-info";
	return "badge-ghost";
}

export const LogLevelFilterOptions = [
	{ value: String(LogLevel.Critical), label: "Critical" },
	{ value: String(LogLevel.Error),    label: "Error" },
	{ value: String(LogLevel.Warning),  label: "Warning" },
	{ value: String(LogLevel.Info),     label: "Info" },
	{ value: String(LogLevel.Debug),    label: "Debug" }
];

export const WorkerMode = {
	Full: 1,
	Execution: 2,
	Drain: 3,
	Coordinator: 4
} as const satisfies Record<string, components["schemas"]["AgentWorkerMode"]>;

export function workerModeLabel(mode: number | null | undefined): string {
	switch (mode) {
		case WorkerMode.Full:        return "Full";
		case WorkerMode.Execution:   return "Execution";
		case WorkerMode.Drain:       return "Drain";
		case WorkerMode.Coordinator: return "Coordinator";
		default: return "Unknown";
	}
}

export function workerModeBadgeClass(_mode?: number | null): string {
	return "badge-ghost";
}

export const WorkerModeFilterOptions = [
	{ value: String(WorkerMode.Full),        label: "Full" },
	{ value: String(WorkerMode.Execution),   label: "Execution" },
	{ value: String(WorkerMode.Drain),       label: "Drain" },
	{ value: String(WorkerMode.Coordinator), label: "Coordinator" }
];

export const RecurrenceExpressionTypeId = {
	NaturalCron: "NaturalCron",
	TimeSpanInterval: "TimeSpanInterval",
	NeverRecurs: "Never-Recurs"
} as const;

export type RecurrenceExpressionTypeId =
	(typeof RecurrenceExpressionTypeId)[keyof typeof RecurrenceExpressionTypeId];
