import type { components } from "$lib/api/schema";

type RecurringScheduleStatusEnum = components["schemas"]["RecurringScheduleStatus"];

export class RecurringSchedulesStatusUtil {
	static readonly Label = {
		Active: "Active",
		Paused: "Paused",
		Inactive: "Inactive",
		Completed: "Completed",
		Failed: "Failed"
	} as const;

	static getLabel(status: number | null | undefined): RecurringScheduleStatusLabel {
		if (status === 1) return RecurringSchedulesStatusUtil.Label.Active;
		if (status === 2) return RecurringSchedulesStatusUtil.Label.Paused;
		if (status === 3) return RecurringSchedulesStatusUtil.Label.Inactive;
		if (status === 4) return RecurringSchedulesStatusUtil.Label.Completed;
		if (status === 5) return RecurringSchedulesStatusUtil.Label.Failed;
		throw new Error(`Unknown recurring schedule status: ${status}`);
	}

	static getBadgeClass(label: RecurringScheduleStatusLabel): string {
		if (label === RecurringSchedulesStatusUtil.Label.Active) return "badge-success";
		if (label === RecurringSchedulesStatusUtil.Label.Completed) return "badge-success";
		if (label === RecurringSchedulesStatusUtil.Label.Failed) return "badge-error";
		if (label === RecurringSchedulesStatusUtil.Label.Paused) return "badge-warning";
		if (label === RecurringSchedulesStatusUtil.Label.Inactive) return "badge-ghost";

		throw new Error(`Unknown recurring schedule status label: ${label}`);
	}
}

export type RecurringScheduleStatusLabel = (typeof RecurringSchedulesStatusUtil.Label)[keyof typeof RecurringSchedulesStatusUtil.Label];