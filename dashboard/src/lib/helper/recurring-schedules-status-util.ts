import type { components } from "$lib/api/schema";

type RecurringScheduleStatusEnum = components["schemas"]["RecurringScheduleStatus"];

export class RecurringSchedulesStatusUtil {
	static readonly Label = {
		PendingSave: "PendingSave",
		Active: "Active",
		Canceled: "Canceled",
		Inactive: "Inactive",
		Completed: "Completed"
	} as const;

	static getLabel(status: number | string | null | undefined): RecurringScheduleStatusLabel {
		const n = Number(status);
		if (n === 1) return RecurringSchedulesStatusUtil.Label.PendingSave;
		if (n === 2) return RecurringSchedulesStatusUtil.Label.Active;
		if (n === 3) return RecurringSchedulesStatusUtil.Label.Canceled;
		if (n === 4) return RecurringSchedulesStatusUtil.Label.Inactive;
		if (n === 5) return RecurringSchedulesStatusUtil.Label.Completed;
		throw new Error(`Unknown recurring schedule status: ${status}`);
	}

	static getBadgeClass(label: RecurringScheduleStatusLabel): string {
		if (label === RecurringSchedulesStatusUtil.Label.PendingSave) return "badge-warning";
		if (label === RecurringSchedulesStatusUtil.Label.Active) return "badge-success";
		if (label === RecurringSchedulesStatusUtil.Label.Canceled) return "badge-ghost";
		if (label === RecurringSchedulesStatusUtil.Label.Inactive) return "badge-ghost";
		if (label === RecurringSchedulesStatusUtil.Label.Completed) return "badge-success";

		throw new Error(`Unknown recurring schedule status label: ${label}`);
	}

	static getBadgeClassByStatus(status: number | string | null | undefined): string {
		const n = Number(status);
		if (n === 1) return "badge-warning";
		if (n === 2) return "badge-success";
		if (n === 3) return "badge-ghost";
		if (n === 4) return "badge-ghost";
		if (n === 5) return "badge-success";
		return "badge-ghost";
	}
}

export type RecurringScheduleStatusLabel = (typeof RecurringSchedulesStatusUtil.Label)[keyof typeof RecurringSchedulesStatusUtil.Label];