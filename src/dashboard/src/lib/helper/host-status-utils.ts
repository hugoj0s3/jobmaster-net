export class HostStatusUtil {
	static readonly Label = {
		Online: "Online",
		Offline: "Offline",
		Warning: "Warning"
	} as const;

	static getLabel(status: string | null | undefined): HostStatusLabel {
		if (status === "Online") return HostStatusUtil.Label.Online;
		if (status === "Offline") return HostStatusUtil.Label.Offline;
		if (status === "Warning") return HostStatusUtil.Label.Warning;
		throw new Error(`Unknown host status: ${status}`);
	}

	static getBadgeClass(label: HostStatusLabel): string {
		if (label === HostStatusUtil.Label.Online) return "badge-success";
		if (label === HostStatusUtil.Label.Warning) return "badge-warning";
		if (label === HostStatusUtil.Label.Offline) return "badge-error";
		throw new Error(`Unknown host status label: ${label}`);
	}

	static getDotClass(label: HostStatusLabel): string {
		if (label === HostStatusUtil.Label.Online) return "bg-success";
		if (label === HostStatusUtil.Label.Warning) return "bg-warning";
		if (label === HostStatusUtil.Label.Offline) return "bg-error";
		throw new Error(`Unknown host status label: ${label}`);
	}
}

export type HostStatusLabel = (typeof HostStatusUtil.Label)[keyof typeof HostStatusUtil.Label];