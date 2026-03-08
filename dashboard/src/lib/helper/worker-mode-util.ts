export type WorkerMode = "Execution" | "Full" | "Draining" | "Unknown";

export const WorkerModeUtil = {
	Mode: {
		Execution: "Execution" as WorkerMode,
		Full: "Full" as WorkerMode,
		Draining: "Draining" as WorkerMode,
		Unknown: "Unknown" as WorkerMode
	},

	getLabel(mode: number | undefined): WorkerMode {
		switch (mode) {
			case 1: return "Execution";
			case 2: return "Full";
			case 3: return "Draining";
			default: return "Unknown";
		}
	},

	getBadgeClass(mode: WorkerMode): string {
		switch (mode) {
			case "Execution": return "badge-success";
			case "Full": return "badge-error";
			case "Draining": return "badge-warning";
			default: return "badge-ghost";
		}
	}
};
