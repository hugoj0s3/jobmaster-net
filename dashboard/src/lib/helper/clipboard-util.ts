import { writable, type Readable } from "svelte/store";

export async function copyText(text: string) {
	try {
		await navigator.clipboard.writeText(text);
	} catch {
		const ta = document.createElement("textarea");
		ta.value = text;
		ta.style.position = "fixed";
		ta.style.left = "-9999px";
		document.body.appendChild(ta);
		ta.focus();
		ta.select();
		document.execCommand("copy");
		document.body.removeChild(ta);
	}
}

export type CopyFeedbackController = {
	copiedId: Readable<string | null>;
	copy(id: string): Promise<void>;
	destroy(): void;
};

export function createCopyFeedback(opts?: { resetAfterMs?: number }): CopyFeedbackController {
	const resetAfterMs = opts?.resetAfterMs ?? 1200;

	const copiedId = writable<string | null>(null);
	let timer: number | undefined;

	return {
		copiedId,

		async copy(id: string) {
			await copyText(id);

			copiedId.set(id);

			if (timer) window.clearTimeout(timer);
			timer = window.setTimeout(() => {
				copiedId.set(null);
			}, resetAfterMs);
		},

		destroy() {
			if (timer) window.clearTimeout(timer);
			timer = undefined;
		}
	};
}