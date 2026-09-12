/**
 * Shared utilities for serializing/deserializing datetime filter values
 * to/from URL query parameters.
 *
 * Relative time is stored as: rel:unit:fromOffset:toOffset  (e.g. rel:min:-5:0)
 * Specific date is stored as: spec,fromISO,toISO             (uses comma because ISO contains colons)
 */

export type DatetimeFilterValue = {
	mode?: "specific" | "relative";
	unit?: string;
	fromOffset?: number;
	toOffset?: number;
	from?: string;
	to?: string;
};

function parseOptionalNumber(raw: string | undefined): number | undefined {
	if (raw == null) return undefined;
	const trimmed = raw.trim();
	if (!trimmed) return undefined;
	const n = Number(trimmed);
	return Number.isFinite(n) ? n : undefined;
}

function toValidDate(raw: string | undefined): Date | undefined {
	if (!raw) return undefined;
	const d = new Date(raw);
	return Number.isNaN(d.getTime()) ? undefined : d;
}

export function unitToMs(unit: string): number {
	if (unit === "sec") return 1000;
	if (unit === "min") return 60_000;
	if (unit === "hour") return 60 * 60_000;
	if (unit === "day") return 24 * 60 * 60_000;
	return 60_000;
}

/**
 * Parse a URL param string back into a filter values object keyed by the given filterKey.
 */
export function parseDatetimeParam(raw: string, filterKey: string): Record<string, unknown> {
	if (!raw) return {};
	if (raw.startsWith("spec,")) {
		const parts = raw.split(",");
		const from = parts[1] || undefined;
		const to = parts[2] || undefined;
		if (!from && !to) return {};
		return {
			[filterKey]: {
				mode: "specific",
				from,
				to
			} satisfies DatetimeFilterValue
		};
	}
	if (raw.startsWith("rel:")) {
		const parts = raw.split(":");
		if (parts.length < 4) return {};
		const unit = parts[1];
		const fromOffset = parseOptionalNumber(parts[2]);
		const toOffset = parseOptionalNumber(parts[3]);
		if (!Number.isFinite(fromOffset) && !Number.isFinite(toOffset)) return {};
		const now = Date.now();
		const ms = unitToMs(unit);
		return {
			[filterKey]: {
				mode: "relative",
				unit,
				fromOffset: Number.isFinite(fromOffset) ? fromOffset : undefined,
				toOffset: Number.isFinite(toOffset) ? toOffset : undefined,
				from: typeof fromOffset === 'number' && Number.isFinite(fromOffset) ? new Date(now + (fromOffset as number) * ms).toISOString() : undefined,
				to: typeof toOffset === 'number' && Number.isFinite(toOffset) ? new Date(now + (toOffset as number) * ms).toISOString() : undefined
			} satisfies DatetimeFilterValue
		};
	}
	return {};
}

/**
 * Serialize filter values for a given filterKey into a URL param string.
 */
export function datetimeToParam(fv: Record<string, unknown>, filterKey: string): string {
	const hb = fv[filterKey] as DatetimeFilterValue | undefined;
	if (!hb) return "";
	if (hb.mode === "relative") {
		const unit = hb.unit ?? "min";
		const from = typeof hb.fromOffset === "number" ? hb.fromOffset : "";
		const to = typeof hb.toOffset === "number" ? hb.toOffset : "";
		if (from === "" && to === "") return "";
		return `rel:${unit}:${from}:${to}`;
	}
	const from = hb.from ?? "";
	const to = hb.to ?? "";
	if (!from && !to) return "";
	return `spec,${from},${to}`;
}

/**
 * Compute absolute from/to dates from a DatetimeFilterValue.
 * For relative mode, recomputes from "now". For specific mode, returns as-is.
 * Returns { from?: Date, to?: Date }.
 */
export function computeDateRange(hb: DatetimeFilterValue): { from?: Date; to?: Date } {
	if (hb.mode === "relative") {
		const now = Date.now();
		const ms = unitToMs(hb.unit ?? "min");
		return {
			from: typeof hb.fromOffset === "number" ? new Date(now + hb.fromOffset * ms) : undefined,
			to: typeof hb.toOffset === "number" ? new Date(now + hb.toOffset * ms) : undefined
		};
	}
	if (hb.mode === "specific") {
		return {
			from: toValidDate(hb.from),
			to: toValidDate(hb.to)
		};
	}
	return {};
}

/**
 * Test whether a given ISO date string passes the datetime filter.
 * Returns true if the value passes (should be included), false if filtered out.
 */
export function passesDatetimeFilter(hb: DatetimeFilterValue, isoValue: string | undefined): boolean {
	if (!hb.mode) return true;
	const range = computeDateRange(hb);
	const hasRange = range.from || range.to;
	if (!hasRange) return true;
	if (!isoValue) return false;
	const t = toValidDate(isoValue);
	if (!t) return false;
	if (range.from && t < range.from) return false;
	if (range.to && t > range.to) return false;
	return true;
}
