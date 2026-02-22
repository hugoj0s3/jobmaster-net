import { goto } from "$app/navigation";
import { page } from "$app/stores";
import { get } from "svelte/store";

export type ParamDef<T = string> = {
	defaultValue: T;
	serialize?: (value: T) => string;
	deserialize?: (raw: string) => T;
};

type ParamDefs = Record<string, ParamDef<any>>;

type Values<D extends ParamDefs> = {
	[K in keyof D]: D[K] extends ParamDef<infer T> ? T : never;
};

function defaultSerialize(v: unknown): string {
	if (Array.isArray(v)) return v.join(",");
	return String(v ?? "");
}

function defaultDeserialize(raw: string): string {
	return raw;
}

export function readUrlParams<D extends ParamDefs>(defs: D): Values<D> {
	const url = get(page).url;
	const result: Record<string, unknown> = {};

	for (const [key, def] of Object.entries(defs)) {
		const raw = url.searchParams.get(key);
		if (raw !== null && raw !== "") {
			const deserialize = def.deserialize ?? defaultDeserialize;
			result[key] = deserialize(raw);
		} else {
			result[key] = def.defaultValue;
		}
	}

	return result as Values<D>;
}

export function writeUrlParams<D extends ParamDefs>(defs: D, values: Values<D>): void {
	const currentPage = get(page);
	const url = new URL(currentPage.url);
	let changed = false;

	for (const [key, def] of Object.entries(defs)) {
		const serialize = def.serialize ?? defaultSerialize;
		const value = values[key];
		const serialized = serialize(value);
		const defaultSerialized = serialize(def.defaultValue);

		if (serialized === defaultSerialized || serialized === "") {
			if (url.searchParams.has(key)) {
				url.searchParams.delete(key);
				changed = true;
			}
		} else {
			if (url.searchParams.get(key) !== serialized) {
				url.searchParams.set(key, serialized);
				changed = true;
			}
		}
	}

	if (changed) {
		goto(url.pathname + url.search, { replaceState: true, keepFocus: true, noScroll: true });
	}
}

export const Serializers = {
	number: {
		serialize: (v: number) => String(v),
		deserialize: (raw: string) => {
			const n = Number(raw);
			return Number.isFinite(n) ? n : 0;
		}
	},
	boolean: {
		serialize: (v: boolean) => (v ? "1" : "0"),
		deserialize: (raw: string) => raw === "1" || raw === "true"
	},
	numberArray: {
		serialize: (v: number[]) => v.join(","),
		deserialize: (raw: string): number[] =>
			raw
				.split(",")
				.map(Number)
				.filter((n) => Number.isFinite(n))
	},
	stringArray: {
		serialize: (v: string[]) => v.join(","),
		deserialize: (raw: string): string[] =>
			raw.split(",").filter((s) => s.length > 0)
	},
	json: {
		serialize: (v: unknown) => {
			try {
				const s = JSON.stringify(v);
				return s === "{}" || s === "null" ? "" : s;
			} catch {
				return "";
			}
		},
		deserialize: (raw: string) => {
			try {
				return JSON.parse(raw);
			} catch {
				return {};
			}
		}
	}
};
