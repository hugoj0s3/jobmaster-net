import type { Writable } from "svelte/store";

export type FilterValue = unknown;
export type FilterValues = Record<string, FilterValue>;

export type FiltersContext = {
    values: Writable<FilterValues>;
    setValue: (id: string, value: FilterValue) => void;
    clearValue: (id: string) => void;
    clearAll: () => void;
    isActiveValue: (value: FilterValue) => boolean;
};

export const FILTERS_CTX_KEY = Symbol("filters");
