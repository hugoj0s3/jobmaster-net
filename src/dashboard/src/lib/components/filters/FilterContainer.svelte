<script lang="ts">
    import { setContext, createEventDispatcher } from "svelte";
    import { writable } from "svelte/store";
    import {
        FILTERS_CTX_KEY,
        type FilterValue,
        type FilterValues,
        type FiltersContext
    } from "$lib/components/filters/context";

    let initialized = false;

    $: if (!initialized) {
        values.set({ ...initialValues });
        initialized = true;
    }

    export let initialValues: FilterValues = {};
    export let onChange: ((values: FilterValues) => void) | undefined = undefined;

    const dispatch = createEventDispatcher<{ change: FilterValues }>();

    const values = writable<FilterValues>({ ...initialValues });

    function isActiveValue(value: FilterValue): boolean {
        if (value === null || value === undefined) return false;
        if (typeof value === "string") return value.trim().length > 0;
        if (Array.isArray(value)) return value.length > 0;
        if (typeof value === "object") {
            const o = value as Record<string, unknown>;
            return Object.values(o).some(isActiveValue);
        }
        return true;
    }

    function emitChange(next: FilterValues) {
        dispatch("change", next);
        onChange?.(next);
    }

    function setValue(id: string, value: FilterValue) {
        values.update((current) => {
            const next = { ...current, [id]: value };
            emitChange(next);
            return next;
        });
    }

    function clearValue(id: string) {
        values.update((current) => {
            const next = { ...current };
            delete next[id];
            emitChange(next);
            return next;
        });
    }

    function clearAll() {
        const next = {};
        values.set(next);
        emitChange(next);
    }

    setContext<FiltersContext>(FILTERS_CTX_KEY, {
        values,
        setValue,
        clearValue,
        clearAll,
        isActiveValue
    });
</script>

<div class="flex flex-wrap items-center gap-2">
    <slot />
</div>
