<script lang="ts">
    import { setContext, createEventDispatcher } from "svelte";
    import { writable, derived, get } from "svelte/store";
    import { FILTERS_CTX_KEY, type FilterValue, type FilterValues, type FiltersContext } from "$lib/components/filters/context";

    export let initialValues: FilterValues = {};

    const dispatch = createEventDispatcher<{ change: FilterValues }>();

    const draftValues = writable<FilterValues>({ ...initialValues });
    const appliedValues = writable<FilterValues>({ ...initialValues });

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

    function setValue(id: string, value: FilterValue) {
        draftValues.update((v) => ({ ...v, [id]: value }));
    }

    function clearValue(id: string) {
        draftValues.update((v) => {
            const next = { ...v };
            delete next[id];
            return next;
        });
    }

    function clearAll() {
        draftValues.set({});
        appliedValues.set({});
        dispatch("change", {});
    }

    function apply() {
        const next = get(draftValues);
        appliedValues.set(next);
        dispatch("change", next);
    }

    const appliedActiveCount = derived(appliedValues, ($values) => {
        return Object.values($values).reduce((acc, v) => (isActiveValue(v) ? acc + 1 : acc), 0);
    });

    const isDirty = derived([draftValues, appliedValues], ([$draft, $applied]) => {
        // Basic (fast) deep compare good enough for small filter objects.
        return JSON.stringify($draft) !== JSON.stringify($applied);
    });

    setContext<FiltersContext>(FILTERS_CTX_KEY, {
        values: draftValues,
        setValue,
        clearValue,
        clearAll,
        isActiveValue
    });
</script>

<div class="flex flex-wrap items-center gap-2">
    <slot />

    {#if $isDirty}
        <button class="btn btn-primary btn-sm" on:click={apply}>Apply</button>
    {:else}
        <button class="btn btn-primary btn-sm" on:click={apply} disabled>Apply</button>
    {/if}

    {#if $appliedActiveCount > 0}
        <button class="btn btn-ghost btn-sm" on:click={clearAll}>Clear</button>
    {/if}
</div>
