<script lang="ts">
    import { createEventDispatcher } from "svelte";

    export type Option<T = string> = { value: T; label: string };

    export let label: string;
    export let options: Option[] = [];
    export let values: string[] = [];

    const dispatch = createEventDispatcher<{ change: string[] }>();

    let open = false;

    $: isActive = values.length > 0;
    $: allSelected = options.length > 0 && options.every((o) => values.includes(o.value));

    function toggle(opt: Option) {
        if (values.includes(opt.value)) {
            values = values.filter((v) => v !== opt.value);
        } else {
            values = [...values, opt.value];
        }
        dispatch("change", values);
    }

    function toggleAll() {
        if (allSelected) {
            values = [];
        } else {
            values = options.map((o) => o.value);
        }
        dispatch("change", values);
    }

    function clear() {
        values = [];
        open = false;
        dispatch("change", values);
    }

    function handleClickOutside(e: MouseEvent) {
        const target = e.target as HTMLElement;
        if (!target.closest(".filter-dropdown-multi-root")) {
            open = false;
        }
    }
</script>

<svelte:window on:click={handleClickOutside} />

<div class="filter-dropdown-multi-root relative inline-block">
    <button
        class={"btn btn-sm rounded-full gap-1 " + (isActive ? "btn-primary" : "btn-ghost")}
        on:click|stopPropagation={() => (open = !open)}
    >
        <span class="truncate max-w-[12rem]">
            {label}{#if isActive}: <span class="font-semibold">{values.length} selected</span>{/if}
        </span>
        <svg
            xmlns="http://www.w3.org/2000/svg"
            class={"h-4 w-4 opacity-70 transition-transform " + (open ? "rotate-180" : "")}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            stroke-width="2"
        >
            <path stroke-linecap="round" stroke-linejoin="round" d="m19 9-7 7-7-7" />
        </svg>
    </button>

    {#if open}
        <div
            class="absolute left-0 z-[1] mt-2 min-w-[14rem] rounded-box border border-base-300 bg-base-100 p-3 shadow-lg"
            on:click|stopPropagation
        >
            <div class="flex items-center justify-between gap-3 mb-2">
                <div class="font-semibold text-sm">{label}</div>
                {#if isActive}
                    <button class="btn btn-ghost btn-xs" on:click={clear}>Clear</button>
                {/if}
            </div>

            <div class="flex items-center justify-between mb-2">
                <label class="label cursor-pointer gap-2 py-0">
                    <input
                        type="checkbox"
                        class="checkbox checkbox-sm"
                        checked={allSelected}
                        on:change={toggleAll}
                    />
                    <span class="text-sm">Select all</span>
                </label>
                <span class="text-xs opacity-60">{values.length} selected</span>
            </div>

            <div class="max-h-56 overflow-auto rounded-box border border-base-200 p-2 flex flex-col gap-0.5">
                {#each options as opt (opt.value)}
                    <label class="label cursor-pointer gap-2 py-1 rounded-lg hover:bg-base-200 px-2">
                        <input
                            type="checkbox"
                            class="checkbox checkbox-sm"
                            checked={values.includes(opt.value)}
                            on:change={() => toggle(opt)}
                        />
                        <span class="text-sm flex-1">{opt.label}</span>
                    </label>
                {/each}
            </div>

            {#if options.length === 0}
                <div class="py-3 text-center text-sm opacity-60">No options</div>
            {/if}
        </div>
    {/if}
</div>
