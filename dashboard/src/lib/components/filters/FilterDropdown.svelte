<script lang="ts">
    import { createEventDispatcher } from "svelte";

    export type Option<T = string> = { value: T; label: string };

    export let label: string;
    export let options: Option[] = [];
    export let value: string | undefined = undefined;
    export let placeholder: string = "Any";

    const dispatch = createEventDispatcher<{ change: string | undefined }>();

    let open = false;

    $: selectedLabel = options.find((o) => o.value === value)?.label ?? "";
    $: isActive = value !== undefined && value !== "";

    function select(opt: Option) {
        value = opt.value;
        open = false;
        dispatch("change", value);
    }

    function clear() {
        value = undefined;
        open = false;
        dispatch("change", undefined);
    }

    function handleClickOutside(e: MouseEvent) {
        const target = e.target as HTMLElement;
        if (!target.closest(".filter-dropdown-root")) {
            open = false;
        }
    }
</script>

<svelte:window on:click={handleClickOutside} />

<div class="filter-dropdown-root relative inline-block">
    <button
        class={"btn btn-sm rounded-full gap-1 " + (isActive ? "btn-primary" : "btn-ghost")}
        on:click|stopPropagation={() => (open = !open)}
    >
        <span class="truncate max-w-[10rem]">
            {label}{#if selectedLabel}: <span class="font-semibold">{selectedLabel}</span>{/if}
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

            <ul class="menu menu-sm p-0 gap-0.5">
                {#each options as opt (opt.value)}
                    <li>
                        <button
                            class={"rounded-lg " + (value === opt.value ? "active" : "")}
                            on:click={() => select(opt)}
                        >
                            {opt.label}
                        </button>
                    </li>
                {/each}
            </ul>

            {#if options.length === 0}
                <div class="py-3 text-center text-sm opacity-60">No options</div>
            {/if}
        </div>
    {/if}
</div>
