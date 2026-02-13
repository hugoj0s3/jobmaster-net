<script lang="ts">
    export let pageIndex: number;
    export let pageSize: number;
    export let totalCount: number;
    export let currentCount: number;

    export let disabled: boolean = false;
    export let showFirstLast: boolean = true;
	export let showPageSize: boolean = false;
	export let pageSizeOptions: number[] = [10, 12, 25, 50, 100];

    let maxPageIndex = 0;

    $: maxPageIndex = Math.max(0, Math.ceil(totalCount / pageSize) - 1);
    $: {
        const clamped = Math.max(0, Math.min(pageIndex, maxPageIndex));
        if (pageIndex !== clamped) pageIndex = clamped;
    }

    $: safePageIndex = pageIndex;

    $: start = totalCount === 0 ? 0 : safePageIndex * pageSize + 1;
    $: end = totalCount === 0 ? 0 : Math.min(totalCount, safePageIndex * pageSize + currentCount);

    function goTo(nextIndex: number) {
        pageIndex = Math.max(0, Math.min(nextIndex, maxPageIndex));
    }
</script>

<div class="flex items-center gap-2 text-xs opacity-80">
	{#if showPageSize}
		<div class="hidden sm:flex items-center gap-2">
			<span class="opacity-70">Size</span>
			<select class="select select-bordered select-xs" bind:value={pageSize} disabled={disabled}>
				{#each pageSizeOptions as opt (opt)}
					<option value={opt}>{opt}</option>
				{/each}
			</select>
		</div>
	{/if}

    <div class="hidden sm:block">
        <span class="font-semibold">{start}-{end}</span>
        <span class="opacity-70">/</span>
        <span class="font-semibold">{totalCount}</span>
    </div>

    <div class="flex items-center gap-1">
        {#if showFirstLast}
            <button class="btn btn-ghost btn-xs" on:click={() => goTo(0)} disabled={disabled || safePageIndex <= 0}>
                «
            </button>
        {/if}

        <button class="btn btn-ghost btn-xs" on:click={() => goTo(safePageIndex - 1)} disabled={disabled || safePageIndex <= 0}>
            ‹
        </button>

        <div class="hidden sm:block">
            Page <span class="font-semibold">{safePageIndex + 1}</span>/<span class="font-semibold">{maxPageIndex + 1}</span>
        </div>

        <button class="btn btn-ghost btn-xs" on:click={() => goTo(safePageIndex + 1)} disabled={disabled || safePageIndex >= maxPageIndex}>
            ›
        </button>

        {#if showFirstLast}
            <button class="btn btn-ghost btn-xs" on:click={() => goTo(maxPageIndex)} disabled={disabled || safePageIndex >= maxPageIndex}>
                »
            </button>
        {/if}
    </div>
</div>
