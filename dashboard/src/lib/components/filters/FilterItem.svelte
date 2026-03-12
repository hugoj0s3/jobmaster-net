<script lang="ts">
    import { getContext } from "svelte";
    import { get } from "svelte/store";
    import { FILTERS_CTX_KEY, type FiltersContext } from "$lib/components/filters/context";

    export type Option<T> = { value: T; label: string };
    
    export let id: string;
    export let label: string;
    export let type: "datetime" | "multiselect" | "single";

    export let options: Option<any>[] = [];

    export type DatePreset =
        | { type: "LAST_MINUTES"; minutes: number; label: string }
        | { type: "NEXT_HOURS"; hours: number; label: string };

    export let presets: DatePreset[] = [];

    const ctx = getContext<FiltersContext>(FILTERS_CTX_KEY);

    let fromLocal = "";
    let toLocal = "";
    let fromDate = "";
    let fromTime = "00:00";
    let toDate = "";
    let toTime = "23:59";
    let dateMode: "specific" | "relative" = "specific";

    function combineDateTime(date: string, time: string): string {
        if (!date) return "";
        return `${date}T${time || "00:00"}`;
    }

    function splitDateTime(local: string): { date: string; time: string } {
        if (!local) return { date: "", time: "" };
        const [d, t] = local.split("T");
        return { date: d ?? "", time: t ?? "00:00" };
    }

    function syncFromParts() {
        fromLocal = combineDateTime(fromDate, fromTime);
        toLocal = combineDateTime(toDate, toTime);
        syncDateTimeValue();
    }

    let relativeUnit: "sec" | "min" | "hour" | "day" = "min";
    let relativeFromText: string | number | null = "";
    let relativeToText: string | number | null = "";

    let searchText = "";

    let selectedMany: any[] = [];
    let selectedOne: any = "";

    function isEqual(a: unknown, b: unknown) {
        return a === b;
    }

    function toIsoOrUndefined(dateTimeLocal: unknown): string | undefined {
        if (dateTimeLocal === null || dateTimeLocal === undefined) return undefined;
        const raw = String(dateTimeLocal).trim();
        if (!raw) return undefined;
        const d = new Date(raw);
        if (Number.isNaN(d.getTime())) return undefined;
        return d.toISOString();
    }

    function isoToLocal(iso: string): string {
        const d = new Date(iso);
        if (Number.isNaN(d.getTime())) return "";
        const pad = (n: number) => String(n).padStart(2, "0");
        return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function syncPartsFromLocal() {
        const f = splitDateTime(fromLocal);
        fromDate = f.date;
        fromTime = f.time;
        const t = splitDateTime(toLocal);
        toDate = t.date;
        toTime = t.time;
    }

    function formatIsoShort(iso: string): string {
        const d = new Date(iso);
        if (Number.isNaN(d.getTime())) return "";
        const pad = (n: number) => String(n).padStart(2, "0");
        return `${pad(d.getMonth() + 1)}/${pad(d.getDate())}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
    }

    function applyPreset(preset: DatePreset) {
        if (preset.type === "LAST_MINUTES") {
            dateMode = "relative";
            relativeUnit = "min";
            relativeFromText = -preset.minutes;
            relativeToText = 0;
        } else if (preset.type === "NEXT_HOURS") {
            dateMode = "relative";
            relativeUnit = "min";
            relativeFromText = 0;
            relativeToText = preset.hours * 60;
        }
        syncRelativeValue();
    }

    function syncDateTimeValue() {
        ctx.setValue(id, {
            mode: dateMode,
            from: toIsoOrUndefined(fromLocal),
            to: toIsoOrUndefined(toLocal)
        });
    }

    function unitToMs(unit: "sec" | "min" | "hour" | "day"): number {
        if (unit === "sec") return 1000;
        if (unit === "min") return 60_000;
        if (unit === "hour") return 60 * 60_000;
        return 24 * 60 * 60_000;
    }

    function parseOffset(raw: unknown): number | undefined {
        if (raw === null || raw === undefined) return undefined;
        if (typeof raw === "number") return Number.isFinite(raw) ? raw : undefined;
        if (typeof raw === "object") return undefined;
        const t = String(raw).trim();
        if (!t) return undefined;
        const n = Number(t);
        return Number.isFinite(n) ? n : undefined;
    }

    function relativeToIsoRange() {
        const now = Date.now();
        const ms = unitToMs(relativeUnit);
        const fromOffset = parseOffset(relativeFromText);
        const toOffset = parseOffset(relativeToText);
        return {
            fromOffset,
            toOffset,
            from: typeof fromOffset === "number" ? new Date(now + fromOffset * ms).toISOString() : undefined,
            to: typeof toOffset === "number" ? new Date(now + toOffset * ms).toISOString() : undefined
        };
    }

    function syncRelativeValue() {
        const range = relativeToIsoRange();
        ctx.setValue(id, {
            mode: "relative",
            unit: relativeUnit,
            fromOffset: range.fromOffset,
            toOffset: range.toOffset,
            from: range.from,
            to: range.to
        });
    }

    function syncMultiValue() {
        ctx.setValue(id, selectedMany);
    }

    function syncSingleValue() {
        const val = selectedOne === "" ? undefined : selectedOne;
        if (val === undefined) ctx.clearValue(id);
        else ctx.setValue(id, val);
    }

    function clearMe() {
        if (type === "datetime") {
            fromLocal = "";
            toLocal = "";
            dateMode = "specific";
            relativeUnit = "min";
            relativeFromText = "";
            relativeToText = "";
        }

        if (type === "multiselect") {
            selectedMany = [];
        }

        if (type === "single") {
            selectedOne = "";
        }

        ctx.clearValue(id);
    }

    function valueSummary(): string {
        const v = get(ctx.values)[id];
        if (!ctx.isActiveValue(v)) return "";

        if (type === "datetime") {
            const o = (v ?? {}) as {
                mode?: "specific" | "relative";
                unit?: "sec" | "min" | "hour" | "day";
                fromOffset?: number;
                toOffset?: number;
                from?: string;
                to?: string;
            };
            if (o.mode === "relative") {
                const u = o.unit ?? "min";
                const fromVal = o.fromOffset;
                const toVal = o.toOffset;
                const f = fromVal != null && Number.isFinite(Number(fromVal)) ? Number(fromVal) : null;
                const t = toVal != null && Number.isFinite(Number(toVal)) ? Number(toVal) : null;
                if (f !== null && t !== null) {
                    if (t === 0 && f < 0) return `Last ${-f} ${u}`;
                    if (f === 0 && t > 0) return `Next ${t} ${u}`;
                    return `${f} to ${t} ${u}`;
                }
                if (f !== null) return `from ${f} ${u}`;
                if (t !== null) return `to ${t} ${u}`;
                return "";
            }

            const from = o.from ? formatIsoShort(o.from) : "";
            const to = o.to ? formatIsoShort(o.to) : "";
            if (from && to) return `${from} - ${to}`;
            if (from) return `from ${from}`;
            if (to) return `to ${to}`;
            return "";
        }

        if (type === "multiselect") {
            const arr = Array.isArray(v) ? v : [];
            const count = options.filter((o) => arr.some((x) => isEqual(x, o.value))).length;
            if (count === 0) return "";
            return `${count} selected`;
        }

        if (type === "single") {
            const opt = options.find((o) => isEqual(o.value, v));
            return opt?.label ?? "";
        }

        return "";
    }

    $: displayedOptions = options.filter((o) => o.label.toLowerCase().includes(searchText.trim().toLowerCase()));

    $: allOptionsSelected = options.length > 0 && options.every((o) => selectedMany.some((v) => isEqual(v, o.value)));

    function toggleSelectAll(checked: boolean) {
        if (checked) selectedMany = options.map((o) => o.value);
        else selectedMany = [];
        syncMultiValue();
    }

    // Ensure local UI reflects store when page re-renders.
    $: {
        const v = get(ctx.values)[id];
        if (type === "datetime") {
            const o = (v ?? {}) as {
                mode?: "specific" | "relative";
                unit?: "sec" | "min" | "hour" | "day";
                fromOffset?: number;
                toOffset?: number;
                from?: string;
                to?: string;
            };

            dateMode = o.mode ?? dateMode;
            if (o.mode === "relative") {
                relativeUnit = o.unit ?? relativeUnit;
                relativeFromText = typeof o.fromOffset === "number" ? String(o.fromOffset) : "";
                relativeToText = typeof o.toOffset === "number" ? String(o.toOffset) : "";
            }

            fromLocal = o.from ? isoToLocal(o.from) : "";
            toLocal = o.to ? isoToLocal(o.to) : "";
            syncPartsFromLocal();
        }

        if (type === "multiselect") {
            if (Array.isArray(v)) selectedMany = v;
        }

        if (type === "single") {
            selectedOne = (v as any) ?? "";
        }
    }
</script>

<div class="dropdown dropdown-bottom">
    <label
        tabindex="0"
        class={
            "btn btn-sm rounded-full " + (ctx.isActiveValue(get(ctx.values)[id]) ? "btn-primary" : "btn-ghost")
        }
    >
        <span class="truncate max-w-[12rem]">{label}{#if valueSummary()}: {/if}</span>
        {#if valueSummary()}
            <span class="truncate max-w-[18rem] font-semibold">{valueSummary()}</span>
        {/if}
        <svg xmlns="http://www.w3.org/2000/svg" class="ml-1 h-4 w-4 opacity-70" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="m19 9-7 7-7-7" />
        </svg>
    </label>

    <div
        tabindex="0"
        class="dropdown-content z-[1] mt-2 w-[22rem] rounded-box border border-base-300 bg-base-100 p-4 shadow"
    >
        <div class="flex items-center justify-between gap-3">
            <div class="font-semibold">{label}</div>
            <div class="flex items-center gap-2">
                {#if ctx.isActiveValue(get(ctx.values)[id])}
                    <button class="btn btn-ghost btn-xs" on:click={clearMe}>Clear</button>
                {/if}
            </div>
        </div>

        {#if type === "datetime"}
            <div class="mt-3">
                <div class="flex items-center gap-4">
                    <label class="label cursor-pointer gap-2 py-0">
                        <input
                            type="radio"
                            class="radio radio-sm"
                            name={`filter-dt-mode-${id}`}
                            checked={dateMode === "specific"}
                            on:change={() => {
                                dateMode = "specific";
                                syncDateTimeValue();
                            }}
                        />
                        <span class="text-sm">Specific Date</span>
                    </label>

                    <label class="label cursor-pointer gap-2 py-0">
                        <input
                            type="radio"
                            class="radio radio-sm"
                            name={`filter-dt-mode-${id}`}
                            checked={dateMode === "relative"}
                            on:change={() => {
                                dateMode = "relative";
                                syncRelativeValue();
                            }}
                        />
                        <span class="text-sm">Relative time</span>
                    </label>
                </div>

                {#if presets.length > 0}
                    <div class="mt-3 flex flex-wrap gap-2">
                        {#each presets as preset (preset.label)}
                            <button
                                class="btn btn-ghost btn-xs"
                                on:click={() => applyPreset(preset)}
                            >
                                {preset.label}
                            </button>
                        {/each}
                    </div>
                {/if}

                {#if dateMode === "specific"}
                    <div class="mt-3 space-y-3">
                        <div>
                            <span class="label-text font-medium">From</span>
                            <div class="mt-1 flex items-center gap-2">
                                <input
                                    type="date"
                                    class="input input-bordered input-sm flex-1"
                                    bind:value={fromDate}
                                    on:input={syncFromParts}
                                />
                                <input
                                    type="time"
                                    class="input input-bordered input-sm w-[7rem]"
                                    bind:value={fromTime}
                                    on:input={syncFromParts}
                                />
                            </div>
                        </div>

                        <div>
                            <span class="label-text font-medium">To</span>
                            <div class="mt-1 flex items-center gap-2">
                                <input
                                    type="date"
                                    class="input input-bordered input-sm flex-1"
                                    bind:value={toDate}
                                    on:input={syncFromParts}
                                />
                                <input
                                    type="time"
                                    class="input input-bordered input-sm w-[7rem]"
                                    bind:value={toTime}
                                    on:input={syncFromParts}
                                />
                            </div>
                        </div>
                    </div>
                {:else}
                    <div class="mt-3 grid grid-cols-3 gap-3">
                        <div class="form-control">
                            <label class="label"><span class="label-text">Unit</span></label>
                            <select
                                class="select select-bordered select-sm"
                                bind:value={relativeUnit}
                                on:change={() => syncRelativeValue()}
                            >
                                <option value="sec">sec</option>
                                <option value="min">min</option>
                                <option value="hour">hour</option>
                                <option value="day">day</option>
                            </select>
                        </div>

                        <div class="form-control">
                            <label class="label"><span class="label-text">From</span></label>
                            <input
                                type="number"
                                class="input input-bordered input-sm"
                                bind:value={relativeFromText}
                                on:input={() => syncRelativeValue()}
                            />
                        </div>

                        <div class="form-control">
                            <label class="label"><span class="label-text">To</span></label>
                            <input
                                type="number"
                                class="input input-bordered input-sm"
                                bind:value={relativeToText}
                                on:input={() => syncRelativeValue()}
                            />
                        </div>
                    </div>
                    <div class="mt-2 text-xs opacity-70">Offsets are relative to now (example: -2 to +2 min)</div>
                {/if}
            </div>
        {/if}

        {#if type === "multiselect"}
            <div class="mt-3">
                <input
                    class="input input-bordered input-sm w-full"
                    placeholder={`Search ${label}`}
                    bind:value={searchText}
                />

                <div class="mt-3 flex items-center justify-between">
                    <label class="label cursor-pointer gap-2 py-0">
                        <input
                            type="checkbox"
                            class="checkbox checkbox-sm"
                            checked={allOptionsSelected}
                            on:change={(e) => toggleSelectAll((e.currentTarget as HTMLInputElement).checked)}
                        />
                        <span class="text-sm">Select all</span>
                    </label>

                    <div class="text-xs opacity-70">{selectedMany.length} selected</div>
                </div>

                <div class="mt-2 max-h-56 overflow-auto rounded-box border border-base-200 p-2">
                    {#each displayedOptions as o (o.label)}
                        <label class="label cursor-pointer gap-2 py-1">
                            <input
                                type="checkbox"
                                class="checkbox checkbox-sm"
                                checked={selectedMany.some((v) => isEqual(v, o.value))}
                                on:change={(e) => {
                                    const checked = (e.currentTarget as HTMLInputElement).checked;
                                    const next = new Set(selectedMany);
                                    if (checked) next.add(o.value);
                                    else next.delete(o.value);
                                    selectedMany = [...next];
                                    syncMultiValue();
                                }}
                            />
                            <span class="text-sm">{o.label}</span>
                        </label>
                    {/each}
                </div>
            </div>
        {/if}

        {#if type === "single"}
            <div class="mt-3">
                <select class="select select-bordered select-sm w-full" bind:value={selectedOne} on:change={syncSingleValue}>
                    <option value="">Any</option>
                    {#each options as o (o.label)}
                        <option value={o.value}>{o.label}</option>
                    {/each}
                </select>
            </div>
        {/if}
    </div>
</div>
