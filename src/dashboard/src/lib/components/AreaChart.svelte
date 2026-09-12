<script lang="ts">
	export let data: { time: number; value: number }[] = [];
	export let maxValue: number = 100;
	export let color: string = "oklch(var(--su))";
	export let fillOpacity: number = 0.18;
	export let label: string = "";
	export let unit: string = "%";
	export let gridLines: number = 4;

	const W = 500;
	const H = 160;
	const PAD_LEFT = 40;
	const PAD_RIGHT = 8;
	const PAD_TOP = 8;
	const PAD_BOTTOM = 24;

	$: chartW = W - PAD_LEFT - PAD_RIGHT;
	$: chartH = H - PAD_TOP - PAD_BOTTOM;

	$: latestValue = data.length > 0 ? data[data.length - 1].value : null;
	$: gaugePercent = latestValue != null ? Math.min(100, Math.max(0, (latestValue / maxValue) * 100)) : 0;

	$: chartData = (() => {
		if (data.length === 0) return [];
		if (data.length === 1) {
			const now = Date.now();
			return [
				{ time: now - 10000, value: data[0].value },
				{ time: now, value: data[0].value }
			];
		}
		return data;
	})();

	$: points = (() => {
		if (chartData.length === 0) return [];
		const minT = chartData[0].time;
		const maxT = chartData[chartData.length - 1].time;
		const rangeT = maxT - minT || 1;

		return chartData.map((d) => ({
			x: PAD_LEFT + ((d.time - minT) / rangeT) * chartW,
			y: PAD_TOP + chartH - (Math.min(d.value, maxValue) / maxValue) * chartH,
			value: d.value,
			time: d.time
		}));
	})();

	$: linePath = points.length > 0
		? "M" + points.map((p) => `${p.x},${p.y}`).join(" L")
		: "";

	$: areaPath = points.length > 0
		? `${linePath} L${points[points.length - 1].x},${PAD_TOP + chartH} L${points[0].x},${PAD_TOP + chartH} Z`
		: "";

	$: yLabels = Array.from({ length: gridLines + 1 }, (_, i) => {
		const val = Math.round((maxValue / gridLines) * (gridLines - i));
		const y = PAD_TOP + (i / gridLines) * chartH;
		return { val, y };
	});

	function formatTime(ts: number): string {
		const d = new Date(ts);
		return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
	}

	$: timeLabels = (() => {
		if (chartData.length < 2) return [];
		const minT = chartData[0].time;
		const maxT = chartData[chartData.length - 1].time;
		const count = Math.min(5, chartData.length);
		return Array.from({ length: count }, (_, i) => {
			const t = minT + ((maxT - minT) / (count - 1)) * i;
			const x = PAD_LEFT + (i / (count - 1)) * chartW;
			return { t, x, label: formatTime(t) };
		});
	})();
</script>

<div class="w-full">
	<!-- Current value gauge bar -->
	<div class="mb-3 flex items-center gap-3">
		<span class="text-2xl font-semibold" style="color: {color};">
			{latestValue != null ? Math.round(latestValue) : '—'}{latestValue != null ? unit : ''}
		</span>
		<div class="flex-1">
			<div class="h-2.5 w-full rounded-full bg-base-300/60 overflow-hidden">
				<div
					class="h-full rounded-full transition-all duration-500"
					style="width: {gaugePercent}%; background: {color};"
				></div>
			</div>
		</div>
		<span class="text-xs text-base-content/40">{maxValue}{unit}</span>
	</div>

	<!-- SVG area chart -->
	{#if data.length === 0}
		<div class="flex h-32 items-center justify-center text-sm text-base-content/40">
			Waiting for data{label ? ` (${label})` : ''}…
		</div>
	{:else}
		<svg viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet" class="w-full" style="height: 160px;">
			<!-- Grid lines -->
			{#each yLabels as yl}
				<line
					x1={PAD_LEFT} y1={yl.y}
					x2={W - PAD_RIGHT} y2={yl.y}
					stroke="currentColor" stroke-opacity="0.08" stroke-width="1"
				/>
				<text
					x={PAD_LEFT - 6} y={yl.y + 4}
					text-anchor="end"
					fill="currentColor" fill-opacity="0.4"
					font-size="10"
				>{yl.val}{unit}</text>
			{/each}

			<!-- Time labels -->
			{#each timeLabels as tl}
				<text
					x={tl.x} y={H - 4}
					text-anchor="middle"
					fill="currentColor" fill-opacity="0.35"
					font-size="9"
				>{tl.label}</text>
			{/each}

			<!-- Area fill -->
			<path d={areaPath} style="fill: {color}; opacity: {fillOpacity};" />

			<!-- Line -->
			<path d={linePath} fill="none" style="stroke: {color}; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round;" />

			<!-- Data point dots -->
			{#each points as p, i}
				{#if i === points.length - 1}
					<circle cx={p.x} cy={p.y} r="4" style="fill: {color};" />
				{:else}
					<circle cx={p.x} cy={p.y} r="2" style="fill: {color}; opacity: 0.5;" />
				{/if}
			{/each}
		</svg>

		<div class="mt-1 text-right text-xs text-base-content/40">
			{data.length} sample{data.length !== 1 ? 's' : ''} collected
		</div>
	{/if}
</div>
