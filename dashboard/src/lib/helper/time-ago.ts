export function formatAgeShort(ms: number): string {
	const s = Math.max(0, Math.floor(ms / 1000));
	if (s < 60) return `${s}s`;

	const m = Math.floor(s / 60);
	if (m < 60) return `${m}m`;

	const h = Math.floor(m / 60);
	return `${h}h`;
}

export function lastUpdatedAgo(now: Date, lastUpdatedAt: Date): string {
	return formatAgeShort(now.getTime() - lastUpdatedAt.getTime());
}