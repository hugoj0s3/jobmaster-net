<script lang="ts">
    let { class: className = '' } = $props();
</script>

<svg viewBox="0 0 200 200" class={className} xmlns="http://www.w3.org/2000/svg">
    <rect x="10" y="10" width="180" height="180" rx="40" ry="40" style="fill: var(--color-logo)" />
    <g style="stroke: var(--color-logo-content); stroke-opacity: 0.12; stroke-width: 1.2" fill="none">
        <line x1="60" y1="30" x2="60" y2="170"/>
        <line x1="100" y1="30" x2="100" y2="170"/>
        <line x1="140" y1="30" x2="140" y2="170"/>
        <line x1="30" y1="60" x2="170" y2="60"/>
        <line x1="30" y1="100" x2="170" y2="100"/>
        <line x1="30" y1="140" x2="170" y2="140"/>
    </g>
    <g style="fill: var(--color-logo-content); opacity: 0.25">
        <circle cx="60" cy="60" r="3"/><circle cx="100" cy="60" r="3"/><circle cx="140" cy="60" r="3"/>
        <circle cx="60" cy="100" r="3"/><circle cx="140" cy="100" r="3"/>
        <circle cx="60" cy="140" r="3"/><circle cx="100" cy="140" r="3"/><circle cx="140" cy="140" r="3"/>
    </g>
    <text x="100" y="118" text-anchor="middle" font-family="'Nunito Variable','Nunito',system-ui,sans-serif" font-weight="800" font-size="72" letter-spacing="-2" style="fill: var(--color-logo-content)">JM</text>
</svg>
