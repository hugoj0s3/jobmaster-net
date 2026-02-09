<script lang="ts">
    import { page } from "$app/stores";
</script>

<div class="min-h-screen bg-base-200">
    <div class="mx-auto w-full max-w-6xl px-6 py-6">
        <div class="text-sm opacity-70 font-mono">Cluster: {$page.params.cluster}</div>
        <h1 class="mt-2 text-2xl font-semibold">Cluster Home (Dummy)</h1>
        <p class="mt-3 opacity-80">
            This is a placeholder home page for the cluster root.
        </p>
        <div class="mt-4">
            <a class="link" href={`/${$page.params.cluster}/dummy`}>Go to Dummy</a>
        </div>
    </div>
</div>
