<script lang="ts">
	import { page } from '$app/state';
	import { goto } from '$app/navigation';

	let statusCode = $derived(page.status);
	let errorMessage = $derived(page.error?.message ?? 'An unexpected error occurred');
</script>

<div class="min-h-screen bg-base-100 flex items-center justify-center px-6">
	<div class="text-center max-w-2xl">
		<div class="mb-8">
			<h1 class="text-9xl font-bold text-primary opacity-20">{statusCode}</h1>
		</div>

		<div class="space-y-4">
			<h2 class="text-3xl font-semibold">
				{#if statusCode === 404}
					Page Not Found
				{:else if statusCode === 500}
					Internal Server Error
				{:else}
					Error {statusCode}
				{/if}
			</h2>

			<p class="text-base-content/70 text-lg">
				{#if statusCode === 404}
					The page you're looking for doesn't exist or has been moved.
				{:else}
					{errorMessage}
				{/if}
			</p>
		</div>

		<div class="mt-8 flex gap-4 justify-center">
			<button
				class="btn btn-primary"
				on:click={() => goto('/')}
			>
				Go to Home
			</button>
			<button
				class="btn btn-ghost"
				on:click={() => window.history.back()}
			>
				Go Back
			</button>
		</div>
	</div>
</div>
