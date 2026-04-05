<script lang="ts">
	import { createEventDispatcher } from "svelte";

	export type SearchSelectOption = {
		value: string;
		label: string;
		description?: string;
	};

	export let open = false;
	export let title = "Pesquisar";
	export let placeholder = "Digite para pesquisar...";
	export let options: SearchSelectOption[] = [];
	export let selectedValue = "";
	export let emptyText = "Nenhum resultado encontrado";

	const dispatch = createEventDispatcher<{
		close: void;
		select: SearchSelectOption;
	}>();

	let searchText = "";

	$: filteredOptions = options.filter((item) => {
		const term = searchText.trim().toLowerCase();
		if (!term) return true;

		return (
			item.label.toLowerCase().includes(term) ||
			item.value.toLowerCase().includes(term) ||
			(item.description ?? "").toLowerCase().includes(term)
		);
	});

	$: if (open) {
		searchText = "";
	}

	function closeModal() {
		dispatch("close");
	}

	function selectOption(option: SearchSelectOption) {
		dispatch("select", option);
	}
</script>

{#if open}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
		role="dialog"
		aria-modal="true"
	>
		<div class="card w-full max-w-xl bg-base-100 shadow-xl">
			<div class="card-body gap-4">
				<div class="flex items-start justify-between gap-4">
					<h3 class="text-lg font-semibold">{title}</h3>
					<button class="btn btn-ghost btn-sm btn-square" aria-label="Fechar" on:click={closeModal}>
						✕
					</button>
				</div>

				<input
					class="input input-bordered w-full"
					type="text"
					bind:value={searchText}
					placeholder={placeholder}
					autofocus
				/>

				<div class="max-h-80 overflow-y-auto rounded-xl border border-base-300">
					{#if filteredOptions.length > 0}
						{#each filteredOptions as option (option.value)}
							<button
								type="button"
								class={`w-full border-b border-base-300 px-4 py-3 text-left last:border-b-0 hover:bg-base-200 ${
                                    option.value === selectedValue ? "bg-base-200" : ""
                                }`}
								on:click={() => selectOption(option)}
							>
								<div class="flex items-center justify-between gap-3">
									<div class="min-w-0">
										<div class="truncate font-medium">{option.label}</div>
										{#if option.description}
											<div class="truncate text-sm opacity-70">{option.description}</div>
										{/if}
									</div>

									{#if option.value === selectedValue}
										<div class="badge badge-primary badge-sm">Selecionado</div>
									{/if}
								</div>
							</button>
						{/each}
					{:else}
						<div class="px-4 py-6 text-sm opacity-70">
							{emptyText}
						</div>
					{/if}
				</div>

				<div class="card-actions justify-end">
					<button class="btn btn-ghost" on:click={closeModal}>Fechar</button>
				</div>
			</div>
		</div>

		<button
			class="absolute inset-0 -z-10 cursor-default"
			aria-label="Fechar modal"
			on:click={closeModal}
		></button>
	</div>
{/if}