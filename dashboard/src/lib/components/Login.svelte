<script lang="ts">
    let { auth, onLogin } = $props();

    let selectedProvider = $state(auth.providers?.[0]);
</script>

<div class="min-h-screen bg-base-100 text-base-content flex items-center justify-center p-6">
    <div class="w-full max-w-md">
        <!-- Header -->
        <div class="text-center mb-6">
            <h1 class="text-4xl font-black tracking-tight">
                <span class="text-base-content">Job</span><span class="text-primary">Master</span>
            </h1>
            <p class="opacity-50 text-sm mt-1">Orchestration Control Plane</p>
        </div>

        <!-- Card -->
        <div class="card w-full shadow-xl bg-base-200 border border-base-300">
            <!-- Provider Tabs -->
            <div class="p-4 pb-0">
                <div class="tabs tabs-boxed bg-base-100 border border-base-300">
                    {#each auth.providers as provider}
                        <button
                                type="button"
                                class="tab flex-1 {selectedProvider?.type === provider.type ? 'tab-active' : ''}"
                                onclick={() => (selectedProvider = provider)}
                        >
                            {provider.displayName}
                        </button>
                    {/each}
                </div>
            </div>

            <!-- Form -->
            <form
                    class="card-body pt-4"
                    onsubmit={(e) => {
          e.preventDefault();
          onLogin();
        }}
            >
                {#if selectedProvider?.type === "API_KEY"}
                    <div class="form-control w-full">
                        <label class="label py-1">
                            <span class="label-text font-semibold">API Secret Key</span>
                        </label>

                        <input
                                type="password"
                                placeholder="Paste your key here"
                                class="input input-bordered w-full bg-base-100"
                                required
                                autocomplete="off"
                        />

                        <button type="submit" class="btn btn-primary w-full mt-4">Set Key</button>
                    </div>

                {:else if selectedProvider?.type === "USER_PASSWORD"}
                    <div class="form-control w-full">
                        <label class="label py-1">
                            <span class="label-text font-semibold">Username</span>
                        </label>
                        <input
                                type="text"
                                class="input input-bordered w-full bg-base-100"
                                required
                                autocomplete="username"
                        />
                    </div>

                    <div class="form-control w-full mt-3">
                        <label class="label py-1">
                            <span class="label-text font-semibold">Password</span>
                        </label>
                        <input
                                type="password"
                                class="input input-bordered w-full bg-base-100"
                                required
                                autocomplete="current-password"
                        />
                    </div>

                    <button type="submit" class="btn btn-primary w-full mt-5">Login</button>

                {:else if selectedProvider?.type === "JWT_CUSTOM_FORM"}
                    {#each selectedProvider.fields as field (field.id)}
                        <div class="form-control w-full mt-3">
                            <label class="label py-1">
                                <span class="label-text font-semibold">{field.label}</span>
                            </label>

                            <input
                                    type={field.type}
                                    class="input input-bordered w-full bg-base-100"
                                    required={field.isRequired === true}
                                    autocomplete={field.id === "username" ? "username" : field.id === "password" ? "current-password" : "off"}
                            />
                        </div>
                    {/each}

                    <button type="submit" class="btn btn-primary w-full mt-5">Login</button>

                {:else if selectedProvider?.type === "JWT_SSO"}
                    <div class="py-4 text-center space-y-4">
                        <div class="p-4 bg-base-100 rounded-lg border border-base-300 border-dashed">
                            <p class="text-[10px] font-bold opacity-50 tracking-widest">EXTERNAL IDENTITY PROVIDER</p>
                        </div>

                        <button type="button" class="btn btn-primary btn-block shadow-lg" onclick={() => onLogin()}>
                            Proceed to SSO
                        </button>
                    </div>
                {/if}
            </form>
        </div>

        <!-- Footer -->
        <div class="mt-6 text-center text-[10px] opacity-40 font-mono">
            JobMaster Dashboard
        </div>
    </div>
</div>
