import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import tailwindcss from '@tailwindcss/vite'; // 1. Import it

export default defineConfig({
    plugins: [
        tailwindcss(), // 2. Add it here (before sveltekit)
        sveltekit()
    ]
});