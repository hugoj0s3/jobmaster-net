import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	server: {
		proxy: {
			"/jm-api": {
				target: "https://localhost:7247",
				changeOrigin: true,
				secure: false
			}
		}
	}
});