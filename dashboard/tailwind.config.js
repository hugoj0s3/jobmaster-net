// tailwind.config.js
import daisyui from "daisyui";

export default {
    content: ["./src/**/*.{html,js,svelte,ts}"],
    plugins: [daisyui],
    daisyui: {
        // ✅ Add your custom themes here so DaisyUI recognizes the names
        themes: ["light", "dark", "corporate", "dracula", "night", "business", "jobmaster-light", "jobmaster-dark"],
    },
};