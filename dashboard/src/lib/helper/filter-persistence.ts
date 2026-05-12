import { browser } from "$app/environment";

/**
 * Read a saved filter from localStorage or fall back to the provided default.
 */
export function readSavedFilter(key: string, fallback: string): string {
    if (!browser) return fallback;
    const saved = localStorage.getItem(key);
    return saved && saved.trim() ? saved.trim() : fallback;
}

/**
 * Persist a filter value into localStorage. Empty values clear the key.
 */
export function writeSavedFilter(key: string, value: string): void {
    if (!browser) return;
    if (value) localStorage.setItem(key, value);
    else localStorage.removeItem(key);
}

/**
 * Persist the current filter when the page is being unloaded.
 * Returns a cleanup function to remove the listeners.
 */
export function setupFilterPersistOnUnload(key: string, getValue: () => string): () => void {
    if (!browser) return () => {};
    const handler = () => writeSavedFilter(key, getValue());
    window.addEventListener("pagehide", handler);
    window.addEventListener("beforeunload", handler);
    return () => {
        window.removeEventListener("pagehide", handler);
        window.removeEventListener("beforeunload", handler);
    };
}
