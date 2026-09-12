import adapter from '@sveltejs/adapter-static';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		adapter: adapter({
			fallback: 'index.html',
			strict: false
		}),
		paths: {
			// This is useful if the dashboard is served at a subpath, e.g. /jm-dashboard
			// However, since we dynamically provide BasePath via C#, we should leave relative paths or handle it at runtime.
			relative: true
		}
	}
};

export default config;
