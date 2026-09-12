import { JobMasterConfigUtil } from '$lib/api/job-master-config-util';
import type { Credentials } from '$lib/api/credentials';

export class AuthRetentionUtil {
	private static credentials: Credentials | null = null;

	static async storeCredentials(value: Credentials): Promise<boolean> {
		const config = await JobMasterConfigUtil.loadConfig();
		if (!config) return false;

		const serialized = JSON.stringify(value);
		if (config.authRetentionMode === 'client') {
			sessionStorage.setItem('jm_credentials', serialized);
			this.credentials = JSON.parse(serialized) as Credentials;
			return true;
		}

		if (config.authRetentionMode === 'server') {
			const basePath = JobMasterConfigUtil.getBasePath();

			// Open session if not already opened
			if (!sessionStorage.getItem('jm_session_opened')) {
				try {
					const openRes = await fetch(`${basePath}/credentials/open-session`, {
						method: 'POST'
					});
					if (!openRes.ok) {
						console.error('Failed to open credentials session on server:', openRes.statusText);
						return false;
					}
					sessionStorage.setItem('jm_session_opened', 'true');
				} catch (err) {
					console.error('Error opening credentials session on server:', err);
					return false;
				}
			}

			// Store the credentials serialized under a generic "credentials" key
			try {
				const storeRes = await fetch(`${basePath}/credentials/jm_credentials`, {
					method: 'POST',
					headers: {
						'Content-Type': 'application/json'
					},
					body: JSON.stringify({
						secrets: {
							credentials: serialized
						}
					})
				});

				if (!storeRes.ok) {
					console.error('Failed to store credentials on server:', storeRes.statusText);
					return false;
				}

				this.credentials = JSON.parse(serialized) as Credentials;
				return true;
			} catch (err) {
				console.error('Error storing credentials on server:', err);
				return false;
			}
		}

		return false;
	}

	static async getCredentials(): Promise<Credentials | null> {
		const config = await JobMasterConfigUtil.loadConfig();

		if (!config) return null;

		if (this.credentials) return { ...this.credentials };

		if (config.authRetentionMode === 'client') {
			const serialized = sessionStorage.getItem('jm_credentials');
			if (serialized) {
				this.credentials = JSON.parse(serialized) as Credentials;
				return { ...this.credentials };
			}

			return null;
		}

		if (config.authRetentionMode === 'server') {
			const basePath = JobMasterConfigUtil.getBasePath();
			try {
				const res = await fetch(`${basePath}/credentials/jm_credentials`);
				if (res.ok) {
					const data = await res.json();
					const serialized = data.secrets?.credentials;
					if (serialized) {
						this.credentials = JSON.parse(serialized) as Credentials;
						return { ...this.credentials };
					}
				}
			} catch (err) {
				console.error('Error retrieving credentials from server:', err);
			}
		}

		return null;
	}

	static clear(): void {
		this.credentials = null;
		sessionStorage.removeItem('jm_credentials');
		sessionStorage.removeItem('jm_session_opened');

		JobMasterConfigUtil.loadConfig().then(config => {
			if (config && config.authRetentionMode === 'server') {
				const basePath = JobMasterConfigUtil.getBasePath();
				fetch(`${basePath}/credentials/close-session`, {
					method: 'DELETE'
				}).catch(err => {
					console.error('Error closing credentials session on server:', err);
				});
			}
		}).catch(err => {
			console.error('Error loading config during clear:', err);
		});
	}
}