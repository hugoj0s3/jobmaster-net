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
			// TODO implement server side credentials
			console.log('store credentials for server');
			return true;
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
			// TODO implement server side recover.
			console.log('get credentials for server');
		}

		return null;
	}

	static clear(): void {
		this.credentials = null;
		sessionStorage.removeItem('jm_credentials');
	}
}