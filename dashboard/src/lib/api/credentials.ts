export type Credentials = {
	type: "API_KEY" | "JWT_SIMPLE" | "JWT_CUSTOM_FORM" | "JWT_SSO" | "USER_PASSWORD";
	secretValue?: string;
	userName?: string;
	userPassword?: string;
};
