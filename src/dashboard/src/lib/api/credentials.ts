export type Credentials = {
	type: "API_KEY" | "JWT_SIMPLE" | "JWT_CUSTOM_FORM" | "OAUTH" | "USER_PASSWORD";
	secretValue?: string;
	userName?: string;
	userPassword?: string;
};
