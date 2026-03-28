const path = require("node:path");

const commonApp = {
	env_file: path.join(__dirname, ".env"),
	autorestart: true,
	stop_exit_codes: [0],
	min_uptime: "10s",
	max_restarts: 50,
	restart_delay: 5000,
	exp_backoff_restart_delay: 100,
	watch: false,
};

module.exports = {
	apps: [
		{
			...commonApp,
			name: "api-server",
			script: "bun",
			args: "src/services/server.ts",
			cwd: __dirname,
		},
		{
			...commonApp,
			name: "markets",
			script: "bun",
			args: "src/services/markets.ts",
			cwd: __dirname,
		},
		{
			...commonApp,
			name: "pipeline",
			script: "bun",
			args: "src/main.ts",
			cwd: __dirname,
		},
		// Frontend dev server - runs on port 4033 and proxies API to port 4069
		// For production, frontend is built and served from public/dist via API server or R2
		{
			...commonApp,
			name: "frontend",
			script: "bun",
			args: "run dev",
			cwd: path.join(__dirname, "frontend"),
			env: {
				FRONTEND_HOST: "127.0.0.1",
				FRONTEND_PORT: "4033",
				API_UPSTREAM_BASE_URL: "http://127.0.0.1:4069",
			},
		},
		{
			...commonApp,
			name: "cloudflared",
			script: path.join(__dirname, "..", "tunnel", "start.sh"),
			interpreter: "bash",
			cwd: __dirname,
		},
	],
};
