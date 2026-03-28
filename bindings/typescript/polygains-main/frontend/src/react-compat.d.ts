declare module "react" {
	export * from "preact/compat";
	export * from "preact/hooks";
	export { Fragment } from "preact";
	import ReactDefault from "preact/compat";
	export default ReactDefault;
}

declare module "react-dom/client" {
	export { createRoot, hydrateRoot } from "preact/compat/client";
}

declare module "react/jsx-runtime" {
	export * from "preact/jsx-runtime";
}
