import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Served under /console behind the reverse proxy (Zitadel owns the apex root).
  // Asset URLs + import.meta.env.BASE_URL are emitted with this prefix; NPM
  // strips /console before forwarding so the container still serves at root.
  base: "/console/",
  server: { port: 8088 },
});
