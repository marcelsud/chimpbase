import { defineConfig } from "vitepress";

export default defineConfig({
  title: "Chimpbase",
  description: "Build complex backends with fewer moving parts.",
  themeConfig: {
    nav: [
      { text: "Guide", link: "/" },
      { text: "Primitives", link: "/actions" },
      { text: "Frameworks", link: "/hono" },
      { text: "Plugins", link: "/auth" },
      { text: "Cloud", link: "/cloud" },
      { text: "llms.txt", link: "/llms.txt", target: "_blank", rel: "noopener" },
    ],
    sidebar: [
      {
        text: "Why Chimpbase",
        items: [
          { text: "Just Use PostgreSQL", link: "/why-postgres" },
          { text: "Explicit Primitives", link: "/why-primitives" },
        ],
      },
      {
        text: "Getting Started",
        items: [
          { text: "Introduction", link: "/" },
          { text: "Getting Started", link: "/getting-started" },
          { text: "Configuration", link: "/configuration" },
        ],
      },
      {
        text: "Primitives",
        items: [
          { text: "Actions", link: "/actions" },
          { text: "Subscriptions", link: "/subscriptions" },
          { text: "Workers & Queues", link: "/workers" },
          { text: "Cron", link: "/cron" },
          { text: "Workflows", link: "/workflows" },
          { text: "Business Modules", link: "/modules" },
          { text: "HTTP Routes", link: "/routes" },
          { text: "Database", link: "/database" },
          { text: "Collections", link: "/collections" },
          { text: "KV Store", link: "/kv" },
          { text: "Blobs", link: "/blobs" },
          { text: "Streams", link: "/streams" },
          { text: "Telemetry", link: "/telemetry" },
          { text: "Plugins", link: "/plugins" },
        ],
      },
      {
        text: "Context",
        items: [
          { text: "Overview", link: "/context" },
          { text: "State & Storage", link: "/state" },
        ],
      },
      {
        text: "Frameworks",
        items: [
          { text: "Hono", link: "/hono" },
          { text: "NestJS", link: "/nestjs" },
          { text: "Express", link: "/express" },
          { text: "Next.js", link: "/nextjs" },
        ],
      },
      {
        text: "Scaling",
        items: [
          { text: "App Composition", link: "/app-composition" },
          { text: "Deployment", link: "/deployment" },
        ],
      },
      {
        text: "Official Plugins",
        items: [
          { text: "Auth", link: "/auth" },
          { text: "Webhooks", link: "/webhooks" },
          { text: "REST Collections", link: "/rest-collections" },
          { text: "Mesh", link: "/mesh" },
        ],
      },
      {
        text: "Testing",
        items: [
          { text: "Contract Testing (Pact)", link: "/pact" },
        ],
      },
      {
        text: "Chimpbase Cloud",
        items: [
          { text: "Overview", link: "/cloud" },
          { text: "CLI (chimpctl)", link: "/cli" },
        ],
      },
    ],
    socialLinks: [
      { icon: "github", link: "https://github.com/chimpbase/chimpbase" },
    ],
  },
});
