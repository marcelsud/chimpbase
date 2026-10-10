import { defineConfig } from "vitepress";

export default defineConfig({
  title: "Chimpbase",
  description: "Build backends with fewer moving parts.",
  themeConfig: {
    nav: [
      { text: "Guide", link: "/getting-started", activeMatch: "^/(?!advanced/)" },
      { text: "Advanced", link: "/advanced/", activeMatch: "^/advanced/" },
      { text: "llms.txt", link: "/llms.txt", target: "_blank", rel: "noopener" },
    ],
    sidebar: {
      "/advanced/": [
        {
          text: "Advanced guides",
          items: [
            { text: "Overview", link: "/advanced/" },
            { text: "App Composition", link: "/advanced/app-composition" },
            { text: "Workflows", link: "/advanced/workflows" },
            { text: "Business Modules", link: "/advanced/modules" },
          ],
        },
        {
          text: "Storage and observability",
          collapsed: true,
          items: [
            { text: "State & Storage", link: "/advanced/state" },
            { text: "Custom Adapters", link: "/advanced/storage-adapters" },
            { text: "KV Store", link: "/advanced/kv" },
            { text: "Blobs", link: "/advanced/blobs" },
            { text: "Streams", link: "/advanced/streams" },
            { text: "Telemetry", link: "/advanced/telemetry" },
          ],
        },
        {
          text: "Framework integrations",
          collapsed: true,
          items: [
            { text: "Hono", link: "/advanced/hono" },
            { text: "NestJS", link: "/advanced/nestjs" },
            { text: "Express", link: "/advanced/express" },
            { text: "Next.js", link: "/advanced/nextjs" },
          ],
        },
        {
          text: "Plugins",
          collapsed: true,
          items: [
            { text: "Custom Plugins", link: "/advanced/plugins" },
            { text: "Event Delivery", link: "/advanced/event-bus" },
            { text: "Auth", link: "/advanced/auth" },
            { text: "Webhooks", link: "/advanced/webhooks" },
            { text: "REST Collections", link: "/advanced/rest-collections" },
            { text: "Mesh", link: "/advanced/mesh" },
            { text: "Contract Testing (Pact)", link: "/advanced/pact" },
          ],
        },
        {
          text: "Deployment",
          collapsed: true,
          items: [
            { text: "Self-hosting", link: "/advanced/deployment" },
            { text: "Chimpbase Cloud", link: "/advanced/cloud" },
            { text: "Cloud CLI (chimpctl)", link: "/advanced/cli" },
          ],
        },
      ],
      "/": [
        {
          text: "Start",
          items: [
            { text: "Introduction", link: "/" },
            { text: "Getting Started", link: "/getting-started" },
          ],
        },
        {
          text: "Build",
          items: [
            { text: "Actions", link: "/actions" },
            { text: "HTTP Routes", link: "/routes" },
            { text: "Collections", link: "/collections" },
            { text: "Database", link: "/database" },
            { text: "Subscriptions", link: "/subscriptions" },
            { text: "Workers & Queues", link: "/workers" },
            { text: "Cron", link: "/cron" },
          ],
        },
        {
          text: "Reference",
          items: [
            { text: "Context", link: "/context" },
            { text: "Configuration", link: "/configuration" },
          ],
        },
        {
          text: "Background",
          collapsed: true,
          items: [
            { text: "Why PostgreSQL", link: "/why-postgres" },
            { text: "Why Primitives", link: "/why-primitives" },
          ],
        },
        { text: "Advanced guides", link: "/advanced/" },
      ],
    },
    socialLinks: [
      { icon: "github", link: "https://github.com/chimpbase/chimpbase" },
    ],
  },
});
