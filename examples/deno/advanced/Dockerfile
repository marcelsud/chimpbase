FROM denoland/deno:2.6.9

WORKDIR /app
COPY packages ./packages
COPY examples/shared ./examples/shared
COPY examples/deno/advanced ./examples/deno/advanced

WORKDIR /app/examples/deno/advanced
RUN deno cache --no-check \
  app.ts \
  tests/app/order-domain.denotest.ts \
  tests/e2e/runtime.e2e.denotest.ts \
  tests/e2e/workflow.denotest.ts

EXPOSE 3000
CMD ["deno", "run", "-A", "--no-check", "app.ts"]
