FROM node:22-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates unzip \
  && curl -fsSL https://bun.sh/install | bash -s "bun-v1.3.14" \
  && ln -s /root/.bun/bin/bun /usr/local/bin/bun \
  && apt-get purge -y curl unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json bun.lock tsconfig.json ./
COPY packages ./packages
COPY examples ./examples

RUN bun install --frozen-lockfile

WORKDIR /app/examples/node/advanced

EXPOSE 3000
CMD ["node", "--import", "tsx/esm", "app.ts"]
