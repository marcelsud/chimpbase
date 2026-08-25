# NestJS

Chimpbase integrates with NestJS by wrapping your action logic in injectable services and wiring them into the Chimpbase registration system.

## Setup

```bash
bun add @nestjs/core @nestjs/common
```

## Define services

Wrap your action handlers in NestJS injectable services:

```ts
import { Injectable } from "@nestjs/common";
import { v, type ChimpbaseContext } from "@chimpbase/runtime";

@Injectable()
export class TodoService {
  async listTodos(ctx: ChimpbaseContext, filters: { projectId: number }) {
    return ctx.db.query("select * from todos where project_id = ?1", [filters.projectId]);
  }

  async createTodo(ctx: ChimpbaseContext, input: { title: string; projectId: number }) {
    const [todo] = await ctx.db.query(
      "insert into todos (title, project_id) values (?1, ?2) returning id",
      [input.title, input.projectId],
      v.object({ id: v.number() }),
    );
    return todo;
  }
}
```

## Create the NestJS module

```ts
import { Module } from "@nestjs/common";
import { TodoService } from "./todo.service.ts";

@Module({
  providers: [TodoService],
  exports: [TodoService],
})
export class TodoModule {}

@Module({
  imports: [TodoModule],
})
export class AppModule {}
```

## Wire into Chimpbase

Bootstrap NestJS, pull out your services, and register them as Chimpbase actions:

```ts
import { NestFactory } from "@nestjs/core";
import type { ChimpbaseAppDefinitionInput } from "@chimpbase/bun";
import { action } from "@chimpbase/runtime";
import { Hono } from "hono";
import type { ChimpbaseRouteEnv } from "@chimpbase/runtime";
import { AppModule } from "./src/nest/app.module.ts";
import { TodoService } from "./src/modules/todos/todo.service.ts";

const nestApp = await NestFactory.createApplicationContext(AppModule);
const todoService = nestApp.get(TodoService);

const app = new Hono<{ Bindings: ChimpbaseRouteEnv }>();

app.get("/todos", async (c) => {
  const todos = await c.env.action("listTodos", { projectId: 1 });
  return c.json(todos);
});

export default {
  project: { name: "my-app" },
  httpHandler: app.fetch,
  registrations: [
    action("listTodos", todoService.listTodos.bind(todoService)),
    action("createTodo", todoService.createTodo.bind(todoService)),
  ],
} satisfies ChimpbaseAppDefinitionInput;
```

## When to use NestJS

NestJS is useful when you need its dependency injection system for organising large codebases. For simpler projects, plain functions with `ChimpbaseAppDefinitionInput` are often enough.
