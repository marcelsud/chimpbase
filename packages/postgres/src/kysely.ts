import {
  type DatabaseIntrospector,
  type Dialect,
  type Driver,
  Kysely,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
} from "kysely";
import { ChimpbaseKyselyDriver, type ChimpbaseKyselyExecutor } from "@chimpbase/core";

class ChimpbasePostgresDialect implements Dialect {
  constructor(private readonly executor: ChimpbaseKyselyExecutor) {}

  createAdapter() {
    return new PostgresAdapter();
  }

  createDriver(): Driver {
    return new ChimpbaseKyselyDriver(this.executor);
  }

  createIntrospector(db: Kysely<Record<string, never>>): DatabaseIntrospector {
    return new PostgresIntrospector(db);
  }

  createQueryCompiler() {
    return new PostgresQueryCompiler();
  }
}

export function createPostgresKysely<TDatabase>(
  executor: ChimpbaseKyselyExecutor,
): Kysely<TDatabase> {
  return new Kysely<TDatabase>({
    dialect: new ChimpbasePostgresDialect(executor),
  });
}
