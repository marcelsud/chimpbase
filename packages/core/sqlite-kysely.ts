import {
  type CompiledQuery,
  type DatabaseConnection,
  type DatabaseIntrospector,
  type Dialect,
  type Driver,
  Kysely,
  type TransactionSettings,
  type QueryResult,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from "kysely";

export interface ChimpbaseKyselyExecutor {
  executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>>;
  streamQuery<R>(compiledQuery: CompiledQuery, chunkSize: number): AsyncIterableIterator<QueryResult<R>>;
}

class ChimpbaseKyselyConnection implements DatabaseConnection {
  constructor(private readonly executor: ChimpbaseKyselyExecutor) {}

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
    return await this.executor.executeQuery<R>(compiledQuery);
  }

  async *streamQuery<R>(compiledQuery: CompiledQuery, chunkSize = 100): AsyncIterableIterator<QueryResult<R>> {
    if (!Number.isSafeInteger(chunkSize)) {
      throw new RangeError("Kysely stream chunk size must be a finite safe integer");
    }
    yield* this.executor.streamQuery<R>(compiledQuery, chunkSize <= 0 ? 100 : chunkSize);
  }
}

export class ChimpbaseKyselyDriver implements Driver {
  private readonly connection: DatabaseConnection;

  constructor(private readonly executor: ChimpbaseKyselyExecutor) {
    this.connection = new ChimpbaseKyselyConnection(executor);
  }

  async init(): Promise<void> {}

  async acquireConnection(): Promise<DatabaseConnection> {
    return this.connection;
  }

  async beginTransaction(_connection: DatabaseConnection, _settings: TransactionSettings): Promise<void> {
    throw new Error("ctx.db.kysely().transaction() is not supported; actions, subscriptions and queues already run inside runtime-managed transactions");
  }

  async commitTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error("ctx.db.kysely().transaction() is not supported; actions, subscriptions and queues already run inside runtime-managed transactions");
  }

  async rollbackTransaction(_connection: DatabaseConnection): Promise<void> {
    throw new Error("ctx.db.kysely().transaction() is not supported; actions, subscriptions and queues already run inside runtime-managed transactions");
  }

  async releaseConnection(_connection: DatabaseConnection): Promise<void> {}

  async destroy(): Promise<void> {}
}

class ChimpbaseSqliteDialect implements Dialect {
  constructor(private readonly executor: ChimpbaseKyselyExecutor) {}

  createAdapter() {
    return new SqliteAdapter();
  }

  createDriver(): Driver {
    return new ChimpbaseKyselyDriver(this.executor);
  }

  createIntrospector(db: Kysely<Record<string, never>>): DatabaseIntrospector {
    return new SqliteIntrospector(db);
  }

  createQueryCompiler() {
    return new SqliteQueryCompiler();
  }
}

export function createSqliteKysely<TDatabase>(
  executor: ChimpbaseKyselyExecutor,
): Kysely<TDatabase> {
  return new Kysely<TDatabase>({
    dialect: new ChimpbaseSqliteDialect(executor),
  });
}
