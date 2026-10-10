# Cron

Cron jobs run on a recurring schedule. Schedules are durable — they survive process restarts and skip missed intervals instead of replaying backlog.

## Defining Cron Jobs

```ts
import { cron } from "chimpbase/runtime";

const dailyReport = cron(
  "reports.daily",
  "0 9 * * 1-5", // 9 AM weekdays
  async (ctx, invocation) => {
    const [summary] = await ctx.db.query("SELECT COUNT(*) AS total FROM orders WHERE DATE(created_at) = DATE('now')");

    ctx.log.info("daily report generated", {
      total: summary.total,
      schedule: invocation.name,
      fireAt: invocation.fireAt,
    });

    await ctx.collection.insert("daily_reports", {
      date: invocation.fireAt,
      total: summary.total,
    });
  },
);
```

## Handler Signature

```ts
(ctx: ChimpbaseContext, invocation: ChimpbaseCronInvocation) => TResult | Promise<TResult>
```

### Invocation Object

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | Cron job name |
| `schedule` | `string` | Cron expression |
| `fireAt` | `string` | ISO 8601 timestamp of this fire |
| `fireAtMs` | `number` | Milliseconds since epoch |

## Schedule Format

Standard 5-field cron expressions:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-7, 0 and 7 are Sunday)
│ │ │ │ │
* * * * *
```

**Examples:**

| Expression | Description |
|------------|-------------|
| `*/15 * * * *` | Every 15 minutes |
| `0 * * * *` | Every hour |
| `0 9 * * 1-5` | 9 AM weekdays |
| `0 0 1 * *` | Midnight on the 1st of each month |
| `30 2 * * *` | 2:30 AM daily |

## Missed Fires

After downtime, the runtime **resumes from the current slot** instead of replaying every missed interval. If a cron was supposed to fire at 10:00, 10:15, and 10:30, but the process was down from 10:05 to 10:25, it fires once at 10:30 — not three times.

## Telemetry

```ts
cron("cleanup.expired", "0 3 * * *", cleanupHandler, {
  telemetry: false, // suppress telemetry for noisy crons
});
```

## Registration

```ts
export default {
  registrations: [
    cron("reports.daily", "0 9 * * 1-5", generateDailyReport),
    cron("cleanup.expired", "0 3 * * *", cleanupExpiredRecords),
  ],
};
```
