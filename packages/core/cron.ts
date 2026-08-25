const CRON_FIELD_COUNT = 5;
const CRON_LOOKAHEAD_YEARS = 8;

interface ParsedCronField {
  valueSet: ReadonlySet<number>;
  values: readonly number[];
  wildcard: boolean;
}

interface ParsedCronExpression {
  dayOfMonth: ParsedCronField;
  dayOfWeek: ParsedCronField;
  hour: ParsedCronField;
  minute: ParsedCronField;
  month: ParsedCronField;
}

const parsedCronCache = new Map<string, ParsedCronExpression>();

export function computeNextCronFireTime(expression: string, afterMs: number): number {
  const parsed = parseCronExpression(expression);
  const start = new Date(afterMs);
  start.setUTCSeconds(0, 0);
  start.setUTCMinutes(start.getUTCMinutes() + 1);

  const startYear = start.getUTCFullYear();
  const startMonth = start.getUTCMonth() + 1;
  const startDay = start.getUTCDate();
  const startHour = start.getUTCHours();
  const startMinute = start.getUTCMinutes();

  for (let year = startYear; year <= startYear + CRON_LOOKAHEAD_YEARS; year += 1) {
    for (const month of parsed.month.values) {
      if (year === startYear && month < startMonth) {
        continue;
      }

      const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
      const firstDay = year === startYear && month === startMonth ? startDay : 1;

      for (let day = firstDay; day <= daysInMonth; day += 1) {
        const candidateDate = new Date(Date.UTC(year, month - 1, day));
        if (!matchesDay(parsed, candidateDate)) {
          continue;
        }

        const sameDay = year === startYear && month === startMonth && day === startDay;

        for (const hour of parsed.hour.values) {
          if (sameDay && hour < startHour) {
            continue;
          }

          const sameHour = sameDay && hour === startHour;

          for (const minute of parsed.minute.values) {
            if (sameHour && minute < startMinute) {
              continue;
            }

            return Date.UTC(year, month - 1, day, hour, minute, 0, 0);
          }
        }
      }
    }
  }

  throw new Error(`cron expression did not produce a future fire time: ${expression}`);
}

function parseCronExpression(expression: string): ParsedCronExpression {
  const normalized = expression.trim().replace(/\s+/g, " ");
  const cached = parsedCronCache.get(normalized);
  if ((cached !== undefined)) {
    return cached;
  }

  const parts = normalized.split(" ");
  if (parts.length !== CRON_FIELD_COUNT) {
    throw new Error(`cron expression requires 5 fields: ${expression}`);
  }

  const parsed = {
    minute: parseCronField(parts[0], 0, 59),
    hour: parseCronField(parts[1], 0, 23),
    dayOfMonth: parseCronField(parts[2], 1, 31),
    month: parseCronField(parts[3], 1, 12),
    dayOfWeek: parseCronField(parts[4], 0, 6, {
      maxInputValue: 7,
      normalizeValue: (value) => value === 7 ? 0 : value,
    }),
  } satisfies ParsedCronExpression;

  parsedCronCache.set(normalized, parsed);
  return parsed;
}

function parseCronField(
  expression: string,
  min: number,
  max: number,
  options: {
    maxInputValue?: number;
    normalizeValue?: (value: number) => number;
  } = {},
): ParsedCronField {
  const tokens = expression.split(",");
  const values = new Set<number>();
  let sawWildcardToken = false;

  for (const rawToken of tokens) {
    const token = rawToken.trim();
    if (token.length === 0) {
      throw new Error(`invalid cron field: ${expression}`);
    }

    const segments = token.split("/");
    if (segments.length > 2) {
      throw new Error(`invalid cron token: ${token}`);
    }

    const base = segments[0] ?? "";
    const step = segments[1] === undefined ? 1 : parsePositiveInteger(segments[1], token);

    if (base.includes("*")) {
      sawWildcardToken = true;
    }

    const [start, end] = parseCronRange(base, min, options.maxInputValue ?? max, token);
    for (let value = start; value <= end; value += step) {
      values.add(normalizeCronValue(value, min, max, options.maxInputValue ?? max, options.normalizeValue, token));
    }
  }

  if (values.size === 0) {
    throw new Error(`cron field has no values: ${expression}`);
  }

  return {
    valueSet: values,
    values: [...values].sort((left, right) => left - right),
    wildcard: sawWildcardToken && values.size === (max - min + 1),
  };
}

function parseCronRange(
  base: string,
  min: number,
  maxInputValue: number,
  token: string,
): readonly [number, number] {
  if (base === "*") {
    return [min, maxInputValue];
  }

  if (base.includes("-")) {
    const [startValue, endValue] = base.split("-");
    const start = parseRawCronValue(startValue ?? "", min, maxInputValue, token);
    const end = parseRawCronValue(endValue ?? "", min, maxInputValue, token);
    if (start > end) {
      throw new Error(`cron range must be ascending: ${token}`);
    }

    return [start, end];
  }

  const value = parseRawCronValue(base, min, maxInputValue, token);
  return [value, value];
}

function parseRawCronValue(
  rawValue: string,
  min: number,
  maxInputValue: number,
  token: string,
): number {
  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed)) {
    throw new Error(`cron token is not an integer: ${token}`);
  }

  if (parsed < min || parsed > maxInputValue) {
    throw new Error(`cron value out of range (${min}-${maxInputValue}): ${token}`);
  }

  return parsed;
}

function normalizeCronValue(
  value: number,
  min: number,
  max: number,
  maxInputValue: number,
  normalizeValue: ((value: number) => number) | undefined,
  token: string,
): number {
  if (value < min || value > maxInputValue) {
    throw new Error(`cron value out of range (${min}-${maxInputValue}): ${token}`);
  }

  const normalized = (normalizeValue !== undefined) ? normalizeValue(value) : value;
  if (normalized < min || normalized > max) {
    throw new Error(`cron value out of range (${min}-${max}): ${token}`);
  }

  return normalized;
}

function parsePositiveInteger(rawValue: string, token: string): number {
  const parsed = Number(rawValue);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`cron step must be a positive integer: ${token}`);
  }

  return parsed;
}

function matchesDay(parsed: ParsedCronExpression, date: Date): boolean {
  const dayOfMonthMatches = parsed.dayOfMonth.valueSet.has(date.getUTCDate());
  const dayOfWeekMatches = parsed.dayOfWeek.valueSet.has(date.getUTCDay());

  if (parsed.dayOfMonth.wildcard && parsed.dayOfWeek.wildcard) {
    return true;
  }

  if (parsed.dayOfMonth.wildcard) {
    return dayOfWeekMatches;
  }

  if (parsed.dayOfWeek.wildcard) {
    return dayOfMonthMatches;
  }

  return dayOfMonthMatches || dayOfWeekMatches;
}
