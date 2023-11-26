// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.

// @ts-ignore internal api
const core = Deno.core;

interface Schedule {
  minute?: number | { start: number; every: number };
  hour?: number | { start: number; every: number };
  day_of_month?: number | { start: number; every: number };
  month?: number | { start: number; every: number };
  day_of_week?: number[];
}

type ScheduleKeys = keyof Schedule;

function isValidSchedule(schedule: string | Schedule): boolean {
  if (typeof schedule === "string") {
    return true;
  }
  const validKeys: Set<ScheduleKeys> = new Set([
    "minute",
    "hour",
    "day_of_month",
    "month",
    "day_of_week",
  ]);

  for (const key in schedule) {
    if (!validKeys.has(key as ScheduleKeys)) {
      return false;
    }
  }

  const {
    minute,
    hour,
    day_of_month: dayOfMonth,
    month,
    day_of_week: dayOfWeek,
  } = schedule;

  return !(
    (typeof minute !== "undefined" &&
      typeof minute !== "number" &&
      (typeof minute !== "object" ||
        typeof minute.start !== "number" ||
        typeof minute.every !== "number")) ||
    (typeof hour !== "undefined" &&
      typeof hour !== "number" &&
      (typeof hour !== "object" ||
        typeof hour.start !== "number" ||
        typeof hour.every !== "number")) ||
    (typeof dayOfMonth !== "undefined" &&
      typeof dayOfMonth !== "number" &&
      (typeof dayOfMonth !== "object" ||
        typeof dayOfMonth.start !== "number" ||
        typeof dayOfMonth.every !== "number")) ||
    (typeof month !== "undefined" &&
      typeof month !== "number" &&
      (typeof month !== "object" ||
        typeof month.start !== "number" ||
        typeof month.every !== "number")) ||
    (typeof dayOfWeek !== "undefined" &&
      (Array.isArray(schedule.day_of_week) &&
        dayOfWeek.every((day) => typeof day === "number")))
  );
}

function formateToCronSchedule(
  value?: number | { start: number; every: number } | number[],
): string {
  if (value === undefined) {
    return "*";
  } else if (typeof value === "number") {
    return value.toString();
  } else if (Array.isArray(value)) {
    return value.join(",");
  } else {
    const { start, every } = value as { start: number; every: number };
    return start + "/" + every;
  }
}

function convertScheduleToString(schedule: string | Schedule): string {
  if (typeof schedule === "string") {
    return schedule;
  } else {
    const {
      minute,
      hour,
      day_of_month: dayOfMonth,
      month,
      day_of_week: dayOfWeek,
    } = schedule;
    return formateToCronSchedule(minute) +
      " " + formateToCronSchedule(hour) +
      " " + formateToCronSchedule(dayOfMonth) +
      " " + formateToCronSchedule(month) +
      " " + formateToCronSchedule(dayOfWeek);
  }
}

function cron(
  name: string,
  schedule: string | Schedule,
  handlerOrOptions1:
    | (() => Promise<void> | void)
    | ({ backoffSchedule?: number[]; signal?: AbortSignal }),
  handlerOrOptions2?:
    | (() => Promise<void> | void)
    | ({ backoffSchedule?: number[]; signal?: AbortSignal }),
) {
  if (name === undefined) {
    throw new TypeError("Deno.cron requires a unique name");
  }
  if (schedule === undefined) {
    throw new TypeError("Deno.cron requires a valid schedule");
  }
  if (!isValidSchedule(schedule)) {
    throw new TypeError("Invalid cron schedule");
  }

  schedule = convertScheduleToString(schedule);

  let handler: () => Promise<void> | void;
  let options: { backoffSchedule?: number[]; signal?: AbortSignal } | undefined;

  if (typeof handlerOrOptions1 === "function") {
    handler = handlerOrOptions1;
    if (typeof handlerOrOptions2 === "function") {
      throw new TypeError("options must be an object");
    }
    options = handlerOrOptions2;
  } else if (typeof handlerOrOptions2 === "function") {
    handler = handlerOrOptions2;
    options = handlerOrOptions1;
  } else {
    throw new TypeError("Deno.cron requires a handler");
  }

  const rid = core.ops.op_cron_create(
    name,
    schedule,
    options?.backoffSchedule,
  );

  if (options?.signal) {
    const signal = options?.signal;
    signal.addEventListener(
      "abort",
      () => {
        core.close(rid);
      },
      { once: true },
    );
  }

  return (async () => {
    let success = true;
    while (true) {
      const r = await core.opAsync("op_cron_next", rid, success);
      if (r === false) {
        break;
      }
      try {
        const result = handler();
        const _res = result instanceof Promise ? (await result) : result;
        success = true;
      } catch (error) {
        console.error(`Exception in cron handler ${name}`, error);
        success = false;
      }
    }
  })();
}

export { cron };
