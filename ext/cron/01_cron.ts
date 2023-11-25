// Copyright 2018-2023 the Deno authors. All rights reserved. MIT license.

// @ts-ignore internal api
const core = Deno.core;

type DayOfWeek = 0 | 1 | 2 | 3 | 4 | 5 | 6;

interface Schedule {
  minute?: number | { start: number; every: number };
  hour?: number | { start: number; every: number };
  day_of_month?: number | { start: number; every: number };
  month?: number | { start: number; every: number };
  day_of_week?: DayOfWeek[];
}

function isNotValidJsonSchedule(schedule: string | Schedule): boolean {
  return (
    (typeof schedule !== "string") && (
      (typeof schedule.minute !== "undefined" &&
        typeof schedule.minute !== "number" &&
        (typeof schedule.minute !== "object" ||
          typeof schedule.minute.start !== "number" ||
          typeof schedule.minute.every !== "number")) ||
      (typeof schedule.hour !== "undefined" &&
        typeof schedule.hour !== "number" &&
        (typeof schedule.hour !== "object" ||
          typeof schedule.hour.start !== "number" ||
          typeof schedule.hour.every !== "number")) ||
      (typeof schedule.day_of_month !== "undefined" &&
        typeof schedule.day_of_month !== "number" &&
        (typeof schedule.day_of_month !== "object" ||
          typeof schedule.day_of_month.start !== "number" ||
          typeof schedule.day_of_month.every !== "number")) ||
      (typeof schedule.month !== "undefined" &&
        typeof schedule.month !== "number" &&
        (typeof schedule.month !== "object" ||
          typeof schedule.month.start !== "number" ||
          typeof schedule.month.every !== "number")) ||
      (typeof schedule.day_of_week !== "undefined" &&
        !Array.isArray(schedule.day_of_week))
    )
  );
}

function formateToCronSchedule(
  value?: number | { start: number; every: number } | DayOfWeek[],
): string {
  if (value === undefined) {
    return "*";
  } else if (typeof value === "number") {
    return value.toString();
  } else if (Array.isArray(value)) {
    return value.join(",");
  } else {
    const { start, every } = value;
    return start + "/" + every;
  }
}

function convertScheduleToString(schedule: string | Schedule): string {
  if (typeof schedule === "string") {
    return schedule;
  } else {
    let {
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
  if (isNotValidJsonSchedule(schedule)) {
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
