/*
 * Filename: timezoneless-date.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * A wrapper class to enforce the use of a pre-existing date implementation
 * as a simple timezoneless calendar date.
 */

import dayjs, {Dayjs} from "dayjs";
//import utc from "dayjs/plugin/utc";

//dayjs.extend(utc);

const iso8601DateRegex = /^[0-9]{4}-[01][0-9]-[0-3][0-9]$/;

export class TimezonelessDate {
    d: Dayjs;

    /*
     * y is the calendar year
     * m is an integer 0-11
     * d is an integer 1-31
     */
    constructor(y: number, m: number, d: number) {
        this.d = dayjs(0).year(y).month(m).date(d);
    }

    toString(): string {
        return this.toISOString();
    }
    toISOString(): string {
        return this.d.format("YYYY-MM-DD");
    }
    toDebugString(): string {
        return this.d.toISOString();
    }

    static parseISODate(s: string): TimezonelessDate {
        if (s.length !== 10) {
            throw new Error(`Failed to parse string as a date. Must be exactly 10 characters. Instead got: ${s}`);
        }
        if (!iso8601DateRegex.test(s)) {
            throw new Error(`Failed to parse string as a date. String must be an ISO8601 date. Instead got: ${s}`);
        }
        const subStrs = s.split("-");
        const y = subStrs[0];
        const m = subStrs[1];
        const d = subStrs[2];
        if ((subStrs.length !== 3) || (!y) || (!m) || (!d)) {
            throw new Error("Expected three integers.");
        }
        return new TimezonelessDate(Number(y), Number(m) - 1, Number(d));
    }
}

