/*
 * Filename: general.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Uncategorized utilities.
 */

import {open} from "node:fs/promises";

import {
    jsonParse,
    isUnknownArray,
} from "./danger";

const ENCODING = "utf8";

export function csvParse(raw: string): string[][] {
    const lines = raw.trim().split("\n");
    return lines.map(s => s.trim().split(",").map(s2 => s2.trim()));
}

/*** ***/

export async function readTextFile(path: string): Promise<string> {
    let f;
    try {
        f = await open(path, "r");
        return await f.readFile({encoding: ENCODING});
    } finally {
        await f?.close();
    }
}

export async function readCsvFile(path: string): Promise<string[][]> {
    const raw = await readTextFile(path);
    return csvParse(raw);
}

/*
 * Throws an error if the JSON top-level is an array.
 */
export async function readJsonObjectFile(path: string): Promise<object> {
    const raw = await readTextFile(path);
    const obj: unknown = jsonParse(raw);
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
        throw new Error(`JSON file top-level must be an object. Path: ${path}`);
    }
    return obj;
}

export async function readJsonArrayFile(path: string): Promise<unknown[]> {
    const raw = await readTextFile(path);
    const arr: unknown = jsonParse(raw);
    if (!isUnknownArray(arr)) {
        throw new Error(`JSON file top-level must be an array. Path: ${path}`);
    }
    return arr;
}

