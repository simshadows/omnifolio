/*
 * Filename: readfiles.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Utilities to read the data files.
 *
 * NOTE: There's a lot of "Not allowed yet." here. I plan on coming around and
 * fixing those later.
 */

import {open, readdir} from "node:fs/promises";
import {join} from 'node:path';

import {
    type Account,
    type TimeseriesEntry,
    type MarketEvent,
    type SingleAssetMarketData,
    type OmnifolioBundle,
} from "omnifolio-core";

import {jsonParse, isObjArray} from "./danger";

const ENCODING = "utf8";


function csvParse(raw: string): string[][] {
    const lines = raw.trim().split("\n");
    return lines.map(s => s.trim().split(",").map(s2 => s2.trim()));
}


/*** ***/


async function readAccounts(path: string): Promise<Map<string, Account>> {
    path;
    // TODO
    return new Map();
}


/*** ***/


function parseTimeseries(raw: string): TimeseriesEntry[] {
    const unsorted: TimeseriesEntry[] = csvParse(raw).map(line => {
        const date = line[0];
        if (!date) {
            throw new Error("Blank date is not allowed.");
        }

        const valueStr = line[1];
        if (!valueStr) {
            throw new Error("Blank date is not allowed.");
        }
        const value = Number(valueStr);

        return {
            date,
            value,
        };
    });

    //const sorted = unsorted.sort((a, b) => a.date.localeCompare(b.date));
    //return sorted;
    return unsorted;
}


async function readMarketEvents(path: string): Promise<MarketEvent[]> {
    const raw = await (async ()=>{
        try {
            const f = await open(path, "r");
            return await f.readFile({encoding: ENCODING});
        } catch {
            return null;
        }
    })();
    if (!raw) return []; // No events

    const obj: unknown = jsonParse(raw);
    if (!isObjArray(obj)) {
        throw new Error(`${path} root must be an array.`);
    }

    return obj.map(event => {
        if (!("date" in event && typeof event.date === "string")) {
            throw new Error(`${path} date must be a string.`);
        }
        const date = event.date;
        return {
            date,
        };
    });
}


async function readMarketData(root: string): Promise<SingleAssetMarketData> {
    const path1 = join(root, "config.json");
    const f1 = await open(path1, "r");
    const raw1: string = await f1.readFile({encoding: ENCODING});
    const obj1: unknown = jsonParse(raw1);
    if (!obj1 || typeof obj1 !== "object" || Array.isArray(obj1)) {
        throw new Error(`${path1} root must be an object.`);
    }

    if (!("id" in obj1 && typeof obj1.id === "string")) {
        throw new Error(`${path1} id must be a string.`);
    }
    const id = obj1.id;

    const path2 = join(root, "timeseries.csv");
    const f2 = await open(path2, "r");
    const raw2: string = await f2.readFile({encoding: ENCODING});

    const timeseriesDaily = parseTimeseries(raw2);

    return {
        id,
        timeseriesDaily,
        events: await readMarketEvents(join(root, "events.json")),
        metadata: {
            directory: root,
        },
    };
}

async function readMarketDataRoot(root: string): Promise<Map<string, SingleAssetMarketData>> {
    const m: Map<string, SingleAssetMarketData> = new Map();

    const dirContents = await readdir(root, {withFileTypes: true});
    for (const dirent of dirContents) {
        if (!dirent.isDirectory()) {
            throw new Error("Not allowed yet.");
            //continue;
        }
        const path = join(root, dirent.name);
        const obj = await readMarketData(path);
        if (m.has(obj.id)) {
            throw new Error(`Duplicate ID ${obj.id} at ${path}`);
        }
        m.set(obj.id, obj);
    }

    return m;
}


/*** ***/


async function readOmnifolio(path: string): Promise<void> {
    const f = await open(path, "r");
    const raw: string = await f.readFile({encoding: ENCODING});

    const obj: unknown = jsonParse(raw);

    if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
        throw new Error("omnifolio.json root must be an object.");
    }

    // This is just an initial sanity check to make sure we're reading
    // an Omnifolio data directory.
    if (!("omnifolioVersion" in obj)) {
        throw new Error("omnifolio.json must have 'omnifolioVersion'.");
    }
    if (obj.omnifolioVersion !== -1) {
        throw new Error("omnifolio.json is invalid.");
    }
}

export async function readFiles(root: string): Promise<OmnifolioBundle> {
    await readOmnifolio(join(root, "omnifolio.json"));
    return {
        accounts: await readAccounts(join(root, "accounts")),
        marketData: await readMarketDataRoot(join(root, "market-data")),
    }
}

