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
    type OmnifolioBundle,
    type SingleAssetMarketData,
} from "omnifolio-core";

import {jsonParse} from "./danger";

const ENCODING = "utf8";

export async function readAccounts(path: string): Promise<void> {
    path;
    // TODO
}


/*** ***/


export async function readMarketData(root: string): Promise<SingleAssetMarketData> {
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

    return {
        id: id,
        timeseriesDaily: new Map(),
        metadata: {
            directory: root,
        },
    };
}

export async function readMarketDataRoot(root: string): Promise<Map<string, SingleAssetMarketData>> {
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


export async function readOmnifolio(path: string): Promise<void> {
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
    await readAccounts(join(root, "accounts"));
    return {
        marketData: await readMarketDataRoot(join(root, "market-data")),
    }
}

