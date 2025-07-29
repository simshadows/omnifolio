/*
 * Filename: readfiles.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Utilities to read the data files.
 */

import {open, readdir} from "node:fs/promises";
import {join} from 'node:path';

import {jsonParse} from "./danger";

const ENCODING = "utf8";

export async function readAccounts(path: string): Promise<void> {
    path;
    // TODO
}

export async function readMarketData(path: string): Promise<void> {
    console.log();
    console.log(await readdir(path, {withFileTypes: true}));

    // TODO
}

export async function readOmnifolio(path: string): Promise<void> {
    const f = await open(path, "r");
    const raw: string = await f.readFile({encoding: ENCODING});

    const obj: unknown = jsonParse(raw);

    if (!obj || typeof obj !== "object") {
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

export async function readFiles(root: string) {
    await readOmnifolio(join(root, "omnifolio.json"));
    await readAccounts(join(root, "accounts"));
    await readMarketData(join(root, "market-data"));
}

