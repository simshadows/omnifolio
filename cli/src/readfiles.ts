/*
 * Filename: readfiles.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * NOTE: There's a lot of "Not allowed yet." here. I plan on coming around and
 * fixing those later.
 */

import {readdir} from "node:fs/promises";
import {join} from 'node:path';

import {
    type Account,
    type TimeseriesEntry,
    type MarketEvent,
    type SingleAssetMarketData,
    type OmnifolioBundle,
} from "omnifolio-core";

import {
    isObjArray,
    readCsvFile,
    readJsonObjectFile,
    readJsonArrayFile,

    objGetStr,
} from "omnifolio-utils";


/*
 * This function intentionally does not fail gracefully if dirContents
 * contains a file, but actually opening the file fails.
 */
async function tryReadAccountNode(path: string): Promise<Account | null> {
    const dirContents = new Map(
        (await readdir(path, {withFileTypes: true}))
            .map(x => [x.name, x])
    );

    const NAME1 = "config.json";
    if (!dirContents.has(NAME1)) return null;
    const path1 = join(path, NAME1);
    const obj1 = await readJsonObjectFile(path1);

    const id = objGetStr(obj1, "id", path1);
    const name = objGetStr(obj1, "name", path1);

    const transactions = await (async ()=>{
        const NAME2 = "transactions.json";
        if (!dirContents.has(NAME2)) return [];
        const path2 = join(path, NAME2);
        const arr2 = await readJsonArrayFile(path2);

        return arr2.map(t => {
            if (!t || typeof t !== "object" || Array.isArray(t)) {
                throw new Error(`${path2} array elements must be an objects.`);
            }

            const date = objGetStr(t, "date", path2);
            return {
                date,
            };
        });
    })();

    return {
        id,
        name,
        transactions: transactions,
    };
}

async function readAccounts(path: string): Promise<Map<string, Account>> {
    const m: Map<string, Account> = new Map();

    const nodeAccount = await tryReadAccountNode(path);
    if (nodeAccount) {
        m.set(nodeAccount.id, nodeAccount);
    }

    const dirContents = await readdir(path, {withFileTypes: true});
    for (const dirent of dirContents) {
        if (!dirent.isDirectory()) continue;
        const path2 = join(path, dirent.name);
        const moreAccounts = await readAccounts(path2);
        for (const [id, account] of moreAccounts) {
            if (m.has(id)) {
                throw new Error(`Duplicate account ID '${id}' at ${path2}`);
            }
            m.set(id, account);
        }
    }
    return m;
}


/*** ***/


async function readTimeseriesFile(path: string): Promise<TimeseriesEntry[]> {
    const csvData = await readCsvFile(path);

    const unsorted: TimeseriesEntry[] = csvData.map(line => {
        const date = line[0];
        if (!date) {
            throw new Error(`Blank date (column 1) is not allowed. ${path}`);
        }

        const valueStr = line[1];
        if (!valueStr) {
            throw new Error(`Blank value (column 2) is not allowed. ${path}`);
        }
        const value = Number(valueStr);

        return {
            date,
            value,
        };
    });

    // TODO: Throw an error if it's out of order.

    //const sorted = unsorted.sort((a, b) => a.date.localeCompare(b.date));
    //return sorted;
    return unsorted;
}


async function readMarketEvents(path: string): Promise<MarketEvent[]> {
    const obj: unknown = await (async ()=>{
        try {
            return await readJsonObjectFile(path);
        } catch {
            return null;
        }
    })();
    if (!obj) return []; // No events

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
    const obj1 = await readJsonObjectFile(path1);

    if (!("id" in obj1 && typeof obj1.id === "string")) {
        throw new Error(`${path1} id must be a string.`);
    }
    const id = obj1.id;

    const path2 = join(root, "timeseries.csv");
    const timeseriesDaily = await readTimeseriesFile(path2);

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
    const obj = await readJsonObjectFile(path);

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

