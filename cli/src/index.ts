import {argv} from 'node:process';

import {readBundle} from "omnifolio-core";

import {readFiles} from "./readfiles";


async function run() {
    console.log("argv:");
    console.log(argv);
    console.log();

    const argv2 = argv[2];
    if (typeof argv2 !== "string" || !argv2) {
        throw new Error("Expected non-empty root path string.");
    }
    const rootPath = argv2.trim();
    if (!rootPath) {
        throw new Error("Expected non-empty root path string.");
    }

    readBundle(await readFiles(rootPath));
}

run();
