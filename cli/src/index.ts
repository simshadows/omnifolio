import {argv} from 'node:process';

import {debug} from "omnifolio-core";

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

    console.log(`Root: ${rootPath}`);
    console.log();

    await readFiles(rootPath);

    debug();
    console.log("Exiting.");
}

run();
