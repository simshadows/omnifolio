/*
 * Filename: danger-tech-debt.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Weird dangerous stuff that I've done just to get things to work.
 * I should try to fix these things eventually.
 */

export function getAttribute(obj: unknown, k: string): unknown {
    if (!obj || typeof obj !== "object") {
        throw new Error("Object is probably not actually an object.");
    }
    if (!(k in obj)) {
        throw new Error(`Failed to read object. Key '${k}' does not exist.`);
    }
    return (obj as {[_k: string]: unknown})[k];
}

