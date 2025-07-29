/*
 * Filename: danger.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Spooky stuff like type checker overrides are defined here. We should aim
 * to remove these whenever possible.
 */


/*
 * A wrapper to cast the returned object in an `unknown`, and the
 * inverse operation to match.
 */
export const jsonParse = (s: string): unknown => JSON.parse(s);
export const jsonStringify = (x: unknown): string => JSON.stringify(x);

/*
 * Workaround for TypeScript's current limitations around narrowing an
 * unknown into an array of a specific type.
 *
 * Still an open issue as of writing:
 * <https://github.com/microsoft/TypeScript/issues/17002>
 */
//export function isUnknownArray(obj: unknown): obj is unknown[] {
//    return Array.isArray(obj);
//}
export function isObjArray(obj: unknown): obj is object[] {
    if (!Array.isArray(obj)) return false;
    for (const v of obj) {
        if (typeof v !== "object") return false;
    }
    return true;
}

