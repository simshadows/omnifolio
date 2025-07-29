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

