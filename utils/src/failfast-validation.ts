/*
 * Filename: failfast-validation.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * Reading and validating unknown types with convenience functions that
 * simply throw exceptions if assumptions aren't being met.
 *
 * Convenience is the emphasis here. The functions are probably not performant.
 * This file is probably tech debt that we should improve later.
 */

import {getAttribute} from "./danger-tech-debt";

export function objGetStr(obj: unknown, k: string, errMsg: string = ""): string {
    errMsg = errMsg ? ` ${errMsg}` : "";

    const v = (()=>{
        try {
            return getAttribute(obj, k);
        } catch (e) {
            if (e instanceof Error) {
                throw new Error(`${e.message}${errMsg}`);
            } else {
                throw e;
            }
        }
    })();

    if (typeof v !== "string") {
        throw new Error(`Failed to read object. Key '${k}' must have a string value.${errMsg}`);
    }
    return v;
}

//export function objGetNumber(obj: unknown, k: string, errMsg: string = ""): number {
//    errMsg = errMsg ? ` ${errMsg}` : "";
//
//    const v = (()=>{
//        try {
//            return getAttribute(obj, k);
//        } catch (e) {
//            if (e instanceof Error) {
//                throw new Error(`${e.message}${errMsg}`);
//            } else {
//                throw e;
//            }
//        }
//    })();
//
//    if (typeof v !== "number") {
//        throw new Error(`Failed to read object. Key '${k}' must have a number value.${errMsg}`);
//    }
//    return v;
//}

