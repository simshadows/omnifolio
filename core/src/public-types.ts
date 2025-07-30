/*
 * Filename: public-types.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * The core types of omnifolio-core
 */

import {TimezonelessDate} from "omnifolio-utils";

interface BaseTransaction {
    date: TimezonelessDate;
}
interface BuySellTransaction extends BaseTransaction {
    asset: string;
    qty: number;
    pricePerUnit: number;
    brokerage: number;
}
export interface BuyTransaction extends BuySellTransaction {
    type: "buy";
}
export interface SellTransaction extends BuySellTransaction {
    type: "sell";
}
export type Transaction = BuyTransaction | SellTransaction;

export interface Account {
    id: string;
    name: string;
    transactions: Transaction[];
}

/*** ***/

export interface TimeseriesEntry {
    date: TimezonelessDate;

    // TODO change to a wrapper class?
    value: number;
};

interface BaseMarketEvent {
    date: TimezonelessDate;
}
export interface ExDistributionEvent extends BaseMarketEvent {
    type: "ex-distribution";
    valuePerUnit: number;
}
export type MarketEvent = ExDistributionEvent;

export interface SingleAssetMarketData {
    // Omnifolio will validate this for expected format.
    id: string;

    // The timeseries doesn't need to be in sorted order.
    // Omnifolio will sort it itself.
    // Omnifolio will also validate for duplicates.
    timeseriesDaily: TimeseriesEntry[];

    // Events don't need to be in sorted order.
    // Omnifolio will sort it itself.
    // Omnifolio will also validate for duplicates.
    events: MarketEvent[];

    // Some optional metadata that can help with debugging
    metadata?: {
        directory?: string;
    }
};

/*** ***/

export interface OmnifolioBundle {
    accounts: Map<string, Account>;
    marketData: Map<string, SingleAssetMarketData>;
};
