/*
 * Filename: public-types.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * The core types of omnifolio-core
 */

interface BaseTransaction {
    // TODO we should probably change this to a proper type
    date: string;

    // TODO: How to differentiate between transactions?
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
    // TODO we should probably change this to a proper type
    date: string;

    // TODO change to a wrapper class?
    value: number;
};

interface BaseMarketEvent {
    // TODO we should probably change this to a proper type
    date: string;

    // TODO: How to differentiate between events?
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
