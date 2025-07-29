/*
 * Filename: types.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * The core types of omnifolio-core
 */

export interface TimeseriesEntry {
    // TODO we should probably change this to a proper type
    date: string;

    // TODO change to a wrapper class?
    value: number;
};

export interface SingleAssetMarketData {
    // Omnifolio will validate this for expected format.
    id: string;

    // The timeseries doesn't need to be in sorted order.
    // Omnifolio will sort it itself.
    // Omnifolio will also validate for duplicates.
    timeseriesDaily: TimeseriesEntry[];

    // Some optional metadata that can help with debugging
    metadata?: {
        directory?: string;
    }
};

export interface OmnifolioBundle {
    marketData: Map<string, SingleAssetMarketData>;
};
