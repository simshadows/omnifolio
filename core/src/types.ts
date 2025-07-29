/*
 * Filename: types.ts
 * Author:   simshadows <contact@simshadows.com>
 *
 * The core types of omnifolio-core
 */

export interface SingleAssetMarketData {
    id: string;
    timeseriesDaily: Map<string, number>;

    // Some optional metadata that can help with debugging
    metadata?: {
        directory?: string;
    }
}

export interface OmnifolioBundle {
    marketData: Map<string, SingleAssetMarketData>;
}
