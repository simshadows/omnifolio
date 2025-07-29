# Omnifolio Docs

In Omnifolio's current state, all input is done by a structured set of files.

The input structure is documented using `*.md` files within the sample data directory. All `*.md` files will be ignored by Omnifolio, so you can add your own free-form documentation as well.

Top-level structure:

## `/omnifolio.json`

Currently only serves to ensure Omnifolio is pointed to the correct filepath.

*In the future, this file will probably contain basic configuration.*

(There's currently some junk data in the file. This is just for sanity checking for now.)

## `/accounts/`

This is a tree of accounts. The directory structure defines the tree structure.

Currently, only the leaves of the tree matter where each leaf is an account. *(In the future, I hope to make the tree structure useful.)*

All accounts should be thought of as collections of assets. Assets can be currencies (you can make a "savings account" as an account with only one currency asset), stocks/ETFs, cryptocurrencies, or anything else that can be represented in units. *(In the future, I might implement liabilities and complex financial tools such as margin accounts.)*

However, you don't manipulate this collection directly. It is manipulated through transactions.

## `/market-data/`

Each directory represents an asset, and contains daily price (timeseries) data, event data (mostly dividends/distributions), and other information such as what currency the asset is valued in.

