# Omnifolio

A personal investment tracking tool, implemented as a Jupyter template.

*This project is still very early in development.*

## Disclaimer

I am NOT an accounting or finance professional, nor have I ever received any formal training in these fields.

It must also be assumed that no part of this codebase has been reviewed by any accounting or finance professional. (Though, I do intend to have it be reviewed at some point.)

As such, I highly discourage anyone from using this tool for their own financial planning and tracking without sufficient independent research and understanding on relevant topics, and fully understanding the codebase.

By using this program, you agree that I, the developer of this codebase, and anyone else involved in developing and/or shaping this codebase cannot be held liable for any damages caused by the use of this codebase.

## Dependencies

```
pip install pandas
pip install yfinance
pip install matplotlib
pip install ipympl
```

## Major Issues

### Libraries using floating point to represent decimal values in finance

Currently, the only data provider I'm using is the `yfinance` library.

Unfortunately, it presents currency data in a floating point data type, which has been *insanely nightmarish* to work with due to imprecision when representing decimal numbers with decimal places. My adapter code that calls this library attempts to make the best estimate it can based on the data provided, but it's impossible to get it perfectly right. For penny stocks (which trade with sub-cent spreads) and dividend payouts (which are often calculated with many decimal places), the numbers will be (painfully) imprecise.

I might consider calling the Yahoo Finance API directly instead in the future, though I'll just use the `yfinance` library for now since it's easier. I also don't have that many options for free data sources, but I'll continue on the lookout for more reliable free APIs and data sources to include as a data provider for this project.

### Potential loss of precision from the use of `fractions.Fraction` and `decimal.Decimal`

I currently have no checks in place for whether a `fractions.Fraction` or `decimal.Decimal` operation will result in loss of precision.

This will need to be audited for in the future.

### I have no idea how taxation works.

If you sell, can you use cost bases from another brokerage account to calculate the realized capital gain?

I'm gonna need to ask an accountant for this one.

## License

All original source code is licensed under the [*GNU Affero General Public License v3.0*](https://www.gnu.org/licenses/agpl-3.0.en.html).

