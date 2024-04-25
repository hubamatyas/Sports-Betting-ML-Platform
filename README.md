# Sports Betting Research and Machine Learning Platform

## Overview
This project integrates various types of sports data — pre-game, in-play, and exchange data — to facilitate sophisticated model building and in-depth analysis in sports betting. The code corresponds to Chapter 3 (Experiment 1) in the BSc Computer Science thesis titled 'Algorithmic Sports Arbitrage Using Statistical Machine Learning'. The rich data integration supports the development of robust betting strategies and enhances the backtesting process.

### Data Integration
The platform combines data from multiple sources:
- **Pre-game Data**: Offers a historical view and key stats, essential for building predictive models before games start.
- **In-Play Data**: Provides a real-time snapshot of the game, crucial for assessing player performance and updating betting odds.
- **Exchange Data**: Sourced from Betfair, it provides a detailed look at betting market trends, crucial for financial analysis.

These data types are crucial for enabling complex analysis beyond what basic models would allow.

### Research Workflow
The platform is designed to streamline the process of testing sports betting strategies. Here's a typical workflow for accessing and analyzing the data:

1. **Query the Pre-game Database**: Identify matches with specific characteristics, e.g., three goals scored by the 52nd minute.
2. **Retrieve Relevant Event Data**: Cross-reference identified matches with Betfair events in MongoDB to fetch `marketIndex.json` files.
3. **Filter Market Data**: Isolate Betfair market IDs for specific betting conditions such as "Over/Under 3.5 Goals".
4. **Load Exchange Data**: Fetch and load the relevant data files from S3 into MongoDB.
5. **Refine Data**: Filter the exchange data to find instances that meet the betting strategy criteria.
6. **Perform Bulk Backtest**: Analyze the effectiveness of the betting strategy using the refined data.

### Data Architecture and Abstraction
To facilitate efficient data querying and research:
- **Abstraction Layers**: Developed for each data source to enable easy access and look-up.
- **Pre-processing**: Data is pre-processed to match the real-life scenario of live trading data.

The platform supports robust data integration and retrieval, significantly reducing the time and effort needed for backtesting sports betting strategies.

### Practical Viability
The platform demonstrates practical viability and adaptability in real-life scenarios:
- **Seamless Sport Integration**: Easily adaptable to new sports, demonstrated through the incorporation of EPL football data.
- **Automated Backtesting**: Utilizes MongoDB's Time Series Collections for efficient and automated backtesting processes, enabling direct server-side backtests.
