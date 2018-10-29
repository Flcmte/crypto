# Crypto Interface
Application interfacing data from your private exchange accounts on Bittrex, Kraken, Binance, pushing all data to a self-hosted MongoDB database, and updating your private Google Spreadsheet for personnal investment follow-up

### Functionalities
* Google Sheets as a web UI, using its full power to personalize the assets management tool
* Arbitrage monitoring between Bittrex, Binance and Kraken
* Pagination of trades from any market on Kraken and storage to MongoDB instance
* Current balance aggregation
* Balance history and value in BTC at the time of trade (work in progress)

### What does the monitoring tool looks like?
* Arbitrage:
![Arbitrages image not available](https://github.com/Flcmte/crypto/blob/master/img/Arbitrages.PNG)
* Assets monitoring:
![AssetsMon image not available](https://github.com/Flcmte/crypto/blob/master/img/AssetMonTool.PNG)
* Some graphs to improve monitoring:
![Graph image not available](https://github.com/Flcmte/crypto/blob/master/img/ToolGraphs.PNG)

### Prerequisites
Dedicated server with:
* [MongoDB](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-debian/) instance
* [Node.js](https://nodejs.org/en/download/package-manager/) with main modules:
* [ccxt](https://github.com/ccxt/ccxt)
* [mongodb driver](https://github.com/mongodb/node-mongodb-native)
* [googleapis](https://github.com/googleapis)

## Contributing
I'm open to any questions and feedback, don't hesitate to ask!

## Authors
**Flcmte**

