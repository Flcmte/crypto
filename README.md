# Crypto Interface
Application interfacing data from your private exchange accounts on Bittrex, Kraken, Binance, pushing all data to a self-hosted MongoDB database, and updating your private Google Spreadsheet for personnal investment follow-up

### Functionalities
* Arbitrage monitoring between Bittrex, Binance and Kraken
* Pagination of trades from any market on Kraken and storage to MongoDB instance
* Balance aggregation
* Google Sheets as a web UI, using it´s full power to personalize the assets management tool

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
**Flcmte** - *Initial work*

