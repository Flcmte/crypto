"use strict";

const ccxt =        require('ccxt') // Exchange library
    , GSheets =     require('./GSheets.js') // Calls on Google Spreadsheets API
    , ws =          require('ws')  // WebSocket
    , MongoDB =     require('./mongodbutils.js')
    , log  =        require ('ololog')
    , asTable =     require ('as-table').configure ({ delimiter: ' | ' })

let sleep = (ms) => new Promise (resolve => setTimeout (resolve, ms))

let exchangeIDs = ['bittrex','kraken','binance'];
let exchanges = {};


MongoDB.connectDB(async (err) => {
  if (err) {
    log("MongoDB connection error")
    log("------------------------")
    throw err;
  }
  // Load db & collections
  const db = await MongoDB.getDB();

  while (true) {
    try {  
      for (let exchangeID of exchangeIDs) {
        let exchange = new ccxt[exchangeID] (ccxt.extend ({
        'enableRateLimit': false,
        'timeout': 30000
          }))
        exchanges[exchangeID] = exchange;
        let markets = await exchange.loadMarkets ()
        }
      let uniqueSymbols = await ccxt.unique (ccxt.flatten (exchangeIDs.map (exchangeID => exchanges[exchangeID].symbols)))
      // filter out symbols that are not present on at least two exchanges
      let arbitrableSymbols = uniqueSymbols
          .filter (symbol =>
              exchangeIDs.filter (exchangeID =>
                  (exchanges[exchangeID].symbols.indexOf (symbol) >= 0)).length > 1)
          .sort ((id1, id2) => (id1 > id2) ? 1 : ((id2 > id1) ? -1 : 0))

      // print a table of arbitrable symbols
      var arbitrableTable = arbitrableSymbols.map (symbol => {
          let row = { symbol }
          for (let exchangeID of exchangeIDs)
              if (exchanges[exchangeID].symbols.indexOf (symbol) >= 0)
                  row[exchangeID] = exchanges[exchangeID]
          return row;
      })

     // for (let CURRENT_RETRY = 0; CURRENT_RETRY < 2; ++CURRENT_RETRY) {

          let arbitragePromises = []

          for (let arbitrableRow of arbitrableTable) {
            let symbol = arbitrableRow.symbol
            let arbitrableExchanges = {}
            for (let data in arbitrableRow){
              if (data !== 'symbol') {
                arbitrableExchanges[data] = arbitrableRow[data];
              }
            }
            arbitragePromises.push(getArbitrage(arbitrableExchanges, symbol, db));
          }

          await Promise.all(arbitragePromises);

          log(arbitrableSymbols.length, 'symbols arbitrages loaded at', new Date(Date.now()).toLocaleTimeString())
          await updateGSheet(db,'Arbitrages','Arbitrages');
          await sleep(10000); // prevent ban from binance call limit of 1200 requests per 60 secondes
        }
     // }
    catch (e) {
      log.dim ('--------------------------------------------------------')
      log (e.constructor.name, e.message)
      log.dim ('--------------------------------------------------------')
      log.error ('Application failed.')
    }
  }
})

/* Fetch exchange public data via CCXT unified methods - REST API */
async function getArbitrage (exchanges = {}, symbol = undefined, db = undefined) {
  // get tickers
  try {
    let tickers = [];
    for (let exchange of Object.values(exchanges)) {

      tickers.push(exchange.fetchTicker(symbol).then(ticker => {
        ticker.exchange = exchange.name;
        
        return ticker;
      }));
    }
    
    tickers = await Promise.all(tickers);
    
    var doc = {};
    doc.market = symbol;
    
    let arbitrageDoc = tickers.reduce(function (acc,ticker,index,sourceArray) {
      
      for (let sourceTicker of sourceArray){
        doc[sourceTicker.exchange] = {}
        doc[sourceTicker.exchange].bid = sourceTicker.bid;
        doc[sourceTicker.exchange].ask = sourceTicker.ask;
        doc[sourceTicker.exchange].datetime = sourceTicker.datetime;
        doc[sourceTicker.exchange].timestamp = sourceTicker.timestamp;
        if (ticker.exchange !== sourceTicker.exchange) {
          var arbitrageWay1 = ticker.exchange + '_to_' + sourceTicker.exchange;
          var arbitrageWay2 = sourceTicker.exchange + '_to_' + ticker.exchange;
          
          doc['difference.' + arbitrageWay1] = sourceTicker.bid - ticker.ask;
          doc['difference.' + arbitrageWay2] = ticker.bid - sourceTicker.ask;
          doc['percentage.' + arbitrageWay1] = Math.round((sourceTicker.bid - ticker.ask)/ticker.ask * 100 * 10000) / 1000000;
          doc['percentage.' + arbitrageWay2] = Math.round((ticker.bid - sourceTicker.ask)/ticker.ask * 100 * 10000) / 1000000;

            }
          }
      return doc;
    })
    await db.collection("Arbitrages").updateOne(
      {_id: symbol}, 
      {$set: arbitrageDoc},
      {upsert: true},
      function(err, result) {
        if (err) {
          console.log(err)
        } else {
          //console.log (symbol + " arbitrage loaded")
        }
      });
  } catch (e) {
      if (e instanceof ccxt.DDoSProtection || e.message.includes('ECONNRESET')) {
        console.log('[DDoS Protection] ' + e.message)
      } else if (e instanceof ccxt.RequestTimeout) {
        console.log('[Request Timeout] ' + e.message)
      } else if (e instanceof ccxt.AuthenticationError) {
        console.log('[Authentication Error] ' + e.message)
      } else if (e instanceof ccxt.ExchangeNotAvailable) {
        console.log('[Exchange Not Available Error] ' + e.message)
      } else if (e instanceof ccxt.ExchangeError) {
        console.log('[Exchange Error] ' + e.message)
      } else if (e instanceof ccxt.NetworkError) {
        console.log('[Network Error] ' + e.message)
      } else {
        throw e;
      }
  }
//   await getPublicExData (db,exchange);
}

async function getDataFromMongo (db, dataType) {
  
  return new Promise(async function(resolve, reject) {
    var data = [];
    switch (dataType) {
      case 'Arbitrages':
        await db.collection(dataType).find().sort({_id: 1}).toArray(function(err, result) {
          if (err) {reject(err)}
          resolve(result);
        });
    }
  });
}

async function updateGSheet (db = undefined, dataType = undefined, sheet = undefined) {
  try {
    var data = [];
    var mdata;
    mdata = await getDataFromMongo(db,dataType);
    await GSheets.updateGSheetData(sheet,mdata,dataType);
    await sleep(2000);
  } catch (e) {
    throw e;
  }
//   await updateGSheet(db,dataType,sheet);
}