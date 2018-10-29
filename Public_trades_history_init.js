"use strict";

const ccxt =        require('ccxt') // Exchange library
    , GSheets =     require('./GSheets.js') // Calls on Google Spreadsheets API
    , ws =          require('ws')  // WebSocket
    , MongoDB =     require('./mongodbutils.js')
    , log  =        require ('ololog')
    , asTable =     require ('as-table').configure ({ delimiter: ' | ' })


let sleep = (ms) => new Promise (resolve => setTimeout (resolve, ms))


MongoDB.connectDB(async (err) => {
  if (err) {
    log("MongoDB connection error")
    log("------------------------")
    throw err;
  }
  // Load db & collections
  const db = MongoDB.getDB();
  
  // Instantiation of exchange
  let kraken = new ccxt.kraken({
    'enableRateLimit': true,
    'timeout': 30000
  });
  try {
  while (true){
    await Promise.all([
      getPublicTrades('BTC/EUR','XXBTZEUR',kraken,db),
      getPublicTrades('BTC/USD','XXBTZUSD',kraken,db),
      
      getPublicTrades('ETH/EUR','XETHZEUR',kraken,db),
      getPublicTrades('ETH/USD','XETHZUSD',kraken,db),
      getPublicTrades('ETH/BTC','XETHXXBT',kraken,db),
      
      getPublicTrades('XRP/EUR','XXRPZEUR',kraken,db),
      getPublicTrades('XRP/USD','XXRPZUSD',kraken,db),
      getPublicTrades('XRP/BTC','XXRPXXBT',kraken,db),
      
      getPublicTrades('BCH/EUR','BCHEUR',kraken,db),
      getPublicTrades('BCH/USD','BCHUSD',kraken,db),
      getPublicTrades('BCH/BTC','BCHXBT',kraken,db),
      
      getPublicTrades('LTC/EUR','XLTCZEUR',kraken,db),
      getPublicTrades('LTC/USD','XLTCZUSD',kraken,db),
      getPublicTrades('LTC/BTC','XLTCXXBT',kraken,db),
      
      getPublicTrades('ETC/EUR','XETCZEUR',kraken,db),
      getPublicTrades('ETC/USD','XETCZUSD',kraken,db),
      getPublicTrades('ETC/BTC','XETCXXBT',kraken,db),
      
      getPublicTrades('ADA/USD','ADAUSD',kraken,db),
      getPublicTrades('ADA/BTC','ADAXBT',kraken,db)
    ])
  }
  }
  catch (e) {
    log.dim ('--------------------------------------------------------')
    log (e.constructor.name, e.message)
    log.dim ('--------------------------------------------------------')
    log.error ('Application failed.')
}
})

async function getPublicTrades (dbSymbol = undefined, exchangeSymbol = undefined, exchange = undefined, db = undefined) {
  
  let since = 1451606400000000000 // get trades since 2016-01-01 00.00.00
  const collection = 
        // for development purpose
        // 'DEV_' + 
        exchange.name + '_' + dbSymbol + '_Public_Trade'; // Kraken_BTC/EUR_Public_Trade

  await db.collection(collection)
   .find({}, {"timestamp":1})
   .sort({_id:-1})
   .limit(1)
   .toArray(function (err,result){
     if (result.length > 0) {
       since = (result[0].timestamp + 1) * 1000000;
     }
   })

   await sleep(1000)
   
   while (true) { // retry infinitely upon exchange API error
     
     try {
       while (since < exchange.microseconds() * 1000) { // pagination of exchange trades
         
         let allTrades = [];
         const trades = await exchange.publicGetTrades ({'pair':exchangeSymbol, 'since':since})
         if (trades.result[exchangeSymbol].length !== 0) {
           let allTrades = [];
           for (var oneTrade in trades.result[exchangeSymbol]) {
             var oneTradeObj = {};
             var tradeToDB = {};
             var uTrade = trades.result[exchangeSymbol][oneTrade];

             var datetime = new Date(Math.trunc(uTrade[2]*1000));
             datetime = datetime.toISOString();

             oneTradeObj.id = undefined;
             oneTradeObj.order = undefined;
             oneTradeObj.info = uTrade;
             oneTradeObj.timestamp = Math.trunc(uTrade[2]*1000);
             oneTradeObj.datetime = datetime;
             oneTradeObj.symbol = dbSymbol;
             oneTradeObj.type = uTrade[4] == 'l' ? 'limit' : 'market';
             oneTradeObj.side = uTrade[3] == 's' ? 'sell' : 'buy';
             oneTradeObj.price = uTrade[0];
             oneTradeObj.amount = uTrade[1];
             oneTradeObj.cost = uTrade[0] * uTrade[1];
             oneTradeObj.fee = undefined;
             oneTradeObj.exchange = exchange.name;
             oneTradeObj.exchangeSymbol = exchangeSymbol;
             
             allTrades.push(oneTradeObj);
           }
           
           
           // Load Data to MongoDB
           await db.collection(collection).insertMany(
              allTrades,
              function(err, result) {
                if (err) {
                  console.log('----- ' + exchange.name + ' update public trades error -----');
                  console.log(err)
                } else {
                 // console.log(exchange.name + " public trades updated");
                }
           });
           
           since = trades.result.last;
           log.blue('-----')
           log.yellow(dbSymbol + ' # of trades loaded :' + allTrades.length)
           log.yellow(dbSymbol + ' Last datetime of trade loaded :' + allTrades[allTrades.length - 1].datetime)

           
         } else {break;}
       }
       log.blue('-----')
       log.green( dbSymbol + ' --- No more trades to load --- waiting 5 sec ---')
       await sleep(5000)
     } catch (e) {
       log.dim ('--------------------------------------------------------')
       log (e.constructor.name, e.message)
       log.dim ('--------------------------------------------------------')
       let nextRetry_ms = Math.floor(Math.random() * Math.floor(10)) * 1000;
       log.error ('Application is waiting ' + (nextRetry_ms/1000) + ' seconds before next attempt.')
       await sleep(nextRetry_ms);
     }
   }
}