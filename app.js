"use strict";

const ccxt =        require('ccxt') // Exchange library
    , GSheets =     require('./GSheets.js') // Calls on Google Spreadsheets API
    , ws =          require('ws')  // WebSocket
    , MongoDB =     require('./mongodbutils.js')

const binancews = new ws('wss://stream.binance.com:9443/ws/!ticker@arr');

let sleep = (ms) => new Promise (resolve => setTimeout (resolve, ms)) // Sleep function: use 'await sleep (1000);' to wait 1sec
let krakenOfsCheck = 0; // 0 for first instance of the private calls and fetch of all trades from Kraken. 1 for all other instance, call only last trades

 // Connect to MongoDB and put server instantiation code inside
 // because we start the connection first
 MongoDB.connectDB(async (err) => {
   if (err) throw err
   // Load db & collections
   const db = MongoDB.getDB();

   
   // get exchange keys from your Google Spreadsheet
   let keys = await GSheets.getGSheetKeys();
   let bmarkets = await GSheets.getGSheetBinanceMarkets();
   // Instantiation of exchanges
   let kraken = new ccxt.kraken({ //create a new APIkey used ONLY by this app, not anything else or it will return NONCE error
     apiKey: keys.kraken.public,
     secret: keys.kraken.private,
     'enableRateLimit': true
   });
   let bittrex = new ccxt.bittrex({
     apiKey: keys.bittrex.public,
     secret: keys.bittrex.private,
    'enableRateLimit': true
   });
   let binance = new ccxt.binance({
     apiKey: keys.binance.public,
     secret: keys.binance.private,
     'enableRateLimit': true
   });

   try {
     
     
//      const bprivate = getPrivateExData(db,binance,bmarkets);
      const bpublic = getPublicExData(db,binance);

     
 //    const kprivate = getPrivateExData(db,kraken);
//      const kpublic = getPublicExData(db,kraken);

     
     
//      const xprivate = getPrivateExData(db,bittrex);
//      const xpublic = getPublicExData(db,bittrex);


     
//      const updateGorder = updateGSheet(db,"Order","nodejsOrder");
//      const updateGbalance = updateGSheet(db,"Balance","nodejs");
      const updateGticker = updateGSheet(db,"Ticker","nodejsTicker");
//      const updateGtrade = updateGSheet(db,"Trade","nodejsTrade");
     
//      await bprivate;
//      await bpublic;
//      await kpublic;
//      await kprivate;
//      await xprivate;
//      await xpublic;
//      await updateGorder;
//      await updateGbalance;
//      await updateGticker;
//     await updateGtrade;
   } catch (e) {
     throw e
   }
 })

/* Fetch exchange private data every minutes */
async function getPrivateExData(db, exchange, markets = undefined) {
  
    try {
    // fetch BALANCE from exchange
      let balance = await exchange.fetchBalance();
      
      
      
    // fetch ORDERS: fetchOrders (symbol = undefined, since = undefined, limit = undefined, params = {})
      let orders = {};
      let trades = {};
      if (markets) { // fetch binance all orders
        for (var symbol of markets) {
          var borders = await exchange.fetchOrders(symbol);
          for (var ord of borders) {
            orders[ord.id] = ord;
          }
        };
      } else if (exchange.name == 'Bittrex') { // fetch bittrex last closed orders
        var xorders = await exchange.fetchClosedOrders();  
        for (var ord of xorders ) {
          orders[ord.id] = ord;
          trades[ord.id] = ord;
        }
        
      } else if (exchange.name == 'Kraken') { // fetch kraken closed orders
        var korders = await exchange.fetchClosedOrders(); 
        for (var ord of korders ) {
          orders[ord.id] = ord;
        }
      } else {
        orders = exchange.fetchOrders();
      }
      
      // fetch TRADES: fetchMyTrades (symbol = undefined, since = undefined, limit = undefined, params = {})
     
      if (markets && exchange.name == 'Binance') { // fetch binance all trades
        for (var symbol of markets) {
          var btrades = await exchange.fetchMyTrades(symbol);
          for (var tra of btrades) {
            trades[tra.id] = tra;
          }
        };
      } else if (exchange.name == 'Kraken') { // fetch kraken trades
        var offset = 0;
        var ktrades = await exchange.fetchMyTrades(undefined,undefined,undefined,{'ofs':offset}); //fetch all trades with offset param, 50 trades max by API call
        for (var tra of ktrades ) {
          trades[tra.id] = tra;
        }
        while (ktrades.length != 0 && krakenOfsCheck == 0) {
          offset = offset + 50;
          ktrades = await exchange.fetchMyTrades(undefined,undefined,undefined,{'ofs':offset});
          for (var tra of ktrades ) {
            trades[tra.id] = tra;
          }

          console.log('Kraken all trades loaded at first function call. Next calls will fetch only last 50 trades');
          console.log('Kraken current offset: ' + offset);
        }
      
        krakenOfsCheck = 1; // Kraken trades initialisation done

      } else if (exchange.name == 'Bittrex') {
        //see in orders, bittrex, as orders and trades are similar for this exchange
      }
      
      // MongoDB update balance
      db.collection("Balance").updateOne(
        {_id: exchange.name + '.balance'},
        {$set: balance},
        {
          upsert: true
        },
        function(err, result) {
          if (err) {
            console.log('----- ' + exchange.name + ' update balance error -----');
            console.log(err)
          } else {
            console.log(exchange.name + " balance updated");
          }
       });
      // MongoDB update orders
      if (Object.keys(orders).length != 0) {
        db.collection("Order").updateOne(
          {_id: exchange.name + '.orders'},
          {$set: orders},
          {
            upsert: true
          },
          function(err, result) {
            if (err) {
              console.log('----- ' + exchange.name + ' update orders error -----');
              console.log(err)
            } else {
              console.log(exchange.name + " orders updated");
            }
         });
      } else {console.log('No orders to update for ' + exchange.name);}
      
      // MongoDB update trades
      if (Object.keys(trades).length != 0) {
        db.collection("Trade").updateOne(
          {_id: exchange.name + '.trades'},
          {$set: trades},
          {
            upsert: true
          },
          function(err, result) {
            if (err) {
              console.log('----- ' + exchange.name + ' update trades error -----');
              console.log(err)
            } else {
              console.log(exchange.name + " trades updated");
            }
        });
      }
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
  
  await sleep(45000);
  await getPrivateExData(db,exchange,markets);
}

/* Fetch exchange public data via WebSocket */
async function wsPublicExData(db,exchange,websocket = undefined) {
  
  /* Binance PUBLIC websocket */
  /* Ticker */
    websocket.on('message',async function incoming(data) {
      var parsedData = JSON.parse(data);
      var TickerObj = parsedData.reduce((obj, item) => {
        obj[item.s] = item
        return obj
      }, {});
      db.collection("Ticker").updateOne({"_id" : exchange.name +".ticker"}, {
          $set: TickerObj
        }, {
          upsert: true
        },
        function(err, result) {
          if (err) {
            console.log(err)
          } else {
            console.log(exchange.name + " tickers updated: " + Object.keys(TickerObj).length);
          }
        });
    });

 }

/* Fetch exchange public data via CCXT unified methods - REST API */
async function getPublicExData (db,exchange) {
  // get tickers
  try {
  var tickers = await exchange.fetchTickers();
  db.collection("Ticker").updateOne({"_id" : exchange.name +".ticker"}, {
    $set: tickers
  }, {
    upsert: true
  },
                       function(err, result) {
    if (err) {
      console.log(err)
    } else {
      console.log(exchange.name + " tickers updated: " + Object.keys(tickers).length);
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
  await sleep(8000);
  await getPublicExData (db,exchange);
}


async function getDataFromMongo (db, dataType) {  
  return new Promise(function(resolve, reject) {
    var data = [];
    
    db.collection(dataType).find({}).toArray(function(err, result) {
      if (err) {
        reject(err);
      }
      
      if (dataType == 'Balance') {
        for (var index of result) {
          data.push({exchange: index._id, total: index.total})
        }
        resolve(data);
      } else {
        resolve(result);
      }
    });
  });
}

async function updateGSheet (db = undefined, dataType = undefined, sheet = undefined) {
  
  var data = []
  var mdata = await getDataFromMongo(db,dataType);
  GSheets.updateGSheetData(sheet,mdata,dataType);
  await sleep(10000);
  await updateGSheet(db,dataType,sheet);
}

