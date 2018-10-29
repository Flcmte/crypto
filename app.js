"use strict";

const ccxt =        require('ccxt') // Exchange library
    , GSheets =     require('./GSheets.js') // Calls on Google Spreadsheets API
    , ws =          require('ws')  // WebSocket
    , MongoDB =     require('./mongodbutils.js')
    , log  =        require ('ololog')
    , asTable =     require ('as-table').configure ({ delimiter: ' | ' })

const binancews = new ws('wss://stream.binance.com:9443/ws/!ticker@arr');

let sleep = (ms) => new Promise (resolve => setTimeout (resolve, ms)) // Sleep function: use 'await sleep (1000);' to wait 1sec
let krakenOfsCheck = 0; // 0 for first instance of the private calls and fetch of all trades from Kraken. 1 for all other instance, call only last trades
let MAX_RETRIES = 5;
 // Connect to MongoDB and put server instantiation code inside
 // because we start the connection first
 MongoDB.connectDB(async (err) => {
   if (err) {
     log("MongoDB connection error")
     log("------------------------")
     throw err
   }
   // Load db & collections
   const db = MongoDB.getDB();

   
   // get exchange keys from your Google Spreadsheet
   let keys = await GSheets.getGSheetKeys();
   let bmarkets = await GSheets.getGSheetBinanceMarkets();
   // Instantiation of exchanges
   let kraken = new ccxt.kraken({ //create a new APIkey used ONLY by this app, not anything else or it will return NONCE error
     apiKey: keys.kraken.public,
     secret: keys.kraken.private,
     'enableRateLimit': true,
     'timeout': 30000
   });
   let bittrex = new ccxt.bittrex({
     apiKey: keys.bittrex.public,
     secret: keys.bittrex.private,
    'enableRateLimit': true,
     'timeout': 30000
   });
   let binance = new ccxt.binance({
     apiKey: keys.binance.public,
     secret: keys.binance.private,
     'enableRateLimit': true,
     'timeout': 30000
   });
//    for (let CURRENT_RETRY = 0; CURRENT_RETRY < MAX_RETRIES; ++CURRENT_RETRY) {
   while (true){
     try {
       while (true){
         await Promise.all([
           getPrivateExData(db,binance,bmarkets),
           getPublicExData(db,binance),
           getPrivateExData(db,kraken),
           getPublicExData(db,kraken),
           getPrivateExData(db,bittrex),
           getPublicExData(db,bittrex)
         ])
         
         // Process MongoDB internal data
         await Promise.all([
           unifiedTrades(db)
         ])
         
         await Promise.all([
           updateGSheet(db,"Order","nodejsOrder"),
           updateGSheet(db,"Balance","Balances"),
           updateGSheet(db,"Ticker","nodejsTicker"),
           updateGSheet(db,"Trade","nodejsTrade"),
           updateGSheet(db,"OHLC","nodejsOHLC","BTC/USD",kraken),
           updateGSheet(db,"Transaction","nodejsTransaction"),
           updateGSheet(db,"Unified_Trades","nodejsuTrades")
         ])
       }

     } catch (e) {
           log.dim ('--------------------------------------------------------')
           log (e.constructor.name, e.message)
           log.dim ('--------------------------------------------------------')
     }
   }
 })

/* Fetch exchange private data every minutes */
async function getPrivateExData(db, exchange, markets = undefined) {
  
    try {
    // fetch BALANCE from exchange
      let balance = await exchange.fetchBalance();
      // MongoDB update balance
      db.collection("Balance").updateOne(
        {_id: exchange.name + '.balance'},
        {$set: balance},
        {upsert: true},
        function(err, result) {
          if (err) {
            console.log('----- ' + exchange.name + ' update balance error -----');
            console.log(err)
          } else {
            console.log(exchange.name + " balance updated");
          }
       });
      await sleep(exchange.rateLimit);
      
      
      
    // fetch ORDERS: fetchOrders (symbol = undefined, since = undefined, limit = undefined, params = {})
      let orders = [];
      let trades = [];
      if (markets && exchange.name == 'Binance') { // fetch binance all orders
        for (var symbol of markets) {
          var result = await exchange.fetchOrders(symbol);
          for (var order of result) {
            order.exchange = exchange.name;
            order._id = exchange.name + '.' + order.id;
            orders.push(order);
          }
          
          await sleep(exchange.rateLimit);
        };
      } else if (exchange.name == 'Bittrex') { // fetch bittrex last closed orders
        var result = await exchange.fetchClosedOrders(); 
        for (var order of result ) {
          order.exchange = exchange.name;
          order._id = exchange.name + '.' + order.id;
          orders.push(order);
        }
        await sleep(exchange.rateLimit);
        
      } else if (exchange.name == 'Kraken') { // fetch kraken closed orders
        var result = await exchange.fetchClosedOrders(); 
        for (var order of result ) {
          order.exchange = exchange.name;
          order._id = exchange.name + '.' + order.id;
          orders.push(order);
        }
        await sleep(exchange.rateLimit);
      } else {
        orders = exchange.fetchOrders();
        await sleep(exchange.rateLimit);
      }
      // MongoDB update orders
      for (var order of orders) {
        if (Object.keys(orders).length !== 0) {
          db.collection("Order").updateOne(
            {_id: order._id},
            {$setOnInsert: order},
            {upsert: true},
            function(err, result) {
              if (err) {
                console.log('----- ' + exchange.name + ' update orders error -----');
                console.log(err)
              }
           });
        } else {console.log('No orders to update for ' + exchange.name);}
      }
      console.log(exchange.name + ' ' + orders.length + ' orders updated')
      
      // fetch TRADES: fetchMyTrades (symbol = undefined, since = undefined, limit = undefined, params = {})
     
      if (markets && exchange.name == 'Binance') { // fetch binance all trades
        for (var symbol of markets) {
          var result = await exchange.fetchMyTrades(symbol);
          for (var trade of result) {
            trade.exchange = exchange.name;
            trade._id = exchange.name + '.' + trade.id;
            trades.push(trade);
          }
          await sleep(exchange.rateLimit);
        };
      } else if (exchange.name == 'Kraken') { // fetch kraken trades
        var offset = 0;
        var result = await exchange.fetchMyTrades(undefined,undefined,undefined,{'ofs':offset}); //fetch all trades with offset param, 50 trades max by API call
        for (var trade of result ) {
          trade.exchange = exchange.name;
          trade._id = exchange.name + '.' + trade.id;
          trades.push(trade);
        }
        await sleep(exchange.rateLimit);
        if (krakenOfsCheck == 0) {
          //*** This part is used to retrieve older trades (offset > 50), ***//
          //*** but deactivated as it will rewrite manual updates in the DB for DAO trades fees ***//
          
//           console.log('Kraken all trades loaded at first function call. Next calls will fetch only last 50 trades');
//           while (result.length != 0) {
//             offset = offset + 50;
//             result = await exchange.fetchMyTrades(undefined,undefined,undefined,{'ofs':offset});
//             for (var trade of result ) {
//               if (trade.info.pair == "XDAOXETH") {trade.symbol = "DAO/ETH"; trade.fee.currency = "ETH";}
              
//               else if (trade.info.pair == "XDAOXXBT") {trade.symbol = "DAO/BTC"; trade.fee.currency = "BTC";}
//               trade.exchange = exchange.name;
//               trade._id = exchange.name + '.' + trade.id;
//               trades.push(trade);
//             }
//             await sleep(exchange.rateLimit);
//             console.log('Kraken current offset: ' + offset);
//           }
        }
        krakenOfsCheck = 1; // Kraken trades initialisation done
      } else if (exchange.name == 'Bittrex') {//Bittrex doesn't give trades details, only orders. Trades are adapted from orders data
        for (let order of orders) {
          if (order.status == "closed") {
            trades.push({
              _id: order._id,
              amount: order.filled, //this is important for trades as it reflect only effective amount traded and filled
              cost: order.cost,
              datetime: order.datetime,
              exchange: order.exchange,
              fee: order.fee,
              id: order.id,
              info: order.info,
              order: order.id,
              price: order.average,
              side: order.side,
              symbol: order.symbol,
              timestamp: order.timestamp,
              type: order.type
            })
          }
        }
      }
      
      // MongoDB update trades
      for (var trade of trades) {
        if (Object.keys(trades).length != 0) {
          db.collection("Trade").updateOne(
              {_id: trade._id},
              {$setOnInsert: trade},
              {upsert: true},
            function(err, result) {
              if (err) {
                console.log('----- ' + exchange.name + ' update trades error -----');
                console.log(err)
              }
          });
        }
      }
      console.log(exchange.name + ' ' + trades.length + ' trades updated')
      
      
      //** Get Transactions - Deposits and Withdrawals **//
      let transactions = []
      if (exchange.name == 'Binance') { // fetch binance transactions
          var result_dep = await exchange.fetchDeposits(); // fetch Deposits
          for (var transaction of result_dep) {
            transaction.exchange = exchange.name;
            transaction._id = exchange.name + '.' + transaction.info.txId;
            transactions.push(transaction);
          }
          var result_wit = await exchange.fetchWithdrawals(); // fetch Deposits
          for (var transaction of result_wit) {
            transaction.exchange = exchange.name;
            transaction._id = exchange.name + '.' + transaction.info.id;
            transactions.push(transaction);
          }
          await sleep(exchange.rateLimit);
      }
      
      else if (exchange.name == 'Kraken') { // fetch kraken transactions
        var depositsResults = await exchange.privatePostLedgers ({'type':'deposit'});
        var withdrawResults = await exchange.privatePostLedgers ({'type':'withdrawal'});
        var deposits = depositsResults.result.ledger;
        var withdraws = withdrawResults.result.ledger;
        var currency;
        var datetime;
        for (var depositId in deposits){
          datetime = new Date(Math.trunc(deposits[depositId].time * 1000));
          datetime = datetime.toISOString();

          if (deposits[depositId].asset === 'XXBT') {currency = 'BTC'}
          else if (deposits[depositId].asset === 'ZEUR') {currency = 'EUR'}
          else if (deposits[depositId].asset === 'ADA') {currency = 'ADA'}
          else if (deposits[depositId].asset === 'QTUM') {currency = 'QTUM'}

          transactions.push({
            _id: exchange.name + '.' + deposits[depositId].refid,
            exchange: exchange.name,
            info: deposits[depositId],
            address: null,
            amount: deposits[depositId].amount,
            currency: currency,
            datetime: datetime,
            fee: {cost: deposits[depositId].fee},
            id: null,
            status: "ok",
            timestamp: Math.trunc(deposits[depositId].time * 1000),
            txid: deposits[depositId].refid,
            type: deposits[depositId].type,
            updated: null,
            tag: ""
          })
        }

        for (var withdrawId in withdraws){
          datetime = new Date(Math.trunc(withdraws[withdrawId].time * 1000));
          datetime = datetime.toISOString();

          if (withdraws[withdrawId].asset === 'XXBT') {currency = 'BTC'}
          else if (withdraws[withdrawId].asset === 'ZEUR') {currency = 'EUR'}
          else if (withdraws[withdrawId].asset === 'XETH') {currency = 'ETH'}
          else if (withdraws[withdrawId].asset === 'XXRP') {currency = 'XRP'}
          else if (withdraws[withdrawId].asset === 'ADA') {currency = 'ADA'}
          else if (withdraws[withdrawId].asset === 'XLTC') {currency = 'LTC'}
          else if (withdraws[withdrawId].asset === 'EOS') {currency = 'EOS'}
          else if (withdraws[withdrawId].asset === 'USDT') {currency = 'USDT'}

          transactions.push({
            _id: exchange.name + '.' + withdraws[withdrawId].refid,
            exchange: exchange.name,
            info: withdraws[withdrawId],
            address: null,
            amount: - withdraws[withdrawId].amount, //api gives negatives amounts
            currency: currency,
            datetime: datetime,
            fee: {cost: withdraws[withdrawId].fee},
            id: null,
            status: "ok",
            timestamp: Math.trunc(withdraws[withdrawId].time * 1000),
            txid: withdraws[withdrawId].refid,
            type: withdraws[withdrawId].type,
            updated: null,
            tag: ""
          })
        }
          await sleep(exchange.rateLimit);
      }
      else if (exchange.name == 'Bittrex') { // fetch bittrex transactions
        var depositsResults = await exchange.accountGetDeposithistory ();
        var withdrawResults = await exchange.accountGetWithdrawalhistory ();

        var deposits = depositsResults.result;
        var withdraws = withdrawResults.result;
        var timestamp;


        for (var deposit of deposits){
          datetime = new Date(deposit.LastUpdated);
          timestamp = datetime.getTime();

          transactions.push({
            _id: exchange.name + '.' + deposit.Id,
            exchange: exchange.name,
            info: deposit,
            address: deposit.CryptoAddress,
            amount: deposit.Amount,
            currency: deposit.Currency,
            datetime: datetime.toISOString(),
            fee: {cost: null},
            id: null,
            status: "ok",
            timestamp: timestamp,
            txid: deposit.TxId,
            type: "deposit",
            updated: null,
            tag: ""
          })
        }

        for (var withdraw of withdraws){
          datetime = new Date(withdraw.Opened);
          timestamp = datetime.getTime();
          
          transactions.push({
            _id: exchange.name + '.' + withdraw.TxId,
            exchange: exchange.name,
            info: withdraw,
            address: withdraw.Address,
            amount: withdraw.Amount,
            currency: withdraw.Currency,
            datetime: datetime.toISOString(),
            fee: {cost: withdraw.TxCost},
            id: null,
            status: "ok",
            timestamp: timestamp,
            txid: withdraw.TxId,
            type: "withdrawal",
            updated: null,
            tag: ""
          })
        }
          await sleep(exchange.rateLimit);
      }
      
      // MongoDB update transactions
      for (var transaction of transactions) {
        if (Object.keys(transactions).length != 0) {
          db.collection("Transaction").updateOne(
              {_id: transaction._id},
              {$setOnInsert: transaction},
              {upsert: true},
            function(err, result) {
              if (err) {
                console.log('----- ' + exchange.name + ' update trades error -----');
                console.log(err)
              }
          });
        }
      }
      console.log(exchange.name + ' ' + transactions.length + ' transactions updated')
      
      
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
  

//   await getPrivateExData(db,exchange,markets);
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
    if (exchange.name == 'Kraken') {await sleep(10000)}
    var result = await exchange.fetchTickers();
    for (var ticker of Object.values(result)) {
      ticker.exchange = exchange.name;
      db.collection("Ticker").updateOne(
        {_id: exchange.name + "." + ticker.symbol}, 
        {$set: ticker},
        {upsert: true},
      function(err, result) {
        if (err) {
          console.log(err)
        }
      });
    }
    console.log(exchange.name + " tickers updated: " + Object.keys(result).length);

    await sleep(exchange.rateLimit);
  
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

// process Transactions and Trades to unify it in one collection for easy calculations process
async function unifiedTrades (db) {
  
  var data = [];
  try {
    await db.collection('Trade')
      .aggregate([{
        $lookup: 
         {
           from: "Unified_Trades",
           localField: "_id",
           foreignField: "_id",
           as: "matching_utrade"
         }
      }])
      .toArray(async function(err, result) {
      if (err) {reject(err)}

      for (var trade of result) {

        let buy_currency,sell_currency,buy_amount,sell_amount;
        let fee_currency = trade.fee.currency;
        let fee_amount = trade.fee.cost;
        let market = trade.symbol;
        let price = trade.price;
        
        if (trade.side == "buy") {
          buy_currency = trade.symbol.substring(0,trade.symbol.indexOf("/"));
          sell_currency = trade.symbol.substring(trade.symbol.indexOf("/") + 1,trade.symbol.length);
          buy_amount = trade.amount;
          sell_amount = trade.cost;
        } else if (trade.side == "sell") {
          buy_currency = trade.symbol.substring(trade.symbol.indexOf("/") + 1,trade.symbol.length);
          sell_currency = trade.symbol.substring(0,trade.symbol.indexOf("/"));
          buy_amount = trade.cost;
          sell_amount = trade.amount;
        }
        data.push({
          _id: trade._id,
          exchange: trade.exchange,
          type: "trade",
          buy_currency: buy_currency,
          buy_amount: buy_amount,
          sell_currency: sell_currency,
          sell_amount: sell_amount,
          fee_currency: fee_currency,
          fee_amount: fee_amount,
          market: market,
          price: price,
          datetime: trade.datetime,
          timestamp: trade.timestamp
        })
      }
    });
    
    await db.collection('Transaction')
      .aggregate([{
        $lookup: 
         {
           from: "Unified_Trades",
           localField: "_id",
           foreignField: "_id",
           as: "matching_utrade"
         }
      }])
      .toArray(async function(err, result) {
      if (err) {reject(err)}

      for (var transaction of result) {
        let buy_currency,sell_currency,buy_amount,sell_amount,fee_currency,fee_amount;
        
        if (transaction.type == "deposit") {
          buy_currency = transaction.currency;
          buy_amount = transaction.amount;
          sell_currency = "";
          sell_amount = "";
          fee_currency = transaction.currency;
          fee_amount = transaction.fee.cost;
        } else if (transaction.type == "withdrawal") {
          buy_currency = "";
          buy_amount = "";
          sell_currency = transaction.currency;
          sell_amount = transaction.amount;
          fee_currency = transaction.currency;
          fee_amount = transaction.fee.cost;
        }
        
        data.push({
          _id: transaction._id,
          exchange: transaction.exchange,
          type: transaction.type,
          buy_currency: buy_currency,
          buy_amount: buy_amount,
          sell_currency: sell_currency,
          sell_amount: sell_amount,
          fee_currency: fee_currency,
          fee_amount: fee_amount,
          datetime: transaction.datetime,
          timestamp: transaction.timestamp
        })
      }
    });
    
    await sleep(1000)
        // MongoDB update unifiedTrades
    for (var utrade of data) {
      if (Object.keys(utrade).length != 0) {
        await db.collection("Unified_Trades").updateOne(
          {_id: utrade._id},
          {$set: utrade},
          {upsert: true},
          function(err, result) {
            if (err) {
              console.log('----- MongoDB unifiedTrades function error -----');
              console.log(err)
            }
            
          });
      }
    }
    console.log('MongoDB unifiedTrades updated: ' + data.length)
  } catch (e) {
       log.dim ('--------------------------------------------------------')
       log (e.constructor.name, e.message)
       log.dim ('--------------------------------------------------------')
     }
}

async function getDataFromMongo (db, dataType, symbol, exchange) {
  
  return new Promise(async function(resolve, reject) {
    var data = [];
    switch (dataType) {
      case 'Balance':
        await db.collection(dataType).find().toArray(function(err, result) {
          if (err) {reject(err)}
          for (var index of result) {
            data.push({exchange: index._id, total: index.total})
          }
          resolve(data);
        });
        break;
      case 'Order':
      case 'Ticker':
      case 'Trade':
      case 'Transaction':
      case 'Unified_Trades':
        await db.collection(dataType).find().sort({datetime:1}).toArray(function(err, result) {
          if (err) {reject(err)}
          resolve(result);
        });
        break;
      case 'OHLC':
        var last15days = new Date(new Date().setDate(new Date().getDate() - 15)); // last 15 days of data
        await db.collection(exchange.name + '_' + symbol + '_Public_Trade').aggregate([
          {$match: {
            timestamp: { $gte: last15days.getTime() }}
          },
          {$group: {
            _id: {
              year: {$year: {$toDate: "$timestamp"}},
              month: {$month: {$toDate: "$timestamp"}},
              day: { $dayOfMonth: {$toDate: "$timestamp"}
//                 $subtract: [{ $dayOfMonth: {$toDate: "$timestamp"}},
//                             { $mod: [{$dayOfMonth: {$toDate: "$timestamp"}}, 1]}] // OHLC by day
              },
              hour: { // $hour: {$toDate: "$timestamp"}}
                $subtract: [{ $hour: {$toDate: "$timestamp"}},
                            { $mod: [{$hour: {$toDate: "$timestamp"}}, 6]}] // OHLC by 6 hours
              }
//               minute: {
//                 $subtract: [{ $minute: {$toDate: "$timestamp"}},
//                             { $mod: [{$minute: {$toDate: "$timestamp"}}, 30]}]
//               }
            },
            open: {$first: "$price"},
            high: {$max: "$price"},
            low: {$min: "$price"},
            close: {$last: "$price"},
            volume: {$sum: {$toDouble: "$amount"}}
          }
          },
          {$sort: {
            _id: 1
          }
          }]).toArray(function (err, result){
          if (err) {reject(err)}
          resolve(result);
        })
    }
  });
}

async function updateGSheet (db = undefined, dataType = undefined, sheet = undefined, symbol = undefined, exchange = undefined) {
  try {
    var data = [];
    var mdata;
    mdata = await getDataFromMongo(db,dataType,symbol,exchange);
    await GSheets.updateGSheetData(sheet,mdata,dataType);
    await sleep(30000);
  } catch (e) {
    throw e;
  }
//   await updateGSheet(db,dataType,sheet);
}

