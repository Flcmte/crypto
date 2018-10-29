const fs = require('fs');
const {google} = require('googleapis');
const path = require('path');
const log = require('ololog')

let spreadsheetId = '1DcjxfNBnnMfqAUAFTh2hcqRX3YsVDWX_llGj73gciaM';
let sleep = (ms) => new Promise (resolve => setTimeout (resolve, ms)) // Sleep function: use 'await sleep (1000);' to wait 1sec


async function getGSheetKeys() {

  // Create a new JWT client using the key file downloaded from the Google Developer Console
  var client = await google.auth.getClient({
    keyFile: path.join(__dirname, 'jwt.keys.json'),
    scopes: 'https://www.googleapis.com/auth/spreadsheets'
  });

  // Obtain a new sheets client, making sure you pass along the auth client
  var sheet = await google.sheets({
    version: 'v4',
    auth: client
  });

  // Make an authorized request to get exchange keys.

  return new Promise(function(resolve, reject) {
    let GSheetObj = {};
    //get API private and public keys
    sheet.spreadsheets.values.get({
      spreadsheetId,
      range: 'Param!A4:C7',
    }, function(err, response) {
      if (err) {
        reject(err);
      }
      var keysTable = response.data.values;
      if (keysTable.length) {
        var headers = keysTable.shift();
        var keyArr = keysTable.map(function(values) {
          return headers.reduce(function(obj, item, index) {
            obj[item] = values[index];
            return obj;
          }, {});
        });
        var keyObj = keyArr.reduce(function(obj, item) {
          GSheetObj[item.exchange] = item;
          return obj
        }, {});
        resolve(GSheetObj)

      } else {
        console.log('No GSheet configuration data found.');
      }
    });
  })
}

async function getGSheetBinanceMarkets() {

  // Create a new JWT client using the key file downloaded from the Google Developer Console
  var client = await google.auth.getClient({
    keyFile: path.join(__dirname, 'jwt.keys.json'),
    scopes: 'https://www.googleapis.com/auth/spreadsheets'
  });

  // Obtain a new sheets client, making sure you pass along the auth client
  var sheet = await google.sheets({
    version: 'v4',
    auth: client
  });

  // Make an authorized request to get exchange keys.

  return new Promise(function(resolve, reject) {
      let GSheetObj = {};
      //get Binance used markets for orders history fecthing
      sheet.spreadsheets.values.get({
        spreadsheetId,
        range: 'Param!E1:E20',
      }, function(err, response) {
        if (err) {
          reject(err);
        }
        var marketsTable = response.data.values.slice(1);
        if (marketsTable.length) {
          var marketObj = marketsTable.reduce(function(obj, item) {
            return obj.concat(item);
          });
          GSheetObj = marketObj;
          resolve(GSheetObj)

        } else {
          console.log('No GSheet binance markets found.');
        }
      });
    });
}

// update data in GSheet 
async function updateGSheetData (sheetName = undefined , values = [], dataType = undefined) {

  // Create a new JWT client using the key file downloaded from the Google Developer Console
  var client = await google.auth.getClient({
    keyFile: path.join(__dirname, 'jwt.keys.json'),
    scopes: 'https://www.googleapis.com/auth/spreadsheets'
  });

  // Obtain a new sheets client, making sure you pass along the auth client
  var sheet = await google.sheets({
    version: 'v4',
    auth: client
  });

  let data = [];
  let arr = [];
  let firstCell = "!A1";
  let majorDimension = "ROWS";
  var index,exId;
  var date = new Date();
  
  if (dataType == 'Balance') {
    for (index of values){
      arr = Object.keys(index.total).map(function(key) {
        if (index.total[key] !== 0) {
          exId = index.exchange.split('.')[0];
          return [key, index.total[key],exId];
        }
      });
      
      if (index.exchange == "Bittrex.balance"){
        firstCell = "!A1";
      } else if (index.exchange == "Kraken.balance"){
        firstCell = "!E1";
      } else if (index.exchange == "Binance.balance"){
        firstCell = "!I1";
      }
        arr.unshift(["currency","balance","exchange"]);
        data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
    }
  }
  
  if (dataType == 'Balance_Historic'){
    let headersSet = new Set();
    let notUsedKeys = ['timestamp','datetime','_id']
    let headers = [];
    
    for (let hBalance of values){
      for (let key of Object.keys(hBalance)){
        if (notUsedKeys.indexOf(key) == -1){
          headersSet.add(key)
        }
      }
      headers = Array.from(headersSet)
      headers.sort()
    }
    headers.unshift("Date");
    arr.push(headers);
    let newHBalanceEntry = new Array(headers.length).fill("")
    for (let hBalance of values){
      date = new Date(hBalance.datetime);
      date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
      for (let key of Object.keys(hBalance)){
        if (notUsedKeys.indexOf(key) == -1){
          // retrieve the normalized balance of the current currency
          newHBalanceEntry.splice(headers.indexOf(key),1,hBalance[key].normalized);
        }
      }
      newHBalanceEntry.splice(0,1,date);

      arr.push([...newHBalanceEntry]);

    }
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'Order'){
    for (var order of values){
      exId = order.exchange
      var feecost = "",feecurrency = "";
      date = new Date(order.timestamp);
      date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
      if (order.fee){
        feecost = order.fee.cost;
        feecurrency = order.fee.currency;
      }
      arr.push([order.id,date,order.type,order.side,order.status,order.symbol,order.amount,order.price,feecost,feecurrency,exId]);
    }
    arr.unshift(["id","datetime","type","side","status","symbol","amount","price","fee_cost","fee_currency","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'Ticker'){
    for (var ticker of values){
      exId = ticker.exchange        
      if (ticker.symbol !== null && (ticker.symbol.split('/')[1] == 'BTC' || ticker.symbol.split('/')[0] == 'BTC')){
        date = new Date(ticker.datetime);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        arr.push([ticker.symbol,date,ticker.last,ticker.change,ticker.percentage,exId]);
      } else if (ticker.symbol !== null && (ticker.symbol.split('/')[1] == 'USDT' || ticker.symbol.split('/')[0] == 'USDT')) {
        date = new Date(ticker.datetime);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        arr.push([ticker.symbol,date,ticker.last,ticker.change,ticker.percentage,exId]);
      } else {continue}
    }
    arr.unshift(["symbol","datetime","last","change","percentage","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }

   
  if (dataType == 'Trade'){
    for (var trade of values){
      exId = trade.exchange;  
        var tfeecost = "",tfeecurrency = "";
        date = new Date(trade.datetime);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        if (trade.fee){
          tfeecost = trade.fee.cost;
          tfeecurrency = trade.fee.currency;
        }
        arr.push([trade.id,date,trade.type,trade.side,trade.cost,trade.symbol,trade.amount,trade.price,tfeecost,tfeecurrency,exId
                  // Add a logo to a cell in GSheet: ,'=IMAGE("' + urloflogo + '",1)'
                 ]); 
    }
    arr.unshift(["id","datetime","type","side","cost","symbol","amount","price","fee_cost","fee_currency","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }

  if (dataType == 'Transaction'){
    for (var transaction of values){
      
        exId = transaction.exchange;  
        date = new Date(transaction.datetime);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        arr.push([date,
                  transaction.type,
                  transaction.currency,
                  transaction.amount,
                  transaction.fee.cost,
                  exId
                 ]); 
    }
    arr.unshift(["datetime","type","currency","amount","fee","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'Unified_Trades'){
    for (var uTrade of values){
      
        exId = uTrade.exchange;  
        date = new Date(uTrade.datetime);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        arr.push([uTrade.timestamp,
                  date,
                  uTrade.type,
                  uTrade.buy_currency,
                  uTrade.buy_amount,
                  uTrade.sell_currency,
                  uTrade.sell_amount,
                  uTrade.fee_currency,
                  uTrade.fee_amount,
                  uTrade.market,
                  uTrade.price,
                  exId
                 ]); 
    }
    arr.unshift(["timestamp","Date","Type","Currency","Buy","Currency","Sell","Currency","Fee","Market","Price","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'OHLC'){
    for (let document of values){
      date = new Date();
      date.setFullYear(document._id.year);
      date.setMonth(document._id.month - 1);
      date.setDate(document._id.day);
      date.setHours(document._id.hour);
      date.setMinutes(0);
      date.setSeconds(0);
      
      date = date.toLocaleDateString()  + ' ' + date.toLocaleTimeString();

      arr.push([date, document.open, document.high, document.low, document.close, document.volume
                  // Add a logo to a cell in GSheet: ,'=IMAGE("' + urloflogo + '",1)'
                 ]); 
    }
    arr.unshift(["date","open","high","low","close","volume"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'Arbitrages'){
    arr = [];
    firstCell = "!A1";
    for (let document of values){
      
      let percentages = document.percentage;
      if(document.percentage.Bittrex_to_Binance !== undefined && document.percentage.Binance_to_Bittrex !== undefined) {
        arr.push([document.market, 
                  document.percentage.Bittrex_to_Binance, 
                  document.percentage.Binance_to_Bittrex 
                 ]);
      }
    }
    arr.unshift(["market","Bittrex->Binance","Binance->Bittrex"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
    
    firstCell = "!E1";
    arr = [];
    for (let document of values){

      let percentages = document.percentage;
      if(document.percentage.Bittrex_to_Kraken !== undefined && document.percentage.Kraken_to_Bittrex !== undefined) {
        arr.push([document.market, 
                  document.percentage.Bittrex_to_Kraken,
                  document.percentage.Kraken_to_Bittrex
                 ]);
      }
    }
    arr.unshift(["market","Bittrex->Kraken","Kraken->Bittrex"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
    
    firstCell = "!I1";
    arr = [];
    for (let document of values){

      let percentages = document.percentage;
      if(document.percentage.Kraken_to_Binance !== undefined && document.percentage.Binance_to_Kraken !== undefined) {
        arr.push([document.market, 
                  document.percentage.Kraken_to_Binance,
                  document.percentage.Binance_to_Kraken
                 ]);
      }
    }
    arr.unshift(["market","Kraken->Binance","Binance->Kraken"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});

    let requests = []
    requests.push({
      "sortRange": {
        "range": {
          "sheetId": 1305390196,
          "startRowIndex": 0,
          "endRowIndex": 1000,
          "startColumnIndex": 0,
          "endColumnIndex": 10
        },
        "sortSpecs": [
          {
            "dimensionIndex": 1,
            "sortOrder": "DESCENDING"
          }
//           {
//             "dimensionIndex": 5,
//             "sortOrder": "DESCENDING"
//           },
//           {
//             "dimensionIndex": 9,
//             "sortOrder": "DESCENDING"
//           }
        ]
      }})
  
//  sheet.spreadsheets.batchUpdate({
//         spreadsheetId,
//         resource: {requests}
//       }, function(err, response) {
//         if (err) {
//           console.log(err);
//         }
//         else  {
//           console.log('Arbitrages sheet sorted')
//         }
//       });
}
   let resource = {
    "valueInputOption": "USER_ENTERED",
    "data": data,
  }
   
   return new Promise(async function(resolve, reject) {
      await sheet.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource
      }, function(err, response) {
        if (err) {
          reject(err);
        }
        else  {
          resolve(console.log(dataType + ' GSheet cells updated: ' + response.data.totalUpdatedCells))
        }
      });
    }
  )
}




if (module === require.main) {
  getGSheetKeys().catch(console.error);
  getGSheetBinanceMarkets().catch(console.error);
  updateGSheetData().catch(console.error);
}


module.exports.getGSheetKeys = getGSheetKeys
module.exports.getGSheetBinanceMarkets = getGSheetBinanceMarkets
module.exports.updateGSheetData = updateGSheetData