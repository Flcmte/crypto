const fs = require('fs');
const {
  google
} = require('googleapis');
const path = require('path');

let spreadsheetId = '1DcjxfNBnnMfqAUAFTh2hcqRX3YsVDWX_llGj73gciaM';


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
  var index,exchange,exId,order,ticker;
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
  
  if (dataType == 'Order'){
    for (exchange of values){
      exId = exchange._id.split('.')[0];  
      delete exchange._id
      for (order of Object.keys(exchange)) {
        var or = exchange[order];
        var feecost = "",feecurrency = "";
        date = new Date(order.timestamp);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        if (or.fee){
          feecost = or.fee.cost;
          feecurrency = or.fee.currency;
        }
        arr.push([or.id,date,or.type,or.side,or.status,or.symbol,or.amount,or.price,feecost,feecurrency,exId]);
      }  
    }
    arr.unshift(["id","datetime","type","side","status","symbol","amount","price","fee_cost","fee_currency","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }
  
  if (dataType == 'Ticker'){
    for (exchange of values){
      exId = exchange._id.split('.')[0];
      delete exchange._id
      for (ticker of Object.keys(exchange)) {
        var tick = exchange[ticker];
        if (tick.symbol.split('/')[1] == 'BTC' || tick.symbol.split('/')[0] == 'BTC'){
          date = new Date(tick.timestamp);
          date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
          arr.push([tick.symbol,date,tick.last,tick.change,tick.percentage,exId]);
        }
      }
    }
    arr.unshift(["symbol","datetime","last","change","percentage","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }

   let resource = {
    "valueInputOption": "USER_ENTERED",
    "data": data
  }
   
  if (dataType == 'Trade'){
    for (exchange of values){
      exId = exchange._id.split('.')[0];  
      delete exchange._id
      for (order of Object.keys(exchange)) {
        var tra = exchange[order];
        var tfeecost = "",tfeecurrency = "";
        date = new Date(order.timestamp);
        date = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
        if (tra.fee){
          tfeecost = tra.fee.cost;
          tfeecurrency = tra.fee.currency;
        }
        arr.push([tra.id,date,tra.type,tra.side,tra.cost,tra.symbol,tra.amount,tra.price,tfeecost,tfeecurrency,exId]);
      }  
    }
    arr.unshift(["id","datetime","type","side","cost","symbol","amount","price","fee_cost","fee_currency","exchange"])
    data.push({range: sheetName + firstCell , majorDimension: majorDimension , values: arr});
  }

   return new Promise(function(resolve, reject) {

      sheet.spreadsheets.values.batchUpdate({
        spreadsheetId,
        resource
      }, function(err, response) {
        if (err) {
          reject(err);
        }
        else  {
          resolve(console.log('Total GSheet cells updated: ' + response.data.totalUpdatedCells))

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