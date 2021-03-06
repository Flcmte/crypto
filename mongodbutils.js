const MongoClient = require('mongodb').MongoClient

/*
require json file with MongoDB connection string, such as: 
{"uri": "mongodb://MongoUser:UserPassword@MongoDBLocalIP:localPort/DatabaseName?authSource=AuthDatabase"}
*/
const uri = require('./MongoDBconf')

let _db

const connectDB = async (callback) => {
  try {
    MongoClient.connect(uri.uri,{ useNewUrlParser: true }, (err, db) => {
      _db = db.db('crypto')
      return callback(err)
    })
  } catch (e) {
    throw e
  }
}

const getDB = () => _db


module.exports = {
  connectDB,
  getDB
}
