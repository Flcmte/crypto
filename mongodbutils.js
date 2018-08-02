const MongoClient = require('mongodb').MongoClient
const uri = 'mongodb://admin:Chimie%2059!@127.0.0.1:27017/crypto?authSource=admin'
let _db

const connectDB = async (callback) => {
  try {
    MongoClient.connect(uri,{ useNewUrlParser: true }, (err, db) => {
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
