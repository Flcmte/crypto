const MongoClient = require('mongodb').MongoClient
const uri = 'mongodb://<uri to mongodb>'
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
