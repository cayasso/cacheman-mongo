'use strict';

/**
 * Module dependencies.
 */

var Client = require('mongodb').MongoClient
  , uri = require('mongodb-uri')
  , thunky = require('thunky')
  , zlib = require('zlib')
  , noop = function () {};

/**
 * Export `MongoStore`.
 */

module.exports = MongoStore;

/**
 * MongoStore constructor.
 *
 * @param {Object} options
 * @param {Bucket} bucket
 * @api public
 */

function MongoStore(conn, options) {
  
  if (!(this instanceof MongoStore)) return new MongoStore(conn, options);

  var store = this;

  if ('object' === typeof conn) {
    if ('function' !== typeof conn.collection) {
      options = conn;
      if (Object.keys(options).length === 0) {
        conn = null;
      } else if (options.client) {
        store.client = options.client
      } else {
        options.database = options.database || options.db;
        options.hosts = options.hosts || [{ 
          port: options.port || 27017,
          host: options.host || '127.0.0.1' 
        }];
        conn = uri.format(options);
      }
    } else {
      store.client = conn;
    }
  }

  conn = conn || 'mongodb://127.0.0.1:27017';
  options = options || {};
  store.coll = options.collection || 'cacheman';
  store.compression = options.compression || false;
  store.ready = thunky(function ready(cb) {
    if ('string' === typeof conn) {
      Client.connect(conn, options, function getDb(err, db) {
        if (err) return cb(err);
        cb(null, store.client = db);
      });
    } else {
      if (store.client) return cb(null, store.client);
      cb(new Error('Invalid mongo connection.'));
    }
  });
}

/**
 * Get an entry.
 *
 * @param {String} key
 * @param {Function} fn
 * @api public
 */

MongoStore.prototype.get = function get(key, fn) {
  var store = this;
  fn = fn || noop;
  store.ready(function ready(err, db) {
    if (err) return fn(err);
    db.collection(store.coll).findOne({ key: key }, function findOne(err, data) {
      if (err) return fn(err);
      if (!data) return fn(null, null);
      if (data.expire < Date.now()) {
        store.del(key);
        return fn(null, null);
      }
      try {
        if (data.compressed) return decompress(data.value, fn);
        fn(null, data.value);
      } catch (err) {
        fn(err);
      }
    });
  });
};

/**
 * Set an entry.
 *
 * @param {String} key
 * @param {Mixed} val
 * @param {Number} ttl
 * @param {Function} fn
 * @api public
 */

MongoStore.prototype.set = function set(key, val, ttl, fn) {
  
  if ('function' === typeof ttl) {
    fn = ttl;
    ttl = null;
  }

  fn = fn || noop;

  var data
    , store = this
    , query = { key: key }
    , options = { upsert: true, safe: true };

  try {
    data = {
      key: key,
      value: val,
      expire: Date.now() + ((ttl || 60) * 1000)
    };
  } catch (err) {
    return fn(err);
  }

  store.ready(function ready(err, db){
    if (err) return fn(err);
    if (!store.compression) {
      update(data);
    } else {
      compress(data, function compressData(err, data) {
        if (err) return fn(err);
        update(data);
      });
    }
    function update(data) {
      db.collection(store.coll).update(query, data, options, function _update(err, data) {
        if (err) return fn(err);
        if (!data) return fn(null, null);
        fn(null, val);
      });
    }
  });
};

/**
 * Delete an entry.
 *
 * @param {String} key
 * @param {Function} fn
 * @api public
 */

MongoStore.prototype.del = function del(key, fn) {
  var store = this;
  fn = fn || noop;
  this.ready(function ready(err, db){
    if (err) return fn(err);
    db.collection(store.coll).remove({ key: key }, { safe: true }, fn);
  });
};

/**
 * Clear all entries for this bucket.
 *
 * @param {String} key
 * @param {Function} fn
 * @api public
 */

MongoStore.prototype.clear = function clear(key, fn) {
  var store = this;

  if ('function' === typeof key) {
    fn = key;
    key = null;
  }

  fn = fn || noop;
  store.ready(function ready(err, db){
    if (err) return fn(err);
    db.collection(store.coll).remove({}, { safe: true }, fn);
  });
};

/**
 * Non-exported Helpers
 */

/**
 * Compress data value.
 *
 * @param {Object} data
 * @param {Function} fn
 * @api public
 */

function compress(data, fn) {

  // Data is not of a "compressable" type (currently only Buffer)
  if (!Buffer.isBuffer(data.value)) return fn(null, data);

  zlib.gzip(data.value, function(err, compressedvalue){
    // If compression was successful, then use the compressed data.
    // Otherwise, save the original data.
    if (!err) {
      data.value = compressedvalue;
      data.compressed = true;
    }

    fn(err, data);
  });
};

/**
 * Decompress data value.
 *
 * @param {Object} value
 * @param {Function} fn
 * @api public
 */

function decompress(value, fn){
  var v = (value.buffer && Buffer.isBuffer(value.buffer)) ? value.buffer : value;
  zlib.gunzip(v, fn);
};
