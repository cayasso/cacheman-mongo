'use strict';

/**
 * Module dependencies.
 */

var mongodb = require('mongodb')
  , Promise = require('./promise')
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

function MongoStore(options, bucket) {
  
  options = options || {};
  this.bucket = bucket || {};
  this.coll = options.collection || this.bucket._name || 'cacheman';
  this.collection = null;
  this.connected = false;

  var store = this
    , db = options.db || 'cache'
    , port = options.port || 27017
    , host = options.host || '127.0.0.1'
    , poolSize = options.poolSize
    , server = new mongodb.Server(host, port, { auto_reconnect: true, poolSize: poolSize })
    , client = new mongodb.Db(db, server, { safe: true });
   
  function open(fn) {
    client.open(function open(err, client) {
      if (err) throw err;
      if (options.username) {
        client.authenticate(options.username, options.password, function auth(err, res) {
          if (err) {
            client.close();
            throw err;
          }
          if (!res) {
            client.close();
            throw new Error('Unable to authenticate');
          }
        });
      }
      fn(null, client);
    });
  }

  store.conn = new Promise();
  open(store.conn.resolve.bind(store.conn));
  this.client = client;
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
  store.conn.then(function then(err, db){
    db.collection(store.coll).findOne({ key: key }, function findOne(err, data) {
      if (err) return fn(err);
      if (!data) return fn(null, null);
      if (data.expire < Date.now()) {
        store.del(key);
        return fn(null, null);
      }
      try {
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
    fn(err);
  }

  store.conn.then(function then(err, db){
    db.collection(store.coll).update(query, data, options, function update(err, data) {
      if (err) return fn(err);
      if (!data) return fn(null, null);
      fn(null, val);
    });
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
  store.conn.then(function then(err, db){
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
  store.conn.then(function then(err, db){
    db.collection(store.coll).remove({}, { safe: true }, fn);
  });
};
