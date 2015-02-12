'use strict';

/**
 * Module dependencies.
 */

var Client = require('mongodb').MongoClient
  , uri = require('mongodb-uri')
  , thunky = require('thunky')
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
        options.hosts = options.hosts || [{ 
          port: options.port, 
          host: options.host 
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

  store.ready(function ready(err, db){
    if (err) return fn(err);
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
