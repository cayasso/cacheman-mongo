var MongoClient = require('mongodb').MongoClient
  , assert = require('assert')
  , crypto = require('crypto')
  , Cache = require('../')
  , fs = require('fs')
  , uri = 'mongodb://127.0.0.1:27017/testing'
  , cache;

describe('cacheman-mongo', function () {

  before(function(done){
    cache = new Cache({ host: '127.0.0.1', port: 27017, database: 'testing' });
    done();
  });

  after(function(done){
    cache.clear('test');
    cache.client.close(done);
  });

  it('should have main methods', function () {
    assert.ok(cache.set);
    assert.ok(cache.get);
    assert.ok(cache.del);
    assert.ok(cache.clear);
  });

  it('should store items', function (done) {
    cache.set('test1', { a: 1 }, function (err) {
      if (err) return done(err);
      cache.get('test1', function (err, data) {
        if (err) return done(err);
        assert.equal(data.a, 1);
        done();
      });
    });
  });

  it('should store zero', function (done) {
    cache.set('test2', 0, function (err) {
      if (err) return done(err);
      cache.get('test2', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, 0);
        done();
      });
    });
  });

  it('should store false', function (done) {
    cache.set('test3', false, function (err) {
      if (err) return done(err);
      cache.get('test3', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, false);
        done();
      });
    });
  });

  it('should store null', function (done) {
    cache.set('test4', null, function (err) {
      if (err) return done(err);
      cache.get('test4', function (err, data) {
        if (err) return done(err);
        assert.strictEqual(data, null);
        done();
      });
    });
  });

  it('should delete items', function (done) {
    var value = Date.now();
    cache.set('test5', value, function (err) {
      if (err) return done(err);
      cache.get('test5', function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.del('test5', function (err) {
          if (err) return done(err);
          cache.get('test5', function (err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it('should clear items', function (done) {
    var value = Date.now();
    cache.set('test6', value, function (err) {
      if (err) return done(err);
      cache.get('test6', function (err, data) {
        if (err) return done(err);
        assert.equal(data, value);
        cache.clear('', function (err) {
          if (err) return done(err);
          cache.get('test6', function (err, data) {
            if (err) return done(err);
            assert.equal(data, null);
            done();
          });
        });
      });
    });
  });

  it('should expire key', function (done) {
    this.timeout(0);
    cache.set('test7', { a: 1 }, 1, function (err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get('test7', function (err, data) {
        if (err) return done(err);
          assert.equal(data, null);
          done();
        });
      }, 1100);
    });
  });

  it('should allow passing mongodb connection string', function (done) {
    cache = new Cache(uri);
    cache.set('test8', { a: 1 }, function (err) {
      if (err) return done(err);
      cache.get('test8', function (err, data) {
        if (err) return done(err);
        assert.equal(data.a, 1);
        done();
      });
    });
  });

  it('should allow passing mongo db instance as first argument', function (done) {
    MongoClient.connect(uri, function (err, db) {
      if (err) return done(err);
      cache = new Cache(db);
      cache.set('test9', { a: 1 }, function (err) {
        if (err) return done(err);
        cache.get('test9', function (err, data) {
          if (err) return done(err);
          assert.equal(data.a, 1);
          done();
        });
      });
    });
  });

  it('should sllow passing mongo db instance as client in object', function (done) {
    MongoClient.connect(uri, function (err, db) {
      if (err) return done(err);
      cache = new Cache({ client: db });
      cache.set('test9', { a: 1 }, function (err) {
        if (err) return done(err);
        cache.get('test9', function (err, data) {
          if (err) return done(err);
          assert.equal(data.a, 1);
          done();
        });
      });
    });
  });

  describe('cacheman-mongo compression', function () {

    before(function(done){
      cache = new Cache({ compression: true });
      done();
    });

    after(function(done){
      cache.clear('test');
      done();
    });

    it('should store compressable item compressed', function (done) {
      var value = Date.now().toString();

      cache.set('test1', new Buffer(value), function (err) {
        if (err) return done(err);
        cache.get('test1', function (err, data) {
          if (err) return done(err);
          assert.equal(data.toString(), value);
          done();
        });
      });
    });

    it('should store non-compressable item normally', function (done) {
      var value = Date.now().toString();

      cache.set('test1', value, function (err) {
        if (err) return done(err);
        cache.get('test1', function (err, data) {
          if (err) return done(err);
          assert.equal(data, value);
          done();
        });
      });
    });

    it('should store large compressable item compressed', function (done) {
      var value = fs.readFileSync('./test/large.bin'), // A file larger than the 16mb MongoDB document size limit
          md5 = function(d){ return crypto.createHash('md5').update(d).digest('hex'); };

      cache.set('test1', value, function (err) {
        if (err) return done(err);
        cache.get('test1', function (err, data) {
          if (err) return done(err);
          assert.equal(md5(data), md5(value));
          done();
        });
      });
    });
  });
});