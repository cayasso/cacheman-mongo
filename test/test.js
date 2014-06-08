var assert = require('assert')
  , fs = require('fs')
  , crypto = require('crypto')
  , Cache = require('../')
  , cache;

describe('cacheman-mongo', function () {

  before(function(done){
    cache = new Cache({}, {});
    done();
  });

  after(function(done){
    cache.clear('test');
    done();
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
    cache.set('test1', { a: 1 }, 1, function (err) {
      if (err) return done(err);
      setTimeout(function () {
        cache.get('test1', function (err, data) {
        if (err) return done(err);
          assert.equal(data, null);
          done();
        });
      }, 1100);
    });
  });

});

describe('cacheman-mongo compression', function () {

  before(function(done){
    cache = new Cache({ compression: true }, {});
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