/*!
 * Connect - Redis
 * Copyright(c) 2012 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var debug = require('debug')('connect:redis');

/**
 * One day in seconds.
 */

var oneDay = 86400;

/**
 * Return the `RedisStore` extending `connect`'s session Store.
 *
 * @param {object} connect
 * @return {Function}
 * @api public
 */
var debugLog = console.log;

module.exports = function(connect){

  /**
   * Connect's Store.
   */

  var Store = connect.session.Store;

  /**
   * Initialize RedisStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function RedisStore(options) {
    var self = this;

    options = options || {};
    Store.call(this, options);
    this.prefix = null == options.prefix
      ? 'sess:'
      : options.prefix;

    if (options.url) {
      var url = require('url').parse(options.url);
      if (url.protocol === 'redis:') {
        if (url.auth) {
          var userparts = url.auth.split(":");
          options.user = userparts[0];
          if (userparts.length === 2) {
            options.pass = userparts[1];
          }
        }
        options.host = url.hostname;
        options.port = url.port;
        if (url.pathname) {
          options.db   = url.pathname.replace("/", "", 1);
        }
      }
    }

    this.client = options.client || new require('redis').createClient(options.port || options.socket, options.host, options);

    if (options.pass) {
      this.client.auth(options.pass, function(err){
        if (err) throw err;
      });    
    }

    this.ttl =  options.ttl;

    if (options.db) {
      self.client.select(options.db);
      self.client.on("connect", function() {
        self.client.send_anyways = true;
        self.client.select(options.db);
        self.client.send_anyways = false;
      });
    }

    self.client.on('error', function () { self.emit('disconnect'); });
    self.client.on('connect', function () { self.emit('connect'); });
  }

  /**
   * Inherit from `Store`.
   */

  RedisStore.prototype.__proto__ = Store.prototype;

  RedisStore.prototype._getUidKey = function(uid) {
    return this.prefix + 'user_sessions:' + uid;
  };

  RedisStore.prototype._dropPrefix = function(sid) {
    return sid.replace(this.prefix, '');
  };

  RedisStore.prototype.allUserSessions = function(uid, cb) {
    var _this = this;
    this.client.smembers(this._getUidKey(uid), function(err, sessions) {
      if (err) {
        console.log(err);
        cb(err);
        return;
      } else {
        var mc = sessions.map(function(sid) {
          return ['get', sid];
        });
        debugLog('sessions', sessions);

        _this.client.multi(mc).exec(function(err, resp) { 
          /**
           * If we retrieve null from Redis, it means this session was destroyed.
           * In that case remove it from the set to speed up future queries.
           */
          var parsed = [];
          resp.forEach(function(data, i) {
            var added = JSON.parse(data);
            // if added is null, we deleted this session, if added.passport is 0, then we're logged out 
            if (added === null || added.passport.user === undefined) {
              var deleteSid = sessions[i];
              debugLog('not found', deleteSid);
              _this.client.srem(_this._getUidKey(uid), deleteSid, function(err, response) {
                debugLog('srem:', err, response);
              });
            } else {
              added.sid = _this._dropPrefix(sessions[i]);
              parsed.push(added);
            }
          });

          console.log(parsed);
          cb(parsed);
        });
      }
    });
  };

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.get = function(sid, fn){
    sid = this.prefix + sid;
    debug('GET "%s"', sid);
    this.client.get(sid, function(err, data){
      if (err) return fn(err);
      if (!data) return fn();
      var result;
      data = data.toString();
      debug('GOT %s', data);
      try {
        result = JSON.parse(data); 
      } catch (err) {
        return fn(err);
      }
      return fn(null, result);
    });
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.set = function(sid, sess, fn){
    sid = this.prefix + sid;
    try {

      if (sess.passport && sess.passport.user) {
        this.client.sadd(this._getUidKey(sess.passport.user), sid);
      }

      var maxAge = sess.cookie.maxAge
        , ttl = this.ttl
        , sess = JSON.stringify(sess);

      ttl = ttl || ('number' == typeof maxAge
          ? maxAge / 1000 | 0
          : oneDay);

      debug('SETEX "%s" ttl:%s %s', sid, ttl, sess);

      this.client.setex(sid, ttl, sess, function(err){
        err || debug('SETEX complete');
        fn && fn.apply(this, arguments);
      });
    } catch (err) {
      fn && fn(err);
    } 
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  RedisStore.prototype.destroy = function(sid, fn){
    debugLog('destroy sid', sid);
    sid = this.prefix + sid;
    this.client.del(sid, fn);
  };

  return RedisStore;
};
