(function(){function r(e,n,t){function o(i,f){if(!n[i]){if(!e[i]){var c="function"==typeof require&&require;if(!f&&c)return c(i,!0);if(u)return u(i,!0);var a=new Error("Cannot find module '"+i+"'");throw a.code="MODULE_NOT_FOUND",a}var p=n[i]={exports:{}};e[i][0].call(p.exports,function(r){var n=e[i][1][r];return o(n||r)},p,p.exports,r,e,n,t)}return n[i].exports}for(var u="function"==typeof require&&require,i=0;i<t.length;i++)o(t[i]);return o}return r})()({1:[function(require,module,exports){
/**
 * cuid.js
 * Collision-resistant UID generator for browsers and node.
 * Sequential for fast db lookups and recency sorting.
 * Safe for element IDs and server-side lookups.
 *
 * Extracted from CLCTR
 *
 * Copyright (c) Eric Elliott 2012
 * MIT License
 */

var fingerprint = require('./lib/fingerprint.js');
var pad = require('./lib/pad.js');
var getRandomValue = require('./lib/getRandomValue.js');

var c = 0,
  blockSize = 4,
  base = 36,
  discreteValues = Math.pow(base, blockSize);

function randomBlock () {
  return pad((getRandomValue() *
    discreteValues << 0)
    .toString(base), blockSize);
}

function safeCounter () {
  c = c < discreteValues ? c : 0;
  c++; // this is not subliminal
  return c - 1;
}

function cuid () {
  // Starting with a lowercase letter makes
  // it HTML element ID friendly.
  var letter = 'c', // hard-coded allows for sequential access

    // timestamp
    // warning: this exposes the exact date and time
    // that the uid was created.
    timestamp = (new Date().getTime()).toString(base),

    // Prevent same-machine collisions.
    counter = pad(safeCounter().toString(base), blockSize),

    // A few chars to generate distinct ids for different
    // clients (so different computers are far less
    // likely to generate the same id)
    print = fingerprint(),

    // Grab some more chars from Math.random()
    random = randomBlock() + randomBlock();

  return letter + timestamp + counter + print + random;
}

cuid.slug = function slug () {
  var date = new Date().getTime().toString(36),
    counter = safeCounter().toString(36).slice(-4),
    print = fingerprint().slice(0, 1) +
      fingerprint().slice(-1),
    random = randomBlock().slice(-2);

  return date.slice(-2) +
    counter + print + random;
};

cuid.isCuid = function isCuid (stringToCheck) {
  if (typeof stringToCheck !== 'string') return false;
  if (stringToCheck.startsWith('c')) return true;
  return false;
};

cuid.isSlug = function isSlug (stringToCheck) {
  if (typeof stringToCheck !== 'string') return false;
  var stringLength = stringToCheck.length;
  if (stringLength >= 7 && stringLength <= 10) return true;
  return false;
};

cuid.fingerprint = fingerprint;

module.exports = cuid;

},{"./lib/fingerprint.js":2,"./lib/getRandomValue.js":3,"./lib/pad.js":4}],2:[function(require,module,exports){
var pad = require('./pad.js');

var env = typeof window === 'object' ? window : self;
var globalCount = Object.keys(env).length;
var mimeTypesLength = navigator.mimeTypes ? navigator.mimeTypes.length : 0;
var clientId = pad((mimeTypesLength +
  navigator.userAgent.length).toString(36) +
  globalCount.toString(36), 4);

module.exports = function fingerprint () {
  return clientId;
};

},{"./pad.js":4}],3:[function(require,module,exports){

var getRandomValue;

var crypto = window.crypto || window.msCrypto;

if (crypto) {
    var lim = Math.pow(2, 32) - 1;
    getRandomValue = function () {
        return Math.abs(crypto.getRandomValues(new Uint32Array(1))[0] / lim);
    };
} else {
    getRandomValue = Math.random;
}

module.exports = getRandomValue;

},{}],4:[function(require,module,exports){
module.exports = function pad (num, size) {
  var s = '000000000' + num;
  return s.substr(s.length - size);
};

},{}],5:[function(require,module,exports){
// This file can be required in Browserify and Node.js for automatic polyfill
// To use it:  require('es6-promise/auto');
'use strict';
module.exports = require('./').polyfill();

},{"./":6}],6:[function(require,module,exports){
(function (process,global){
/*!
 * @overview es6-promise - a tiny implementation of Promises/A+.
 * @copyright Copyright (c) 2014 Yehuda Katz, Tom Dale, Stefan Penner and contributors (Conversion to ES6 API by Jake Archibald)
 * @license   Licensed under MIT license
 *            See https://raw.githubusercontent.com/stefanpenner/es6-promise/master/LICENSE
 * @version   v4.2.5+7f2b526d
 */

(function (global, factory) {
	typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
	typeof define === 'function' && define.amd ? define(factory) :
	(global.ES6Promise = factory());
}(this, (function () { 'use strict';

function objectOrFunction(x) {
  var type = typeof x;
  return x !== null && (type === 'object' || type === 'function');
}

function isFunction(x) {
  return typeof x === 'function';
}



var _isArray = void 0;
if (Array.isArray) {
  _isArray = Array.isArray;
} else {
  _isArray = function (x) {
    return Object.prototype.toString.call(x) === '[object Array]';
  };
}

var isArray = _isArray;

var len = 0;
var vertxNext = void 0;
var customSchedulerFn = void 0;

var asap = function asap(callback, arg) {
  queue[len] = callback;
  queue[len + 1] = arg;
  len += 2;
  if (len === 2) {
    // If len is 2, that means that we need to schedule an async flush.
    // If additional callbacks are queued before the queue is flushed, they
    // will be processed by this flush that we are scheduling.
    if (customSchedulerFn) {
      customSchedulerFn(flush);
    } else {
      scheduleFlush();
    }
  }
};

function setScheduler(scheduleFn) {
  customSchedulerFn = scheduleFn;
}

function setAsap(asapFn) {
  asap = asapFn;
}

var browserWindow = typeof window !== 'undefined' ? window : undefined;
var browserGlobal = browserWindow || {};
var BrowserMutationObserver = browserGlobal.MutationObserver || browserGlobal.WebKitMutationObserver;
var isNode = typeof self === 'undefined' && typeof process !== 'undefined' && {}.toString.call(process) === '[object process]';

// test for web worker but not in IE10
var isWorker = typeof Uint8ClampedArray !== 'undefined' && typeof importScripts !== 'undefined' && typeof MessageChannel !== 'undefined';

// node
function useNextTick() {
  // node version 0.10.x displays a deprecation warning when nextTick is used recursively
  // see https://github.com/cujojs/when/issues/410 for details
  return function () {
    return process.nextTick(flush);
  };
}

// vertx
function useVertxTimer() {
  if (typeof vertxNext !== 'undefined') {
    return function () {
      vertxNext(flush);
    };
  }

  return useSetTimeout();
}

function useMutationObserver() {
  var iterations = 0;
  var observer = new BrowserMutationObserver(flush);
  var node = document.createTextNode('');
  observer.observe(node, { characterData: true });

  return function () {
    node.data = iterations = ++iterations % 2;
  };
}

// web worker
function useMessageChannel() {
  var channel = new MessageChannel();
  channel.port1.onmessage = flush;
  return function () {
    return channel.port2.postMessage(0);
  };
}

function useSetTimeout() {
  // Store setTimeout reference so es6-promise will be unaffected by
  // other code modifying setTimeout (like sinon.useFakeTimers())
  var globalSetTimeout = setTimeout;
  return function () {
    return globalSetTimeout(flush, 1);
  };
}

var queue = new Array(1000);
function flush() {
  for (var i = 0; i < len; i += 2) {
    var callback = queue[i];
    var arg = queue[i + 1];

    callback(arg);

    queue[i] = undefined;
    queue[i + 1] = undefined;
  }

  len = 0;
}

function attemptVertx() {
  try {
    var vertx = Function('return this')().require('vertx');
    vertxNext = vertx.runOnLoop || vertx.runOnContext;
    return useVertxTimer();
  } catch (e) {
    return useSetTimeout();
  }
}

var scheduleFlush = void 0;
// Decide what async method to use to triggering processing of queued callbacks:
if (isNode) {
  scheduleFlush = useNextTick();
} else if (BrowserMutationObserver) {
  scheduleFlush = useMutationObserver();
} else if (isWorker) {
  scheduleFlush = useMessageChannel();
} else if (browserWindow === undefined && typeof require === 'function') {
  scheduleFlush = attemptVertx();
} else {
  scheduleFlush = useSetTimeout();
}

function then(onFulfillment, onRejection) {
  var parent = this;

  var child = new this.constructor(noop);

  if (child[PROMISE_ID] === undefined) {
    makePromise(child);
  }

  var _state = parent._state;


  if (_state) {
    var callback = arguments[_state - 1];
    asap(function () {
      return invokeCallback(_state, child, callback, parent._result);
    });
  } else {
    subscribe(parent, child, onFulfillment, onRejection);
  }

  return child;
}

/**
  `Promise.resolve` returns a promise that will become resolved with the
  passed `value`. It is shorthand for the following:

  ```javascript
  let promise = new Promise(function(resolve, reject){
    resolve(1);
  });

  promise.then(function(value){
    // value === 1
  });
  ```

  Instead of writing the above, your code now simply becomes the following:

  ```javascript
  let promise = Promise.resolve(1);

  promise.then(function(value){
    // value === 1
  });
  ```

  @method resolve
  @static
  @param {Any} value value that the returned promise will be resolved with
  Useful for tooling.
  @return {Promise} a promise that will become fulfilled with the given
  `value`
*/
function resolve$1(object) {
  /*jshint validthis:true */
  var Constructor = this;

  if (object && typeof object === 'object' && object.constructor === Constructor) {
    return object;
  }

  var promise = new Constructor(noop);
  resolve(promise, object);
  return promise;
}

var PROMISE_ID = Math.random().toString(36).substring(2);

function noop() {}

var PENDING = void 0;
var FULFILLED = 1;
var REJECTED = 2;

var TRY_CATCH_ERROR = { error: null };

function selfFulfillment() {
  return new TypeError("You cannot resolve a promise with itself");
}

function cannotReturnOwn() {
  return new TypeError('A promises callback cannot return that same promise.');
}

function getThen(promise) {
  try {
    return promise.then;
  } catch (error) {
    TRY_CATCH_ERROR.error = error;
    return TRY_CATCH_ERROR;
  }
}

function tryThen(then$$1, value, fulfillmentHandler, rejectionHandler) {
  try {
    then$$1.call(value, fulfillmentHandler, rejectionHandler);
  } catch (e) {
    return e;
  }
}

function handleForeignThenable(promise, thenable, then$$1) {
  asap(function (promise) {
    var sealed = false;
    var error = tryThen(then$$1, thenable, function (value) {
      if (sealed) {
        return;
      }
      sealed = true;
      if (thenable !== value) {
        resolve(promise, value);
      } else {
        fulfill(promise, value);
      }
    }, function (reason) {
      if (sealed) {
        return;
      }
      sealed = true;

      reject(promise, reason);
    }, 'Settle: ' + (promise._label || ' unknown promise'));

    if (!sealed && error) {
      sealed = true;
      reject(promise, error);
    }
  }, promise);
}

function handleOwnThenable(promise, thenable) {
  if (thenable._state === FULFILLED) {
    fulfill(promise, thenable._result);
  } else if (thenable._state === REJECTED) {
    reject(promise, thenable._result);
  } else {
    subscribe(thenable, undefined, function (value) {
      return resolve(promise, value);
    }, function (reason) {
      return reject(promise, reason);
    });
  }
}

function handleMaybeThenable(promise, maybeThenable, then$$1) {
  if (maybeThenable.constructor === promise.constructor && then$$1 === then && maybeThenable.constructor.resolve === resolve$1) {
    handleOwnThenable(promise, maybeThenable);
  } else {
    if (then$$1 === TRY_CATCH_ERROR) {
      reject(promise, TRY_CATCH_ERROR.error);
      TRY_CATCH_ERROR.error = null;
    } else if (then$$1 === undefined) {
      fulfill(promise, maybeThenable);
    } else if (isFunction(then$$1)) {
      handleForeignThenable(promise, maybeThenable, then$$1);
    } else {
      fulfill(promise, maybeThenable);
    }
  }
}

function resolve(promise, value) {
  if (promise === value) {
    reject(promise, selfFulfillment());
  } else if (objectOrFunction(value)) {
    handleMaybeThenable(promise, value, getThen(value));
  } else {
    fulfill(promise, value);
  }
}

function publishRejection(promise) {
  if (promise._onerror) {
    promise._onerror(promise._result);
  }

  publish(promise);
}

function fulfill(promise, value) {
  if (promise._state !== PENDING) {
    return;
  }

  promise._result = value;
  promise._state = FULFILLED;

  if (promise._subscribers.length !== 0) {
    asap(publish, promise);
  }
}

function reject(promise, reason) {
  if (promise._state !== PENDING) {
    return;
  }
  promise._state = REJECTED;
  promise._result = reason;

  asap(publishRejection, promise);
}

function subscribe(parent, child, onFulfillment, onRejection) {
  var _subscribers = parent._subscribers;
  var length = _subscribers.length;


  parent._onerror = null;

  _subscribers[length] = child;
  _subscribers[length + FULFILLED] = onFulfillment;
  _subscribers[length + REJECTED] = onRejection;

  if (length === 0 && parent._state) {
    asap(publish, parent);
  }
}

function publish(promise) {
  var subscribers = promise._subscribers;
  var settled = promise._state;

  if (subscribers.length === 0) {
    return;
  }

  var child = void 0,
      callback = void 0,
      detail = promise._result;

  for (var i = 0; i < subscribers.length; i += 3) {
    child = subscribers[i];
    callback = subscribers[i + settled];

    if (child) {
      invokeCallback(settled, child, callback, detail);
    } else {
      callback(detail);
    }
  }

  promise._subscribers.length = 0;
}

function tryCatch(callback, detail) {
  try {
    return callback(detail);
  } catch (e) {
    TRY_CATCH_ERROR.error = e;
    return TRY_CATCH_ERROR;
  }
}

function invokeCallback(settled, promise, callback, detail) {
  var hasCallback = isFunction(callback),
      value = void 0,
      error = void 0,
      succeeded = void 0,
      failed = void 0;

  if (hasCallback) {
    value = tryCatch(callback, detail);

    if (value === TRY_CATCH_ERROR) {
      failed = true;
      error = value.error;
      value.error = null;
    } else {
      succeeded = true;
    }

    if (promise === value) {
      reject(promise, cannotReturnOwn());
      return;
    }
  } else {
    value = detail;
    succeeded = true;
  }

  if (promise._state !== PENDING) {
    // noop
  } else if (hasCallback && succeeded) {
    resolve(promise, value);
  } else if (failed) {
    reject(promise, error);
  } else if (settled === FULFILLED) {
    fulfill(promise, value);
  } else if (settled === REJECTED) {
    reject(promise, value);
  }
}

function initializePromise(promise, resolver) {
  try {
    resolver(function resolvePromise(value) {
      resolve(promise, value);
    }, function rejectPromise(reason) {
      reject(promise, reason);
    });
  } catch (e) {
    reject(promise, e);
  }
}

var id = 0;
function nextId() {
  return id++;
}

function makePromise(promise) {
  promise[PROMISE_ID] = id++;
  promise._state = undefined;
  promise._result = undefined;
  promise._subscribers = [];
}

function validationError() {
  return new Error('Array Methods must be provided an Array');
}

var Enumerator = function () {
  function Enumerator(Constructor, input) {
    this._instanceConstructor = Constructor;
    this.promise = new Constructor(noop);

    if (!this.promise[PROMISE_ID]) {
      makePromise(this.promise);
    }

    if (isArray(input)) {
      this.length = input.length;
      this._remaining = input.length;

      this._result = new Array(this.length);

      if (this.length === 0) {
        fulfill(this.promise, this._result);
      } else {
        this.length = this.length || 0;
        this._enumerate(input);
        if (this._remaining === 0) {
          fulfill(this.promise, this._result);
        }
      }
    } else {
      reject(this.promise, validationError());
    }
  }

  Enumerator.prototype._enumerate = function _enumerate(input) {
    for (var i = 0; this._state === PENDING && i < input.length; i++) {
      this._eachEntry(input[i], i);
    }
  };

  Enumerator.prototype._eachEntry = function _eachEntry(entry, i) {
    var c = this._instanceConstructor;
    var resolve$$1 = c.resolve;


    if (resolve$$1 === resolve$1) {
      var _then = getThen(entry);

      if (_then === then && entry._state !== PENDING) {
        this._settledAt(entry._state, i, entry._result);
      } else if (typeof _then !== 'function') {
        this._remaining--;
        this._result[i] = entry;
      } else if (c === Promise$1) {
        var promise = new c(noop);
        handleMaybeThenable(promise, entry, _then);
        this._willSettleAt(promise, i);
      } else {
        this._willSettleAt(new c(function (resolve$$1) {
          return resolve$$1(entry);
        }), i);
      }
    } else {
      this._willSettleAt(resolve$$1(entry), i);
    }
  };

  Enumerator.prototype._settledAt = function _settledAt(state, i, value) {
    var promise = this.promise;


    if (promise._state === PENDING) {
      this._remaining--;

      if (state === REJECTED) {
        reject(promise, value);
      } else {
        this._result[i] = value;
      }
    }

    if (this._remaining === 0) {
      fulfill(promise, this._result);
    }
  };

  Enumerator.prototype._willSettleAt = function _willSettleAt(promise, i) {
    var enumerator = this;

    subscribe(promise, undefined, function (value) {
      return enumerator._settledAt(FULFILLED, i, value);
    }, function (reason) {
      return enumerator._settledAt(REJECTED, i, reason);
    });
  };

  return Enumerator;
}();

/**
  `Promise.all` accepts an array of promises, and returns a new promise which
  is fulfilled with an array of fulfillment values for the passed promises, or
  rejected with the reason of the first passed promise to be rejected. It casts all
  elements of the passed iterable to promises as it runs this algorithm.

  Example:

  ```javascript
  let promise1 = resolve(1);
  let promise2 = resolve(2);
  let promise3 = resolve(3);
  let promises = [ promise1, promise2, promise3 ];

  Promise.all(promises).then(function(array){
    // The array here would be [ 1, 2, 3 ];
  });
  ```

  If any of the `promises` given to `all` are rejected, the first promise
  that is rejected will be given as an argument to the returned promises's
  rejection handler. For example:

  Example:

  ```javascript
  let promise1 = resolve(1);
  let promise2 = reject(new Error("2"));
  let promise3 = reject(new Error("3"));
  let promises = [ promise1, promise2, promise3 ];

  Promise.all(promises).then(function(array){
    // Code here never runs because there are rejected promises!
  }, function(error) {
    // error.message === "2"
  });
  ```

  @method all
  @static
  @param {Array} entries array of promises
  @param {String} label optional string for labeling the promise.
  Useful for tooling.
  @return {Promise} promise that is fulfilled when all `promises` have been
  fulfilled, or rejected if any of them become rejected.
  @static
*/
function all(entries) {
  return new Enumerator(this, entries).promise;
}

/**
  `Promise.race` returns a new promise which is settled in the same way as the
  first passed promise to settle.

  Example:

  ```javascript
  let promise1 = new Promise(function(resolve, reject){
    setTimeout(function(){
      resolve('promise 1');
    }, 200);
  });

  let promise2 = new Promise(function(resolve, reject){
    setTimeout(function(){
      resolve('promise 2');
    }, 100);
  });

  Promise.race([promise1, promise2]).then(function(result){
    // result === 'promise 2' because it was resolved before promise1
    // was resolved.
  });
  ```

  `Promise.race` is deterministic in that only the state of the first
  settled promise matters. For example, even if other promises given to the
  `promises` array argument are resolved, but the first settled promise has
  become rejected before the other promises became fulfilled, the returned
  promise will become rejected:

  ```javascript
  let promise1 = new Promise(function(resolve, reject){
    setTimeout(function(){
      resolve('promise 1');
    }, 200);
  });

  let promise2 = new Promise(function(resolve, reject){
    setTimeout(function(){
      reject(new Error('promise 2'));
    }, 100);
  });

  Promise.race([promise1, promise2]).then(function(result){
    // Code here never runs
  }, function(reason){
    // reason.message === 'promise 2' because promise 2 became rejected before
    // promise 1 became fulfilled
  });
  ```

  An example real-world use case is implementing timeouts:

  ```javascript
  Promise.race([ajax('foo.json'), timeout(5000)])
  ```

  @method race
  @static
  @param {Array} promises array of promises to observe
  Useful for tooling.
  @return {Promise} a promise which settles in the same way as the first passed
  promise to settle.
*/
function race(entries) {
  /*jshint validthis:true */
  var Constructor = this;

  if (!isArray(entries)) {
    return new Constructor(function (_, reject) {
      return reject(new TypeError('You must pass an array to race.'));
    });
  } else {
    return new Constructor(function (resolve, reject) {
      var length = entries.length;
      for (var i = 0; i < length; i++) {
        Constructor.resolve(entries[i]).then(resolve, reject);
      }
    });
  }
}

/**
  `Promise.reject` returns a promise rejected with the passed `reason`.
  It is shorthand for the following:

  ```javascript
  let promise = new Promise(function(resolve, reject){
    reject(new Error('WHOOPS'));
  });

  promise.then(function(value){
    // Code here doesn't run because the promise is rejected!
  }, function(reason){
    // reason.message === 'WHOOPS'
  });
  ```

  Instead of writing the above, your code now simply becomes the following:

  ```javascript
  let promise = Promise.reject(new Error('WHOOPS'));

  promise.then(function(value){
    // Code here doesn't run because the promise is rejected!
  }, function(reason){
    // reason.message === 'WHOOPS'
  });
  ```

  @method reject
  @static
  @param {Any} reason value that the returned promise will be rejected with.
  Useful for tooling.
  @return {Promise} a promise rejected with the given `reason`.
*/
function reject$1(reason) {
  /*jshint validthis:true */
  var Constructor = this;
  var promise = new Constructor(noop);
  reject(promise, reason);
  return promise;
}

function needsResolver() {
  throw new TypeError('You must pass a resolver function as the first argument to the promise constructor');
}

function needsNew() {
  throw new TypeError("Failed to construct 'Promise': Please use the 'new' operator, this object constructor cannot be called as a function.");
}

/**
  Promise objects represent the eventual result of an asynchronous operation. The
  primary way of interacting with a promise is through its `then` method, which
  registers callbacks to receive either a promise's eventual value or the reason
  why the promise cannot be fulfilled.

  Terminology
  -----------

  - `promise` is an object or function with a `then` method whose behavior conforms to this specification.
  - `thenable` is an object or function that defines a `then` method.
  - `value` is any legal JavaScript value (including undefined, a thenable, or a promise).
  - `exception` is a value that is thrown using the throw statement.
  - `reason` is a value that indicates why a promise was rejected.
  - `settled` the final resting state of a promise, fulfilled or rejected.

  A promise can be in one of three states: pending, fulfilled, or rejected.

  Promises that are fulfilled have a fulfillment value and are in the fulfilled
  state.  Promises that are rejected have a rejection reason and are in the
  rejected state.  A fulfillment value is never a thenable.

  Promises can also be said to *resolve* a value.  If this value is also a
  promise, then the original promise's settled state will match the value's
  settled state.  So a promise that *resolves* a promise that rejects will
  itself reject, and a promise that *resolves* a promise that fulfills will
  itself fulfill.


  Basic Usage:
  ------------

  ```js
  let promise = new Promise(function(resolve, reject) {
    // on success
    resolve(value);

    // on failure
    reject(reason);
  });

  promise.then(function(value) {
    // on fulfillment
  }, function(reason) {
    // on rejection
  });
  ```

  Advanced Usage:
  ---------------

  Promises shine when abstracting away asynchronous interactions such as
  `XMLHttpRequest`s.

  ```js
  function getJSON(url) {
    return new Promise(function(resolve, reject){
      let xhr = new XMLHttpRequest();

      xhr.open('GET', url);
      xhr.onreadystatechange = handler;
      xhr.responseType = 'json';
      xhr.setRequestHeader('Accept', 'application/json');
      xhr.send();

      function handler() {
        if (this.readyState === this.DONE) {
          if (this.status === 200) {
            resolve(this.response);
          } else {
            reject(new Error('getJSON: `' + url + '` failed with status: [' + this.status + ']'));
          }
        }
      };
    });
  }

  getJSON('/posts.json').then(function(json) {
    // on fulfillment
  }, function(reason) {
    // on rejection
  });
  ```

  Unlike callbacks, promises are great composable primitives.

  ```js
  Promise.all([
    getJSON('/posts'),
    getJSON('/comments')
  ]).then(function(values){
    values[0] // => postsJSON
    values[1] // => commentsJSON

    return values;
  });
  ```

  @class Promise
  @param {Function} resolver
  Useful for tooling.
  @constructor
*/

var Promise$1 = function () {
  function Promise(resolver) {
    this[PROMISE_ID] = nextId();
    this._result = this._state = undefined;
    this._subscribers = [];

    if (noop !== resolver) {
      typeof resolver !== 'function' && needsResolver();
      this instanceof Promise ? initializePromise(this, resolver) : needsNew();
    }
  }

  /**
  The primary way of interacting with a promise is through its `then` method,
  which registers callbacks to receive either a promise's eventual value or the
  reason why the promise cannot be fulfilled.
   ```js
  findUser().then(function(user){
    // user is available
  }, function(reason){
    // user is unavailable, and you are given the reason why
  });
  ```
   Chaining
  --------
   The return value of `then` is itself a promise.  This second, 'downstream'
  promise is resolved with the return value of the first promise's fulfillment
  or rejection handler, or rejected if the handler throws an exception.
   ```js
  findUser().then(function (user) {
    return user.name;
  }, function (reason) {
    return 'default name';
  }).then(function (userName) {
    // If `findUser` fulfilled, `userName` will be the user's name, otherwise it
    // will be `'default name'`
  });
   findUser().then(function (user) {
    throw new Error('Found user, but still unhappy');
  }, function (reason) {
    throw new Error('`findUser` rejected and we're unhappy');
  }).then(function (value) {
    // never reached
  }, function (reason) {
    // if `findUser` fulfilled, `reason` will be 'Found user, but still unhappy'.
    // If `findUser` rejected, `reason` will be '`findUser` rejected and we're unhappy'.
  });
  ```
  If the downstream promise does not specify a rejection handler, rejection reasons will be propagated further downstream.
   ```js
  findUser().then(function (user) {
    throw new PedagogicalException('Upstream error');
  }).then(function (value) {
    // never reached
  }).then(function (value) {
    // never reached
  }, function (reason) {
    // The `PedgagocialException` is propagated all the way down to here
  });
  ```
   Assimilation
  ------------
   Sometimes the value you want to propagate to a downstream promise can only be
  retrieved asynchronously. This can be achieved by returning a promise in the
  fulfillment or rejection handler. The downstream promise will then be pending
  until the returned promise is settled. This is called *assimilation*.
   ```js
  findUser().then(function (user) {
    return findCommentsByAuthor(user);
  }).then(function (comments) {
    // The user's comments are now available
  });
  ```
   If the assimliated promise rejects, then the downstream promise will also reject.
   ```js
  findUser().then(function (user) {
    return findCommentsByAuthor(user);
  }).then(function (comments) {
    // If `findCommentsByAuthor` fulfills, we'll have the value here
  }, function (reason) {
    // If `findCommentsByAuthor` rejects, we'll have the reason here
  });
  ```
   Simple Example
  --------------
   Synchronous Example
   ```javascript
  let result;
   try {
    result = findResult();
    // success
  } catch(reason) {
    // failure
  }
  ```
   Errback Example
   ```js
  findResult(function(result, err){
    if (err) {
      // failure
    } else {
      // success
    }
  });
  ```
   Promise Example;
   ```javascript
  findResult().then(function(result){
    // success
  }, function(reason){
    // failure
  });
  ```
   Advanced Example
  --------------
   Synchronous Example
   ```javascript
  let author, books;
   try {
    author = findAuthor();
    books  = findBooksByAuthor(author);
    // success
  } catch(reason) {
    // failure
  }
  ```
   Errback Example
   ```js
   function foundBooks(books) {
   }
   function failure(reason) {
   }
   findAuthor(function(author, err){
    if (err) {
      failure(err);
      // failure
    } else {
      try {
        findBoooksByAuthor(author, function(books, err) {
          if (err) {
            failure(err);
          } else {
            try {
              foundBooks(books);
            } catch(reason) {
              failure(reason);
            }
          }
        });
      } catch(error) {
        failure(err);
      }
      // success
    }
  });
  ```
   Promise Example;
   ```javascript
  findAuthor().
    then(findBooksByAuthor).
    then(function(books){
      // found books
  }).catch(function(reason){
    // something went wrong
  });
  ```
   @method then
  @param {Function} onFulfilled
  @param {Function} onRejected
  Useful for tooling.
  @return {Promise}
  */

  /**
  `catch` is simply sugar for `then(undefined, onRejection)` which makes it the same
  as the catch block of a try/catch statement.
  ```js
  function findAuthor(){
  throw new Error('couldn't find that author');
  }
  // synchronous
  try {
  findAuthor();
  } catch(reason) {
  // something went wrong
  }
  // async with promises
  findAuthor().catch(function(reason){
  // something went wrong
  });
  ```
  @method catch
  @param {Function} onRejection
  Useful for tooling.
  @return {Promise}
  */


  Promise.prototype.catch = function _catch(onRejection) {
    return this.then(null, onRejection);
  };

  /**
    `finally` will be invoked regardless of the promise's fate just as native
    try/catch/finally behaves
  
    Synchronous example:
  
    ```js
    findAuthor() {
      if (Math.random() > 0.5) {
        throw new Error();
      }
      return new Author();
    }
  
    try {
      return findAuthor(); // succeed or fail
    } catch(error) {
      return findOtherAuther();
    } finally {
      // always runs
      // doesn't affect the return value
    }
    ```
  
    Asynchronous example:
  
    ```js
    findAuthor().catch(function(reason){
      return findOtherAuther();
    }).finally(function(){
      // author was either found, or not
    });
    ```
  
    @method finally
    @param {Function} callback
    @return {Promise}
  */


  Promise.prototype.finally = function _finally(callback) {
    var promise = this;
    var constructor = promise.constructor;

    if (isFunction(callback)) {
      return promise.then(function (value) {
        return constructor.resolve(callback()).then(function () {
          return value;
        });
      }, function (reason) {
        return constructor.resolve(callback()).then(function () {
          throw reason;
        });
      });
    }

    return promise.then(callback, callback);
  };

  return Promise;
}();

Promise$1.prototype.then = then;
Promise$1.all = all;
Promise$1.race = race;
Promise$1.resolve = resolve$1;
Promise$1.reject = reject$1;
Promise$1._setScheduler = setScheduler;
Promise$1._setAsap = setAsap;
Promise$1._asap = asap;

/*global self*/
function polyfill() {
  var local = void 0;

  if (typeof global !== 'undefined') {
    local = global;
  } else if (typeof self !== 'undefined') {
    local = self;
  } else {
    try {
      local = Function('return this')();
    } catch (e) {
      throw new Error('polyfill failed because global object is unavailable in this environment');
    }
  }

  var P = local.Promise;

  if (P) {
    var promiseToString = null;
    try {
      promiseToString = Object.prototype.toString.call(P.resolve());
    } catch (e) {
      // silently ignored
    }

    if (promiseToString === '[object Promise]' && !P.cast) {
      return;
    }
  }

  local.Promise = Promise$1;
}

// Strange compat..
Promise$1.polyfill = polyfill;
Promise$1.Promise = Promise$1;

return Promise$1;

})));





}).call(this,require('_process'),typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})

},{"_process":13}],7:[function(require,module,exports){
'use strict';

var hasOwn = Object.prototype.hasOwnProperty;
var toStr = Object.prototype.toString;
var defineProperty = Object.defineProperty;
var gOPD = Object.getOwnPropertyDescriptor;

var isArray = function isArray(arr) {
	if (typeof Array.isArray === 'function') {
		return Array.isArray(arr);
	}

	return toStr.call(arr) === '[object Array]';
};

var isPlainObject = function isPlainObject(obj) {
	if (!obj || toStr.call(obj) !== '[object Object]') {
		return false;
	}

	var hasOwnConstructor = hasOwn.call(obj, 'constructor');
	var hasIsPrototypeOf = obj.constructor && obj.constructor.prototype && hasOwn.call(obj.constructor.prototype, 'isPrototypeOf');
	// Not own constructor property must be Object
	if (obj.constructor && !hasOwnConstructor && !hasIsPrototypeOf) {
		return false;
	}

	// Own properties are enumerated firstly, so to speed up,
	// if last one is own, then all properties are own.
	var key;
	for (key in obj) { /**/ }

	return typeof key === 'undefined' || hasOwn.call(obj, key);
};

// If name is '__proto__', and Object.defineProperty is available, define __proto__ as an own property on target
var setProperty = function setProperty(target, options) {
	if (defineProperty && options.name === '__proto__') {
		defineProperty(target, options.name, {
			enumerable: true,
			configurable: true,
			value: options.newValue,
			writable: true
		});
	} else {
		target[options.name] = options.newValue;
	}
};

// Return undefined instead of __proto__ if '__proto__' is not an own property
var getProperty = function getProperty(obj, name) {
	if (name === '__proto__') {
		if (!hasOwn.call(obj, name)) {
			return void 0;
		} else if (gOPD) {
			// In early versions of node, obj['__proto__'] is buggy when obj has
			// __proto__ as an own property. Object.getOwnPropertyDescriptor() works.
			return gOPD(obj, name).value;
		}
	}

	return obj[name];
};

module.exports = function extend() {
	var options, name, src, copy, copyIsArray, clone;
	var target = arguments[0];
	var i = 1;
	var length = arguments.length;
	var deep = false;

	// Handle a deep copy situation
	if (typeof target === 'boolean') {
		deep = target;
		target = arguments[1] || {};
		// skip the boolean and the target
		i = 2;
	}
	if (target == null || (typeof target !== 'object' && typeof target !== 'function')) {
		target = {};
	}

	for (; i < length; ++i) {
		options = arguments[i];
		// Only deal with non-null/undefined values
		if (options != null) {
			// Extend the base object
			for (name in options) {
				src = getProperty(target, name);
				copy = getProperty(options, name);

				// Prevent never-ending loop
				if (target !== copy) {
					// Recurse if we're merging plain objects or arrays
					if (deep && copy && (isPlainObject(copy) || (copyIsArray = isArray(copy)))) {
						if (copyIsArray) {
							copyIsArray = false;
							clone = src && isArray(src) ? src : [];
						} else {
							clone = src && isPlainObject(src) ? src : {};
						}

						// Never move original objects, clone them
						setProperty(target, { name: name, newValue: extend(deep, clone, copy) });

					// Don't bring in undefined values
					} else if (typeof copy !== 'undefined') {
						setProperty(target, { name: name, newValue: copy });
					}
				}
			}
		}
	}

	// Return the modified object
	return target;
};

},{}],8:[function(require,module,exports){
(function (global){
/*
 *  base64.js
 *
 *  Licensed under the BSD 3-Clause License.
 *    http://opensource.org/licenses/BSD-3-Clause
 *
 *  References:
 *    http://en.wikipedia.org/wiki/Base64
 */
;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined'
        ? module.exports = factory(global)
        : typeof define === 'function' && define.amd
        ? define(factory) : factory(global)
}((
    typeof self !== 'undefined' ? self
        : typeof window !== 'undefined' ? window
        : typeof global !== 'undefined' ? global
: this
), function(global) {
    'use strict';
    // existing version for noConflict()
    global = global || {};
    var _Base64 = global.Base64;
    var version = "2.5.1";
    // if node.js and NOT React Native, we use Buffer
    var buffer;
    if (typeof module !== 'undefined' && module.exports) {
        try {
            buffer = eval("require('buffer').Buffer");
        } catch (err) {
            buffer = undefined;
        }
    }
    // constants
    var b64chars
        = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    var b64tab = function(bin) {
        var t = {};
        for (var i = 0, l = bin.length; i < l; i++) t[bin.charAt(i)] = i;
        return t;
    }(b64chars);
    var fromCharCode = String.fromCharCode;
    // encoder stuff
    var cb_utob = function(c) {
        if (c.length < 2) {
            var cc = c.charCodeAt(0);
            return cc < 0x80 ? c
                : cc < 0x800 ? (fromCharCode(0xc0 | (cc >>> 6))
                                + fromCharCode(0x80 | (cc & 0x3f)))
                : (fromCharCode(0xe0 | ((cc >>> 12) & 0x0f))
                   + fromCharCode(0x80 | ((cc >>>  6) & 0x3f))
                   + fromCharCode(0x80 | ( cc         & 0x3f)));
        } else {
            var cc = 0x10000
                + (c.charCodeAt(0) - 0xD800) * 0x400
                + (c.charCodeAt(1) - 0xDC00);
            return (fromCharCode(0xf0 | ((cc >>> 18) & 0x07))
                    + fromCharCode(0x80 | ((cc >>> 12) & 0x3f))
                    + fromCharCode(0x80 | ((cc >>>  6) & 0x3f))
                    + fromCharCode(0x80 | ( cc         & 0x3f)));
        }
    };
    var re_utob = /[\uD800-\uDBFF][\uDC00-\uDFFFF]|[^\x00-\x7F]/g;
    var utob = function(u) {
        return u.replace(re_utob, cb_utob);
    };
    var cb_encode = function(ccc) {
        var padlen = [0, 2, 1][ccc.length % 3],
        ord = ccc.charCodeAt(0) << 16
            | ((ccc.length > 1 ? ccc.charCodeAt(1) : 0) << 8)
            | ((ccc.length > 2 ? ccc.charCodeAt(2) : 0)),
        chars = [
            b64chars.charAt( ord >>> 18),
            b64chars.charAt((ord >>> 12) & 63),
            padlen >= 2 ? '=' : b64chars.charAt((ord >>> 6) & 63),
            padlen >= 1 ? '=' : b64chars.charAt(ord & 63)
        ];
        return chars.join('');
    };
    var btoa = global.btoa ? function(b) {
        return global.btoa(b);
    } : function(b) {
        return b.replace(/[\s\S]{1,3}/g, cb_encode);
    };
    var _encode = buffer ?
        buffer.from && Uint8Array && buffer.from !== Uint8Array.from
        ? function (u) {
            return (u.constructor === buffer.constructor ? u : buffer.from(u))
                .toString('base64')
        }
        :  function (u) {
            return (u.constructor === buffer.constructor ? u : new  buffer(u))
                .toString('base64')
        }
        : function (u) { return btoa(utob(u)) }
    ;
    var encode = function(u, urisafe) {
        return !urisafe
            ? _encode(String(u))
            : _encode(String(u)).replace(/[+\/]/g, function(m0) {
                return m0 == '+' ? '-' : '_';
            }).replace(/=/g, '');
    };
    var encodeURI = function(u) { return encode(u, true) };
    // decoder stuff
    var re_btou = new RegExp([
        '[\xC0-\xDF][\x80-\xBF]',
        '[\xE0-\xEF][\x80-\xBF]{2}',
        '[\xF0-\xF7][\x80-\xBF]{3}'
    ].join('|'), 'g');
    var cb_btou = function(cccc) {
        switch(cccc.length) {
        case 4:
            var cp = ((0x07 & cccc.charCodeAt(0)) << 18)
                |    ((0x3f & cccc.charCodeAt(1)) << 12)
                |    ((0x3f & cccc.charCodeAt(2)) <<  6)
                |     (0x3f & cccc.charCodeAt(3)),
            offset = cp - 0x10000;
            return (fromCharCode((offset  >>> 10) + 0xD800)
                    + fromCharCode((offset & 0x3FF) + 0xDC00));
        case 3:
            return fromCharCode(
                ((0x0f & cccc.charCodeAt(0)) << 12)
                    | ((0x3f & cccc.charCodeAt(1)) << 6)
                    |  (0x3f & cccc.charCodeAt(2))
            );
        default:
            return  fromCharCode(
                ((0x1f & cccc.charCodeAt(0)) << 6)
                    |  (0x3f & cccc.charCodeAt(1))
            );
        }
    };
    var btou = function(b) {
        return b.replace(re_btou, cb_btou);
    };
    var cb_decode = function(cccc) {
        var len = cccc.length,
        padlen = len % 4,
        n = (len > 0 ? b64tab[cccc.charAt(0)] << 18 : 0)
            | (len > 1 ? b64tab[cccc.charAt(1)] << 12 : 0)
            | (len > 2 ? b64tab[cccc.charAt(2)] <<  6 : 0)
            | (len > 3 ? b64tab[cccc.charAt(3)]       : 0),
        chars = [
            fromCharCode( n >>> 16),
            fromCharCode((n >>>  8) & 0xff),
            fromCharCode( n         & 0xff)
        ];
        chars.length -= [0, 0, 2, 1][padlen];
        return chars.join('');
    };
    var _atob = global.atob ? function(a) {
        return global.atob(a);
    } : function(a){
        return a.replace(/\S{1,4}/g, cb_decode);
    };
    var atob = function(a) {
        return _atob(String(a).replace(/[^A-Za-z0-9\+\/]/g, ''));
    };
    var _decode = buffer ?
        buffer.from && Uint8Array && buffer.from !== Uint8Array.from
        ? function(a) {
            return (a.constructor === buffer.constructor
                    ? a : buffer.from(a, 'base64')).toString();
        }
        : function(a) {
            return (a.constructor === buffer.constructor
                    ? a : new buffer(a, 'base64')).toString();
        }
        : function(a) { return btou(_atob(a)) };
    var decode = function(a){
        return _decode(
            String(a).replace(/[-_]/g, function(m0) { return m0 == '-' ? '+' : '/' })
                .replace(/[^A-Za-z0-9\+\/]/g, '')
        );
    };
    var noConflict = function() {
        var Base64 = global.Base64;
        global.Base64 = _Base64;
        return Base64;
    };
    // export Base64
    global.Base64 = {
        VERSION: version,
        atob: atob,
        btoa: btoa,
        fromBase64: decode,
        toBase64: encode,
        utob: utob,
        encode: encode,
        encodeURI: encodeURI,
        btou: btou,
        decode: decode,
        noConflict: noConflict,
        __buffer__: buffer
    };
    // if ES5 is available, make Base64.extendString() available
    if (typeof Object.defineProperty === 'function') {
        var noEnum = function(v){
            return {value:v,enumerable:false,writable:true,configurable:true};
        };
        global.Base64.extendString = function () {
            Object.defineProperty(
                String.prototype, 'fromBase64', noEnum(function () {
                    return decode(this)
                }));
            Object.defineProperty(
                String.prototype, 'toBase64', noEnum(function (urisafe) {
                    return encode(this, urisafe)
                }));
            Object.defineProperty(
                String.prototype, 'toBase64URI', noEnum(function () {
                    return encode(this, true)
                }));
        };
    }
    //
    // export Base64 to the namespace
    //
    if (global['Meteor']) { // Meteor.js
        Base64 = global.Base64;
    }
    // module.exports and AMD are mutually exclusive.
    // module.exports has precedence.
    if (typeof module !== 'undefined' && module.exports) {
        module.exports.Base64 = global.Base64;
    }
    else if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define([], function(){ return global.Base64 });
    }
    // that's it!
    return {Base64: global.Base64}
}));

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})

},{}],9:[function(require,module,exports){
(function (global){
/**
 * lodash (Custom Build) <https://lodash.com/>
 * Build: `lodash modularize exports="npm" -o ./`
 * Copyright jQuery Foundation and other contributors <https://jquery.org/>
 * Released under MIT license <https://lodash.com/license>
 * Based on Underscore.js 1.8.3 <http://underscorejs.org/LICENSE>
 * Copyright Jeremy Ashkenas, DocumentCloud and Investigative Reporters & Editors
 */

/** Used as the `TypeError` message for "Functions" methods. */
var FUNC_ERROR_TEXT = 'Expected a function';

/** Used as references for various `Number` constants. */
var NAN = 0 / 0;

/** `Object#toString` result references. */
var symbolTag = '[object Symbol]';

/** Used to match leading and trailing whitespace. */
var reTrim = /^\s+|\s+$/g;

/** Used to detect bad signed hexadecimal string values. */
var reIsBadHex = /^[-+]0x[0-9a-f]+$/i;

/** Used to detect binary string values. */
var reIsBinary = /^0b[01]+$/i;

/** Used to detect octal string values. */
var reIsOctal = /^0o[0-7]+$/i;

/** Built-in method references without a dependency on `root`. */
var freeParseInt = parseInt;

/** Detect free variable `global` from Node.js. */
var freeGlobal = typeof global == 'object' && global && global.Object === Object && global;

/** Detect free variable `self`. */
var freeSelf = typeof self == 'object' && self && self.Object === Object && self;

/** Used as a reference to the global object. */
var root = freeGlobal || freeSelf || Function('return this')();

/** Used for built-in method references. */
var objectProto = Object.prototype;

/**
 * Used to resolve the
 * [`toStringTag`](http://ecma-international.org/ecma-262/7.0/#sec-object.prototype.tostring)
 * of values.
 */
var objectToString = objectProto.toString;

/* Built-in method references for those with the same name as other `lodash` methods. */
var nativeMax = Math.max,
    nativeMin = Math.min;

/**
 * Gets the timestamp of the number of milliseconds that have elapsed since
 * the Unix epoch (1 January 1970 00:00:00 UTC).
 *
 * @static
 * @memberOf _
 * @since 2.4.0
 * @category Date
 * @returns {number} Returns the timestamp.
 * @example
 *
 * _.defer(function(stamp) {
 *   console.log(_.now() - stamp);
 * }, _.now());
 * // => Logs the number of milliseconds it took for the deferred invocation.
 */
var now = function() {
  return root.Date.now();
};

/**
 * Creates a debounced function that delays invoking `func` until after `wait`
 * milliseconds have elapsed since the last time the debounced function was
 * invoked. The debounced function comes with a `cancel` method to cancel
 * delayed `func` invocations and a `flush` method to immediately invoke them.
 * Provide `options` to indicate whether `func` should be invoked on the
 * leading and/or trailing edge of the `wait` timeout. The `func` is invoked
 * with the last arguments provided to the debounced function. Subsequent
 * calls to the debounced function return the result of the last `func`
 * invocation.
 *
 * **Note:** If `leading` and `trailing` options are `true`, `func` is
 * invoked on the trailing edge of the timeout only if the debounced function
 * is invoked more than once during the `wait` timeout.
 *
 * If `wait` is `0` and `leading` is `false`, `func` invocation is deferred
 * until to the next tick, similar to `setTimeout` with a timeout of `0`.
 *
 * See [David Corbacho's article](https://css-tricks.com/debouncing-throttling-explained-examples/)
 * for details over the differences between `_.debounce` and `_.throttle`.
 *
 * @static
 * @memberOf _
 * @since 0.1.0
 * @category Function
 * @param {Function} func The function to debounce.
 * @param {number} [wait=0] The number of milliseconds to delay.
 * @param {Object} [options={}] The options object.
 * @param {boolean} [options.leading=false]
 *  Specify invoking on the leading edge of the timeout.
 * @param {number} [options.maxWait]
 *  The maximum time `func` is allowed to be delayed before it's invoked.
 * @param {boolean} [options.trailing=true]
 *  Specify invoking on the trailing edge of the timeout.
 * @returns {Function} Returns the new debounced function.
 * @example
 *
 * // Avoid costly calculations while the window size is in flux.
 * jQuery(window).on('resize', _.debounce(calculateLayout, 150));
 *
 * // Invoke `sendMail` when clicked, debouncing subsequent calls.
 * jQuery(element).on('click', _.debounce(sendMail, 300, {
 *   'leading': true,
 *   'trailing': false
 * }));
 *
 * // Ensure `batchLog` is invoked once after 1 second of debounced calls.
 * var debounced = _.debounce(batchLog, 250, { 'maxWait': 1000 });
 * var source = new EventSource('/stream');
 * jQuery(source).on('message', debounced);
 *
 * // Cancel the trailing debounced invocation.
 * jQuery(window).on('popstate', debounced.cancel);
 */
function debounce(func, wait, options) {
  var lastArgs,
      lastThis,
      maxWait,
      result,
      timerId,
      lastCallTime,
      lastInvokeTime = 0,
      leading = false,
      maxing = false,
      trailing = true;

  if (typeof func != 'function') {
    throw new TypeError(FUNC_ERROR_TEXT);
  }
  wait = toNumber(wait) || 0;
  if (isObject(options)) {
    leading = !!options.leading;
    maxing = 'maxWait' in options;
    maxWait = maxing ? nativeMax(toNumber(options.maxWait) || 0, wait) : maxWait;
    trailing = 'trailing' in options ? !!options.trailing : trailing;
  }

  function invokeFunc(time) {
    var args = lastArgs,
        thisArg = lastThis;

    lastArgs = lastThis = undefined;
    lastInvokeTime = time;
    result = func.apply(thisArg, args);
    return result;
  }

  function leadingEdge(time) {
    // Reset any `maxWait` timer.
    lastInvokeTime = time;
    // Start the timer for the trailing edge.
    timerId = setTimeout(timerExpired, wait);
    // Invoke the leading edge.
    return leading ? invokeFunc(time) : result;
  }

  function remainingWait(time) {
    var timeSinceLastCall = time - lastCallTime,
        timeSinceLastInvoke = time - lastInvokeTime,
        result = wait - timeSinceLastCall;

    return maxing ? nativeMin(result, maxWait - timeSinceLastInvoke) : result;
  }

  function shouldInvoke(time) {
    var timeSinceLastCall = time - lastCallTime,
        timeSinceLastInvoke = time - lastInvokeTime;

    // Either this is the first call, activity has stopped and we're at the
    // trailing edge, the system time has gone backwards and we're treating
    // it as the trailing edge, or we've hit the `maxWait` limit.
    return (lastCallTime === undefined || (timeSinceLastCall >= wait) ||
      (timeSinceLastCall < 0) || (maxing && timeSinceLastInvoke >= maxWait));
  }

  function timerExpired() {
    var time = now();
    if (shouldInvoke(time)) {
      return trailingEdge(time);
    }
    // Restart the timer.
    timerId = setTimeout(timerExpired, remainingWait(time));
  }

  function trailingEdge(time) {
    timerId = undefined;

    // Only invoke if we have `lastArgs` which means `func` has been
    // debounced at least once.
    if (trailing && lastArgs) {
      return invokeFunc(time);
    }
    lastArgs = lastThis = undefined;
    return result;
  }

  function cancel() {
    if (timerId !== undefined) {
      clearTimeout(timerId);
    }
    lastInvokeTime = 0;
    lastArgs = lastCallTime = lastThis = timerId = undefined;
  }

  function flush() {
    return timerId === undefined ? result : trailingEdge(now());
  }

  function debounced() {
    var time = now(),
        isInvoking = shouldInvoke(time);

    lastArgs = arguments;
    lastThis = this;
    lastCallTime = time;

    if (isInvoking) {
      if (timerId === undefined) {
        return leadingEdge(lastCallTime);
      }
      if (maxing) {
        // Handle invocations in a tight loop.
        timerId = setTimeout(timerExpired, wait);
        return invokeFunc(lastCallTime);
      }
    }
    if (timerId === undefined) {
      timerId = setTimeout(timerExpired, wait);
    }
    return result;
  }
  debounced.cancel = cancel;
  debounced.flush = flush;
  return debounced;
}

/**
 * Creates a throttled function that only invokes `func` at most once per
 * every `wait` milliseconds. The throttled function comes with a `cancel`
 * method to cancel delayed `func` invocations and a `flush` method to
 * immediately invoke them. Provide `options` to indicate whether `func`
 * should be invoked on the leading and/or trailing edge of the `wait`
 * timeout. The `func` is invoked with the last arguments provided to the
 * throttled function. Subsequent calls to the throttled function return the
 * result of the last `func` invocation.
 *
 * **Note:** If `leading` and `trailing` options are `true`, `func` is
 * invoked on the trailing edge of the timeout only if the throttled function
 * is invoked more than once during the `wait` timeout.
 *
 * If `wait` is `0` and `leading` is `false`, `func` invocation is deferred
 * until to the next tick, similar to `setTimeout` with a timeout of `0`.
 *
 * See [David Corbacho's article](https://css-tricks.com/debouncing-throttling-explained-examples/)
 * for details over the differences between `_.throttle` and `_.debounce`.
 *
 * @static
 * @memberOf _
 * @since 0.1.0
 * @category Function
 * @param {Function} func The function to throttle.
 * @param {number} [wait=0] The number of milliseconds to throttle invocations to.
 * @param {Object} [options={}] The options object.
 * @param {boolean} [options.leading=true]
 *  Specify invoking on the leading edge of the timeout.
 * @param {boolean} [options.trailing=true]
 *  Specify invoking on the trailing edge of the timeout.
 * @returns {Function} Returns the new throttled function.
 * @example
 *
 * // Avoid excessively updating the position while scrolling.
 * jQuery(window).on('scroll', _.throttle(updatePosition, 100));
 *
 * // Invoke `renewToken` when the click event is fired, but not more than once every 5 minutes.
 * var throttled = _.throttle(renewToken, 300000, { 'trailing': false });
 * jQuery(element).on('click', throttled);
 *
 * // Cancel the trailing throttled invocation.
 * jQuery(window).on('popstate', throttled.cancel);
 */
function throttle(func, wait, options) {
  var leading = true,
      trailing = true;

  if (typeof func != 'function') {
    throw new TypeError(FUNC_ERROR_TEXT);
  }
  if (isObject(options)) {
    leading = 'leading' in options ? !!options.leading : leading;
    trailing = 'trailing' in options ? !!options.trailing : trailing;
  }
  return debounce(func, wait, {
    'leading': leading,
    'maxWait': wait,
    'trailing': trailing
  });
}

/**
 * Checks if `value` is the
 * [language type](http://www.ecma-international.org/ecma-262/7.0/#sec-ecmascript-language-types)
 * of `Object`. (e.g. arrays, functions, objects, regexes, `new Number(0)`, and `new String('')`)
 *
 * @static
 * @memberOf _
 * @since 0.1.0
 * @category Lang
 * @param {*} value The value to check.
 * @returns {boolean} Returns `true` if `value` is an object, else `false`.
 * @example
 *
 * _.isObject({});
 * // => true
 *
 * _.isObject([1, 2, 3]);
 * // => true
 *
 * _.isObject(_.noop);
 * // => true
 *
 * _.isObject(null);
 * // => false
 */
function isObject(value) {
  var type = typeof value;
  return !!value && (type == 'object' || type == 'function');
}

/**
 * Checks if `value` is object-like. A value is object-like if it's not `null`
 * and has a `typeof` result of "object".
 *
 * @static
 * @memberOf _
 * @since 4.0.0
 * @category Lang
 * @param {*} value The value to check.
 * @returns {boolean} Returns `true` if `value` is object-like, else `false`.
 * @example
 *
 * _.isObjectLike({});
 * // => true
 *
 * _.isObjectLike([1, 2, 3]);
 * // => true
 *
 * _.isObjectLike(_.noop);
 * // => false
 *
 * _.isObjectLike(null);
 * // => false
 */
function isObjectLike(value) {
  return !!value && typeof value == 'object';
}

/**
 * Checks if `value` is classified as a `Symbol` primitive or object.
 *
 * @static
 * @memberOf _
 * @since 4.0.0
 * @category Lang
 * @param {*} value The value to check.
 * @returns {boolean} Returns `true` if `value` is a symbol, else `false`.
 * @example
 *
 * _.isSymbol(Symbol.iterator);
 * // => true
 *
 * _.isSymbol('abc');
 * // => false
 */
function isSymbol(value) {
  return typeof value == 'symbol' ||
    (isObjectLike(value) && objectToString.call(value) == symbolTag);
}

/**
 * Converts `value` to a number.
 *
 * @static
 * @memberOf _
 * @since 4.0.0
 * @category Lang
 * @param {*} value The value to process.
 * @returns {number} Returns the number.
 * @example
 *
 * _.toNumber(3.2);
 * // => 3.2
 *
 * _.toNumber(Number.MIN_VALUE);
 * // => 5e-324
 *
 * _.toNumber(Infinity);
 * // => Infinity
 *
 * _.toNumber('3.2');
 * // => 3.2
 */
function toNumber(value) {
  if (typeof value == 'number') {
    return value;
  }
  if (isSymbol(value)) {
    return NAN;
  }
  if (isObject(value)) {
    var other = typeof value.valueOf == 'function' ? value.valueOf() : value;
    value = isObject(other) ? (other + '') : other;
  }
  if (typeof value != 'string') {
    return value === 0 ? value : +value;
  }
  value = value.replace(reTrim, '');
  var isBinary = reIsBinary.test(value);
  return (isBinary || reIsOctal.test(value))
    ? freeParseInt(value.slice(2), isBinary ? 2 : 8)
    : (reIsBadHex.test(value) ? NAN : +value);
}

module.exports = throttle;

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})

},{}],10:[function(require,module,exports){
var wildcard = require('wildcard');
var reMimePartSplit = /[\/\+\.]/;

/**
  # mime-match

  A simple function to checker whether a target mime type matches a mime-type
  pattern (e.g. image/jpeg matches image/jpeg OR image/*).

  ## Example Usage

  <<< example.js

**/
module.exports = function(target, pattern) {
  function test(pattern) {
    var result = wildcard(pattern, target, reMimePartSplit);

    // ensure that we have a valid mime type (should have two parts)
    return result && result.length >= 2;
  }

  return pattern ? test(pattern.split(';')[0]) : test;
};

},{"wildcard":18}],11:[function(require,module,exports){
/**
* Create an event emitter with namespaces
* @name createNamespaceEmitter
* @example
* var emitter = require('./index')()
*
* emitter.on('*', function () {
*   console.log('all events emitted', this.event)
* })
*
* emitter.on('example', function () {
*   console.log('example event emitted')
* })
*/
module.exports = function createNamespaceEmitter () {
  var emitter = {}
  var _fns = emitter._fns = {}

  /**
  * Emit an event. Optionally namespace the event. Handlers are fired in the order in which they were added with exact matches taking precedence. Separate the namespace and event with a `:`
  * @name emit
  * @param {String} event – the name of the event, with optional namespace
  * @param {...*} data – up to 6 arguments that are passed to the event listener
  * @example
  * emitter.emit('example')
  * emitter.emit('demo:test')
  * emitter.emit('data', { example: true}, 'a string', 1)
  */
  emitter.emit = function emit (event, arg1, arg2, arg3, arg4, arg5, arg6) {
    var toEmit = getListeners(event)

    if (toEmit.length) {
      emitAll(event, toEmit, [arg1, arg2, arg3, arg4, arg5, arg6])
    }
  }

  /**
  * Create en event listener.
  * @name on
  * @param {String} event
  * @param {Function} fn
  * @example
  * emitter.on('example', function () {})
  * emitter.on('demo', function () {})
  */
  emitter.on = function on (event, fn) {
    if (!_fns[event]) {
      _fns[event] = []
    }

    _fns[event].push(fn)
  }

  /**
  * Create en event listener that fires once.
  * @name once
  * @param {String} event
  * @param {Function} fn
  * @example
  * emitter.once('example', function () {})
  * emitter.once('demo', function () {})
  */
  emitter.once = function once (event, fn) {
    function one () {
      fn.apply(this, arguments)
      emitter.off(event, one)
    }
    this.on(event, one)
  }

  /**
  * Stop listening to an event. Stop all listeners on an event by only passing the event name. Stop a single listener by passing that event handler as a callback.
  * You must be explicit about what will be unsubscribed: `emitter.off('demo')` will unsubscribe an `emitter.on('demo')` listener,
  * `emitter.off('demo:example')` will unsubscribe an `emitter.on('demo:example')` listener
  * @name off
  * @param {String} event
  * @param {Function} [fn] – the specific handler
  * @example
  * emitter.off('example')
  * emitter.off('demo', function () {})
  */
  emitter.off = function off (event, fn) {
    var keep = []

    if (event && fn) {
      var fns = this._fns[event]
      var i = 0
      var l = fns ? fns.length : 0

      for (i; i < l; i++) {
        if (fns[i] !== fn) {
          keep.push(fns[i])
        }
      }
    }

    keep.length ? this._fns[event] = keep : delete this._fns[event]
  }

  function getListeners (e) {
    var out = _fns[e] ? _fns[e] : []
    var idx = e.indexOf(':')
    var args = (idx === -1) ? [e] : [e.substring(0, idx), e.substring(idx + 1)]

    var keys = Object.keys(_fns)
    var i = 0
    var l = keys.length

    for (i; i < l; i++) {
      var key = keys[i]
      if (key === '*') {
        out = out.concat(_fns[key])
      }

      if (args.length === 2 && args[0] === key) {
        out = out.concat(_fns[key])
        break
      }
    }

    return out
  }

  function emitAll (e, fns, args) {
    var i = 0
    var l = fns.length

    for (i; i < l; i++) {
      if (!fns[i]) break
      fns[i].event = e
      fns[i].apply(fns[i], args)
    }
  }

  return emitter
}

},{}],12:[function(require,module,exports){
!function() {
    'use strict';
    function VNode() {}
    function h(nodeName, attributes) {
        var lastSimple, child, simple, i, children = EMPTY_CHILDREN;
        for (i = arguments.length; i-- > 2; ) stack.push(arguments[i]);
        if (attributes && null != attributes.children) {
            if (!stack.length) stack.push(attributes.children);
            delete attributes.children;
        }
        while (stack.length) if ((child = stack.pop()) && void 0 !== child.pop) for (i = child.length; i--; ) stack.push(child[i]); else {
            if ('boolean' == typeof child) child = null;
            if (simple = 'function' != typeof nodeName) if (null == child) child = ''; else if ('number' == typeof child) child = String(child); else if ('string' != typeof child) simple = !1;
            if (simple && lastSimple) children[children.length - 1] += child; else if (children === EMPTY_CHILDREN) children = [ child ]; else children.push(child);
            lastSimple = simple;
        }
        var p = new VNode();
        p.nodeName = nodeName;
        p.children = children;
        p.attributes = null == attributes ? void 0 : attributes;
        p.key = null == attributes ? void 0 : attributes.key;
        if (void 0 !== options.vnode) options.vnode(p);
        return p;
    }
    function extend(obj, props) {
        for (var i in props) obj[i] = props[i];
        return obj;
    }
    function cloneElement(vnode, props) {
        return h(vnode.nodeName, extend(extend({}, vnode.attributes), props), arguments.length > 2 ? [].slice.call(arguments, 2) : vnode.children);
    }
    function enqueueRender(component) {
        if (!component.__d && (component.__d = !0) && 1 == items.push(component)) (options.debounceRendering || defer)(rerender);
    }
    function rerender() {
        var p, list = items;
        items = [];
        while (p = list.pop()) if (p.__d) renderComponent(p);
    }
    function isSameNodeType(node, vnode, hydrating) {
        if ('string' == typeof vnode || 'number' == typeof vnode) return void 0 !== node.splitText;
        if ('string' == typeof vnode.nodeName) return !node._componentConstructor && isNamedNode(node, vnode.nodeName); else return hydrating || node._componentConstructor === vnode.nodeName;
    }
    function isNamedNode(node, nodeName) {
        return node.__n === nodeName || node.nodeName.toLowerCase() === nodeName.toLowerCase();
    }
    function getNodeProps(vnode) {
        var props = extend({}, vnode.attributes);
        props.children = vnode.children;
        var defaultProps = vnode.nodeName.defaultProps;
        if (void 0 !== defaultProps) for (var i in defaultProps) if (void 0 === props[i]) props[i] = defaultProps[i];
        return props;
    }
    function createNode(nodeName, isSvg) {
        var node = isSvg ? document.createElementNS('http://www.w3.org/2000/svg', nodeName) : document.createElement(nodeName);
        node.__n = nodeName;
        return node;
    }
    function removeNode(node) {
        var parentNode = node.parentNode;
        if (parentNode) parentNode.removeChild(node);
    }
    function setAccessor(node, name, old, value, isSvg) {
        if ('className' === name) name = 'class';
        if ('key' === name) ; else if ('ref' === name) {
            if (old) old(null);
            if (value) value(node);
        } else if ('class' === name && !isSvg) node.className = value || ''; else if ('style' === name) {
            if (!value || 'string' == typeof value || 'string' == typeof old) node.style.cssText = value || '';
            if (value && 'object' == typeof value) {
                if ('string' != typeof old) for (var i in old) if (!(i in value)) node.style[i] = '';
                for (var i in value) node.style[i] = 'number' == typeof value[i] && !1 === IS_NON_DIMENSIONAL.test(i) ? value[i] + 'px' : value[i];
            }
        } else if ('dangerouslySetInnerHTML' === name) {
            if (value) node.innerHTML = value.__html || '';
        } else if ('o' == name[0] && 'n' == name[1]) {
            var useCapture = name !== (name = name.replace(/Capture$/, ''));
            name = name.toLowerCase().substring(2);
            if (value) {
                if (!old) node.addEventListener(name, eventProxy, useCapture);
            } else node.removeEventListener(name, eventProxy, useCapture);
            (node.__l || (node.__l = {}))[name] = value;
        } else if ('list' !== name && 'type' !== name && !isSvg && name in node) {
            setProperty(node, name, null == value ? '' : value);
            if (null == value || !1 === value) node.removeAttribute(name);
        } else {
            var ns = isSvg && name !== (name = name.replace(/^xlink:?/, ''));
            if (null == value || !1 === value) if (ns) node.removeAttributeNS('http://www.w3.org/1999/xlink', name.toLowerCase()); else node.removeAttribute(name); else if ('function' != typeof value) if (ns) node.setAttributeNS('http://www.w3.org/1999/xlink', name.toLowerCase(), value); else node.setAttribute(name, value);
        }
    }
    function setProperty(node, name, value) {
        try {
            node[name] = value;
        } catch (e) {}
    }
    function eventProxy(e) {
        return this.__l[e.type](options.event && options.event(e) || e);
    }
    function flushMounts() {
        var c;
        while (c = mounts.pop()) {
            if (options.afterMount) options.afterMount(c);
            if (c.componentDidMount) c.componentDidMount();
        }
    }
    function diff(dom, vnode, context, mountAll, parent, componentRoot) {
        if (!diffLevel++) {
            isSvgMode = null != parent && void 0 !== parent.ownerSVGElement;
            hydrating = null != dom && !('__preactattr_' in dom);
        }
        var ret = idiff(dom, vnode, context, mountAll, componentRoot);
        if (parent && ret.parentNode !== parent) parent.appendChild(ret);
        if (!--diffLevel) {
            hydrating = !1;
            if (!componentRoot) flushMounts();
        }
        return ret;
    }
    function idiff(dom, vnode, context, mountAll, componentRoot) {
        var out = dom, prevSvgMode = isSvgMode;
        if (null == vnode || 'boolean' == typeof vnode) vnode = '';
        if ('string' == typeof vnode || 'number' == typeof vnode) {
            if (dom && void 0 !== dom.splitText && dom.parentNode && (!dom._component || componentRoot)) {
                if (dom.nodeValue != vnode) dom.nodeValue = vnode;
            } else {
                out = document.createTextNode(vnode);
                if (dom) {
                    if (dom.parentNode) dom.parentNode.replaceChild(out, dom);
                    recollectNodeTree(dom, !0);
                }
            }
            out.__preactattr_ = !0;
            return out;
        }
        var vnodeName = vnode.nodeName;
        if ('function' == typeof vnodeName) return buildComponentFromVNode(dom, vnode, context, mountAll);
        isSvgMode = 'svg' === vnodeName ? !0 : 'foreignObject' === vnodeName ? !1 : isSvgMode;
        vnodeName = String(vnodeName);
        if (!dom || !isNamedNode(dom, vnodeName)) {
            out = createNode(vnodeName, isSvgMode);
            if (dom) {
                while (dom.firstChild) out.appendChild(dom.firstChild);
                if (dom.parentNode) dom.parentNode.replaceChild(out, dom);
                recollectNodeTree(dom, !0);
            }
        }
        var fc = out.firstChild, props = out.__preactattr_, vchildren = vnode.children;
        if (null == props) {
            props = out.__preactattr_ = {};
            for (var a = out.attributes, i = a.length; i--; ) props[a[i].name] = a[i].value;
        }
        if (!hydrating && vchildren && 1 === vchildren.length && 'string' == typeof vchildren[0] && null != fc && void 0 !== fc.splitText && null == fc.nextSibling) {
            if (fc.nodeValue != vchildren[0]) fc.nodeValue = vchildren[0];
        } else if (vchildren && vchildren.length || null != fc) innerDiffNode(out, vchildren, context, mountAll, hydrating || null != props.dangerouslySetInnerHTML);
        diffAttributes(out, vnode.attributes, props);
        isSvgMode = prevSvgMode;
        return out;
    }
    function innerDiffNode(dom, vchildren, context, mountAll, isHydrating) {
        var j, c, f, vchild, child, originalChildren = dom.childNodes, children = [], keyed = {}, keyedLen = 0, min = 0, len = originalChildren.length, childrenLen = 0, vlen = vchildren ? vchildren.length : 0;
        if (0 !== len) for (var i = 0; i < len; i++) {
            var _child = originalChildren[i], props = _child.__preactattr_, key = vlen && props ? _child._component ? _child._component.__k : props.key : null;
            if (null != key) {
                keyedLen++;
                keyed[key] = _child;
            } else if (props || (void 0 !== _child.splitText ? isHydrating ? _child.nodeValue.trim() : !0 : isHydrating)) children[childrenLen++] = _child;
        }
        if (0 !== vlen) for (var i = 0; i < vlen; i++) {
            vchild = vchildren[i];
            child = null;
            var key = vchild.key;
            if (null != key) {
                if (keyedLen && void 0 !== keyed[key]) {
                    child = keyed[key];
                    keyed[key] = void 0;
                    keyedLen--;
                }
            } else if (!child && min < childrenLen) for (j = min; j < childrenLen; j++) if (void 0 !== children[j] && isSameNodeType(c = children[j], vchild, isHydrating)) {
                child = c;
                children[j] = void 0;
                if (j === childrenLen - 1) childrenLen--;
                if (j === min) min++;
                break;
            }
            child = idiff(child, vchild, context, mountAll);
            f = originalChildren[i];
            if (child && child !== dom && child !== f) if (null == f) dom.appendChild(child); else if (child === f.nextSibling) removeNode(f); else dom.insertBefore(child, f);
        }
        if (keyedLen) for (var i in keyed) if (void 0 !== keyed[i]) recollectNodeTree(keyed[i], !1);
        while (min <= childrenLen) if (void 0 !== (child = children[childrenLen--])) recollectNodeTree(child, !1);
    }
    function recollectNodeTree(node, unmountOnly) {
        var component = node._component;
        if (component) unmountComponent(component); else {
            if (null != node.__preactattr_ && node.__preactattr_.ref) node.__preactattr_.ref(null);
            if (!1 === unmountOnly || null == node.__preactattr_) removeNode(node);
            removeChildren(node);
        }
    }
    function removeChildren(node) {
        node = node.lastChild;
        while (node) {
            var next = node.previousSibling;
            recollectNodeTree(node, !0);
            node = next;
        }
    }
    function diffAttributes(dom, attrs, old) {
        var name;
        for (name in old) if ((!attrs || null == attrs[name]) && null != old[name]) setAccessor(dom, name, old[name], old[name] = void 0, isSvgMode);
        for (name in attrs) if (!('children' === name || 'innerHTML' === name || name in old && attrs[name] === ('value' === name || 'checked' === name ? dom[name] : old[name]))) setAccessor(dom, name, old[name], old[name] = attrs[name], isSvgMode);
    }
    function collectComponent(component) {
        var name = component.constructor.name;
        (components[name] || (components[name] = [])).push(component);
    }
    function createComponent(Ctor, props, context) {
        var inst, list = components[Ctor.name];
        if (Ctor.prototype && Ctor.prototype.render) {
            inst = new Ctor(props, context);
            Component.call(inst, props, context);
        } else {
            inst = new Component(props, context);
            inst.constructor = Ctor;
            inst.render = doRender;
        }
        if (list) for (var i = list.length; i--; ) if (list[i].constructor === Ctor) {
            inst.__b = list[i].__b;
            list.splice(i, 1);
            break;
        }
        return inst;
    }
    function doRender(props, state, context) {
        return this.constructor(props, context);
    }
    function setComponentProps(component, props, opts, context, mountAll) {
        if (!component.__x) {
            component.__x = !0;
            if (component.__r = props.ref) delete props.ref;
            if (component.__k = props.key) delete props.key;
            if (!component.base || mountAll) {
                if (component.componentWillMount) component.componentWillMount();
            } else if (component.componentWillReceiveProps) component.componentWillReceiveProps(props, context);
            if (context && context !== component.context) {
                if (!component.__c) component.__c = component.context;
                component.context = context;
            }
            if (!component.__p) component.__p = component.props;
            component.props = props;
            component.__x = !1;
            if (0 !== opts) if (1 === opts || !1 !== options.syncComponentUpdates || !component.base) renderComponent(component, 1, mountAll); else enqueueRender(component);
            if (component.__r) component.__r(component);
        }
    }
    function renderComponent(component, opts, mountAll, isChild) {
        if (!component.__x) {
            var rendered, inst, cbase, props = component.props, state = component.state, context = component.context, previousProps = component.__p || props, previousState = component.__s || state, previousContext = component.__c || context, isUpdate = component.base, nextBase = component.__b, initialBase = isUpdate || nextBase, initialChildComponent = component._component, skip = !1;
            if (isUpdate) {
                component.props = previousProps;
                component.state = previousState;
                component.context = previousContext;
                if (2 !== opts && component.shouldComponentUpdate && !1 === component.shouldComponentUpdate(props, state, context)) skip = !0; else if (component.componentWillUpdate) component.componentWillUpdate(props, state, context);
                component.props = props;
                component.state = state;
                component.context = context;
            }
            component.__p = component.__s = component.__c = component.__b = null;
            component.__d = !1;
            if (!skip) {
                rendered = component.render(props, state, context);
                if (component.getChildContext) context = extend(extend({}, context), component.getChildContext());
                var toUnmount, base, childComponent = rendered && rendered.nodeName;
                if ('function' == typeof childComponent) {
                    var childProps = getNodeProps(rendered);
                    inst = initialChildComponent;
                    if (inst && inst.constructor === childComponent && childProps.key == inst.__k) setComponentProps(inst, childProps, 1, context, !1); else {
                        toUnmount = inst;
                        component._component = inst = createComponent(childComponent, childProps, context);
                        inst.__b = inst.__b || nextBase;
                        inst.__u = component;
                        setComponentProps(inst, childProps, 0, context, !1);
                        renderComponent(inst, 1, mountAll, !0);
                    }
                    base = inst.base;
                } else {
                    cbase = initialBase;
                    toUnmount = initialChildComponent;
                    if (toUnmount) cbase = component._component = null;
                    if (initialBase || 1 === opts) {
                        if (cbase) cbase._component = null;
                        base = diff(cbase, rendered, context, mountAll || !isUpdate, initialBase && initialBase.parentNode, !0);
                    }
                }
                if (initialBase && base !== initialBase && inst !== initialChildComponent) {
                    var baseParent = initialBase.parentNode;
                    if (baseParent && base !== baseParent) {
                        baseParent.replaceChild(base, initialBase);
                        if (!toUnmount) {
                            initialBase._component = null;
                            recollectNodeTree(initialBase, !1);
                        }
                    }
                }
                if (toUnmount) unmountComponent(toUnmount);
                component.base = base;
                if (base && !isChild) {
                    var componentRef = component, t = component;
                    while (t = t.__u) (componentRef = t).base = base;
                    base._component = componentRef;
                    base._componentConstructor = componentRef.constructor;
                }
            }
            if (!isUpdate || mountAll) mounts.unshift(component); else if (!skip) {
                if (component.componentDidUpdate) component.componentDidUpdate(previousProps, previousState, previousContext);
                if (options.afterUpdate) options.afterUpdate(component);
            }
            if (null != component.__h) while (component.__h.length) component.__h.pop().call(component);
            if (!diffLevel && !isChild) flushMounts();
        }
    }
    function buildComponentFromVNode(dom, vnode, context, mountAll) {
        var c = dom && dom._component, originalComponent = c, oldDom = dom, isDirectOwner = c && dom._componentConstructor === vnode.nodeName, isOwner = isDirectOwner, props = getNodeProps(vnode);
        while (c && !isOwner && (c = c.__u)) isOwner = c.constructor === vnode.nodeName;
        if (c && isOwner && (!mountAll || c._component)) {
            setComponentProps(c, props, 3, context, mountAll);
            dom = c.base;
        } else {
            if (originalComponent && !isDirectOwner) {
                unmountComponent(originalComponent);
                dom = oldDom = null;
            }
            c = createComponent(vnode.nodeName, props, context);
            if (dom && !c.__b) {
                c.__b = dom;
                oldDom = null;
            }
            setComponentProps(c, props, 1, context, mountAll);
            dom = c.base;
            if (oldDom && dom !== oldDom) {
                oldDom._component = null;
                recollectNodeTree(oldDom, !1);
            }
        }
        return dom;
    }
    function unmountComponent(component) {
        if (options.beforeUnmount) options.beforeUnmount(component);
        var base = component.base;
        component.__x = !0;
        if (component.componentWillUnmount) component.componentWillUnmount();
        component.base = null;
        var inner = component._component;
        if (inner) unmountComponent(inner); else if (base) {
            if (base.__preactattr_ && base.__preactattr_.ref) base.__preactattr_.ref(null);
            component.__b = base;
            removeNode(base);
            collectComponent(component);
            removeChildren(base);
        }
        if (component.__r) component.__r(null);
    }
    function Component(props, context) {
        this.__d = !0;
        this.context = context;
        this.props = props;
        this.state = this.state || {};
    }
    function render(vnode, parent, merge) {
        return diff(merge, vnode, {}, !1, parent, !1);
    }
    var options = {};
    var stack = [];
    var EMPTY_CHILDREN = [];
    var defer = 'function' == typeof Promise ? Promise.resolve().then.bind(Promise.resolve()) : setTimeout;
    var IS_NON_DIMENSIONAL = /acit|ex(?:s|g|n|p|$)|rph|ows|mnc|ntw|ine[ch]|zoo|^ord/i;
    var items = [];
    var mounts = [];
    var diffLevel = 0;
    var isSvgMode = !1;
    var hydrating = !1;
    var components = {};
    extend(Component.prototype, {
        setState: function(state, callback) {
            var s = this.state;
            if (!this.__s) this.__s = extend({}, s);
            extend(s, 'function' == typeof state ? state(s, this.props) : state);
            if (callback) (this.__h = this.__h || []).push(callback);
            enqueueRender(this);
        },
        forceUpdate: function(callback) {
            if (callback) (this.__h = this.__h || []).push(callback);
            renderComponent(this, 2);
        },
        render: function() {}
    });
    var preact = {
        h: h,
        createElement: h,
        cloneElement: cloneElement,
        Component: Component,
        render: render,
        rerender: rerender,
        options: options
    };
    if ('undefined' != typeof module) module.exports = preact; else self.preact = preact;
}();

},{}],13:[function(require,module,exports){
// shim for using process in browser
var process = module.exports = {};

// cached from whatever global is present so that test runners that stub it
// don't break things.  But we need to wrap it in a try catch in case it is
// wrapped in strict mode code which doesn't define any globals.  It's inside a
// function because try/catches deoptimize in certain engines.

var cachedSetTimeout;
var cachedClearTimeout;

function defaultSetTimout() {
    throw new Error('setTimeout has not been defined');
}
function defaultClearTimeout () {
    throw new Error('clearTimeout has not been defined');
}
(function () {
    try {
        if (typeof setTimeout === 'function') {
            cachedSetTimeout = setTimeout;
        } else {
            cachedSetTimeout = defaultSetTimout;
        }
    } catch (e) {
        cachedSetTimeout = defaultSetTimout;
    }
    try {
        if (typeof clearTimeout === 'function') {
            cachedClearTimeout = clearTimeout;
        } else {
            cachedClearTimeout = defaultClearTimeout;
        }
    } catch (e) {
        cachedClearTimeout = defaultClearTimeout;
    }
} ())
function runTimeout(fun) {
    if (cachedSetTimeout === setTimeout) {
        //normal enviroments in sane situations
        return setTimeout(fun, 0);
    }
    // if setTimeout wasn't available but was latter defined
    if ((cachedSetTimeout === defaultSetTimout || !cachedSetTimeout) && setTimeout) {
        cachedSetTimeout = setTimeout;
        return setTimeout(fun, 0);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedSetTimeout(fun, 0);
    } catch(e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't trust the global object when called normally
            return cachedSetTimeout.call(null, fun, 0);
        } catch(e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error
            return cachedSetTimeout.call(this, fun, 0);
        }
    }


}
function runClearTimeout(marker) {
    if (cachedClearTimeout === clearTimeout) {
        //normal enviroments in sane situations
        return clearTimeout(marker);
    }
    // if clearTimeout wasn't available but was latter defined
    if ((cachedClearTimeout === defaultClearTimeout || !cachedClearTimeout) && clearTimeout) {
        cachedClearTimeout = clearTimeout;
        return clearTimeout(marker);
    }
    try {
        // when when somebody has screwed with setTimeout but no I.E. maddness
        return cachedClearTimeout(marker);
    } catch (e){
        try {
            // When we are in I.E. but the script has been evaled so I.E. doesn't  trust the global object when called normally
            return cachedClearTimeout.call(null, marker);
        } catch (e){
            // same as above but when it's a version of I.E. that must have the global object for 'this', hopfully our context correct otherwise it will throw a global error.
            // Some versions of I.E. have different rules for clearTimeout vs setTimeout
            return cachedClearTimeout.call(this, marker);
        }
    }



}
var queue = [];
var draining = false;
var currentQueue;
var queueIndex = -1;

function cleanUpNextTick() {
    if (!draining || !currentQueue) {
        return;
    }
    draining = false;
    if (currentQueue.length) {
        queue = currentQueue.concat(queue);
    } else {
        queueIndex = -1;
    }
    if (queue.length) {
        drainQueue();
    }
}

function drainQueue() {
    if (draining) {
        return;
    }
    var timeout = runTimeout(cleanUpNextTick);
    draining = true;

    var len = queue.length;
    while(len) {
        currentQueue = queue;
        queue = [];
        while (++queueIndex < len) {
            if (currentQueue) {
                currentQueue[queueIndex].run();
            }
        }
        queueIndex = -1;
        len = queue.length;
    }
    currentQueue = null;
    draining = false;
    runClearTimeout(timeout);
}

process.nextTick = function (fun) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
        for (var i = 1; i < arguments.length; i++) {
            args[i - 1] = arguments[i];
        }
    }
    queue.push(new Item(fun, args));
    if (queue.length === 1 && !draining) {
        runTimeout(drainQueue);
    }
};

// v8 likes predictible objects
function Item(fun, array) {
    this.fun = fun;
    this.array = array;
}
Item.prototype.run = function () {
    this.fun.apply(null, this.array);
};
process.title = 'browser';
process.browser = true;
process.env = {};
process.argv = [];
process.version = ''; // empty string to avoid regexp issues
process.versions = {};

function noop() {}

process.on = noop;
process.addListener = noop;
process.once = noop;
process.off = noop;
process.removeListener = noop;
process.removeAllListeners = noop;
process.emit = noop;
process.prependListener = noop;
process.prependOnceListener = noop;

process.listeners = function (name) { return [] }

process.binding = function (name) {
    throw new Error('process.binding is not supported');
};

process.cwd = function () { return '/' };
process.chdir = function (dir) {
    throw new Error('process.chdir is not supported');
};
process.umask = function() { return 0; };

},{}],14:[function(require,module,exports){
'use strict';

var has = Object.prototype.hasOwnProperty
  , undef;

/**
 * Decode a URI encoded string.
 *
 * @param {String} input The URI encoded string.
 * @returns {String|Null} The decoded string.
 * @api private
 */
function decode(input) {
  try {
    return decodeURIComponent(input.replace(/\+/g, ' '));
  } catch (e) {
    return null;
  }
}

/**
 * Attempts to encode a given input.
 *
 * @param {String} input The string that needs to be encoded.
 * @returns {String|Null} The encoded string.
 * @api private
 */
function encode(input) {
  try {
    return encodeURIComponent(input);
  } catch (e) {
    return null;
  }
}

/**
 * Simple query string parser.
 *
 * @param {String} query The query string that needs to be parsed.
 * @returns {Object}
 * @api public
 */
function querystring(query) {
  var parser = /([^=?&]+)=?([^&]*)/g
    , result = {}
    , part;

  while (part = parser.exec(query)) {
    var key = decode(part[1])
      , value = decode(part[2]);

    //
    // Prevent overriding of existing properties. This ensures that build-in
    // methods like `toString` or __proto__ are not overriden by malicious
    // querystrings.
    //
    // In the case if failed decoding, we want to omit the key/value pairs
    // from the result.
    //
    if (key === null || value === null || key in result) continue;
    result[key] = value;
  }

  return result;
}

/**
 * Transform a query string to an object.
 *
 * @param {Object} obj Object that should be transformed.
 * @param {String} prefix Optional prefix.
 * @returns {String}
 * @api public
 */
function querystringify(obj, prefix) {
  prefix = prefix || '';

  var pairs = []
    , value
    , key;

  //
  // Optionally prefix with a '?' if needed
  //
  if ('string' !== typeof prefix) prefix = '?';

  for (key in obj) {
    if (has.call(obj, key)) {
      value = obj[key];

      //
      // Edge cases where we actually want to encode the value to an empty
      // string instead of the stringified value.
      //
      if (!value && (value === null || value === undef || isNaN(value))) {
        value = '';
      }

      key = encodeURIComponent(key);
      value = encodeURIComponent(value);

      //
      // If we failed to encode the strings, we should bail out as we don't
      // want to add invalid strings to the query.
      //
      if (key === null || value === null) continue;
      pairs.push(key +'='+ value);
    }
  }

  return pairs.length ? prefix + pairs.join('&') : '';
}

//
// Expose the module.
//
exports.stringify = querystringify;
exports.parse = querystring;

},{}],15:[function(require,module,exports){
'use strict';

/**
 * Check if we're required to add a port number.
 *
 * @see https://url.spec.whatwg.org/#default-port
 * @param {Number|String} port Port number we need to check
 * @param {String} protocol Protocol we need to check against.
 * @returns {Boolean} Is it a default port for the given protocol
 * @api private
 */
module.exports = function required(port, protocol) {
  protocol = protocol.split(':')[0];
  port = +port;

  if (!port) return false;

  switch (protocol) {
    case 'http':
    case 'ws':
    return port !== 80;

    case 'https':
    case 'wss':
    return port !== 443;

    case 'ftp':
    return port !== 21;

    case 'gopher':
    return port !== 70;

    case 'file':
    return false;
  }

  return port !== 0;
};

},{}],16:[function(require,module,exports){
(function (global){
'use strict';

var required = require('requires-port')
  , qs = require('querystringify')
  , slashes = /^[A-Za-z][A-Za-z0-9+-.]*:\/\//
  , protocolre = /^([a-z][a-z0-9.+-]*:)?(\/\/)?([\S\s]*)/i
  , whitespace = '[\\x09\\x0A\\x0B\\x0C\\x0D\\x20\\xA0\\u1680\\u180E\\u2000\\u2001\\u2002\\u2003\\u2004\\u2005\\u2006\\u2007\\u2008\\u2009\\u200A\\u202F\\u205F\\u3000\\u2028\\u2029\\uFEFF]'
  , left = new RegExp('^'+ whitespace +'+');

/**
 * Trim a given string.
 *
 * @param {String} str String to trim.
 * @public
 */
function trimLeft(str) {
  return (str ? str : '').toString().replace(left, '');
}

/**
 * These are the parse rules for the URL parser, it informs the parser
 * about:
 *
 * 0. The char it Needs to parse, if it's a string it should be done using
 *    indexOf, RegExp using exec and NaN means set as current value.
 * 1. The property we should set when parsing this value.
 * 2. Indication if it's backwards or forward parsing, when set as number it's
 *    the value of extra chars that should be split off.
 * 3. Inherit from location if non existing in the parser.
 * 4. `toLowerCase` the resulting value.
 */
var rules = [
  ['#', 'hash'],                        // Extract from the back.
  ['?', 'query'],                       // Extract from the back.
  function sanitize(address) {          // Sanitize what is left of the address
    return address.replace('\\', '/');
  },
  ['/', 'pathname'],                    // Extract from the back.
  ['@', 'auth', 1],                     // Extract from the front.
  [NaN, 'host', undefined, 1, 1],       // Set left over value.
  [/:(\d+)$/, 'port', undefined, 1],    // RegExp the back.
  [NaN, 'hostname', undefined, 1, 1]    // Set left over.
];

/**
 * These properties should not be copied or inherited from. This is only needed
 * for all non blob URL's as a blob URL does not include a hash, only the
 * origin.
 *
 * @type {Object}
 * @private
 */
var ignore = { hash: 1, query: 1 };

/**
 * The location object differs when your code is loaded through a normal page,
 * Worker or through a worker using a blob. And with the blobble begins the
 * trouble as the location object will contain the URL of the blob, not the
 * location of the page where our code is loaded in. The actual origin is
 * encoded in the `pathname` so we can thankfully generate a good "default"
 * location from it so we can generate proper relative URL's again.
 *
 * @param {Object|String} loc Optional default location object.
 * @returns {Object} lolcation object.
 * @public
 */
function lolcation(loc) {
  var globalVar;

  if (typeof window !== 'undefined') globalVar = window;
  else if (typeof global !== 'undefined') globalVar = global;
  else if (typeof self !== 'undefined') globalVar = self;
  else globalVar = {};

  var location = globalVar.location || {};
  loc = loc || location;

  var finaldestination = {}
    , type = typeof loc
    , key;

  if ('blob:' === loc.protocol) {
    finaldestination = new Url(unescape(loc.pathname), {});
  } else if ('string' === type) {
    finaldestination = new Url(loc, {});
    for (key in ignore) delete finaldestination[key];
  } else if ('object' === type) {
    for (key in loc) {
      if (key in ignore) continue;
      finaldestination[key] = loc[key];
    }

    if (finaldestination.slashes === undefined) {
      finaldestination.slashes = slashes.test(loc.href);
    }
  }

  return finaldestination;
}

/**
 * @typedef ProtocolExtract
 * @type Object
 * @property {String} protocol Protocol matched in the URL, in lowercase.
 * @property {Boolean} slashes `true` if protocol is followed by "//", else `false`.
 * @property {String} rest Rest of the URL that is not part of the protocol.
 */

/**
 * Extract protocol information from a URL with/without double slash ("//").
 *
 * @param {String} address URL we want to extract from.
 * @return {ProtocolExtract} Extracted information.
 * @private
 */
function extractProtocol(address) {
  address = trimLeft(address);
  var match = protocolre.exec(address);

  return {
    protocol: match[1] ? match[1].toLowerCase() : '',
    slashes: !!match[2],
    rest: match[3]
  };
}

/**
 * Resolve a relative URL pathname against a base URL pathname.
 *
 * @param {String} relative Pathname of the relative URL.
 * @param {String} base Pathname of the base URL.
 * @return {String} Resolved pathname.
 * @private
 */
function resolve(relative, base) {
  if (relative === '') return base;

  var path = (base || '/').split('/').slice(0, -1).concat(relative.split('/'))
    , i = path.length
    , last = path[i - 1]
    , unshift = false
    , up = 0;

  while (i--) {
    if (path[i] === '.') {
      path.splice(i, 1);
    } else if (path[i] === '..') {
      path.splice(i, 1);
      up++;
    } else if (up) {
      if (i === 0) unshift = true;
      path.splice(i, 1);
      up--;
    }
  }

  if (unshift) path.unshift('');
  if (last === '.' || last === '..') path.push('');

  return path.join('/');
}

/**
 * The actual URL instance. Instead of returning an object we've opted-in to
 * create an actual constructor as it's much more memory efficient and
 * faster and it pleases my OCD.
 *
 * It is worth noting that we should not use `URL` as class name to prevent
 * clashes with the global URL instance that got introduced in browsers.
 *
 * @constructor
 * @param {String} address URL we want to parse.
 * @param {Object|String} [location] Location defaults for relative paths.
 * @param {Boolean|Function} [parser] Parser for the query string.
 * @private
 */
function Url(address, location, parser) {
  address = trimLeft(address);

  if (!(this instanceof Url)) {
    return new Url(address, location, parser);
  }

  var relative, extracted, parse, instruction, index, key
    , instructions = rules.slice()
    , type = typeof location
    , url = this
    , i = 0;

  //
  // The following if statements allows this module two have compatibility with
  // 2 different API:
  //
  // 1. Node.js's `url.parse` api which accepts a URL, boolean as arguments
  //    where the boolean indicates that the query string should also be parsed.
  //
  // 2. The `URL` interface of the browser which accepts a URL, object as
  //    arguments. The supplied object will be used as default values / fall-back
  //    for relative paths.
  //
  if ('object' !== type && 'string' !== type) {
    parser = location;
    location = null;
  }

  if (parser && 'function' !== typeof parser) parser = qs.parse;

  location = lolcation(location);

  //
  // Extract protocol information before running the instructions.
  //
  extracted = extractProtocol(address || '');
  relative = !extracted.protocol && !extracted.slashes;
  url.slashes = extracted.slashes || relative && location.slashes;
  url.protocol = extracted.protocol || location.protocol || '';
  address = extracted.rest;

  //
  // When the authority component is absent the URL starts with a path
  // component.
  //
  if (!extracted.slashes) instructions[3] = [/(.*)/, 'pathname'];

  for (; i < instructions.length; i++) {
    instruction = instructions[i];

    if (typeof instruction === 'function') {
      address = instruction(address);
      continue;
    }

    parse = instruction[0];
    key = instruction[1];

    if (parse !== parse) {
      url[key] = address;
    } else if ('string' === typeof parse) {
      if (~(index = address.indexOf(parse))) {
        if ('number' === typeof instruction[2]) {
          url[key] = address.slice(0, index);
          address = address.slice(index + instruction[2]);
        } else {
          url[key] = address.slice(index);
          address = address.slice(0, index);
        }
      }
    } else if ((index = parse.exec(address))) {
      url[key] = index[1];
      address = address.slice(0, index.index);
    }

    url[key] = url[key] || (
      relative && instruction[3] ? location[key] || '' : ''
    );

    //
    // Hostname, host and protocol should be lowercased so they can be used to
    // create a proper `origin`.
    //
    if (instruction[4]) url[key] = url[key].toLowerCase();
  }

  //
  // Also parse the supplied query string in to an object. If we're supplied
  // with a custom parser as function use that instead of the default build-in
  // parser.
  //
  if (parser) url.query = parser(url.query);

  //
  // If the URL is relative, resolve the pathname against the base URL.
  //
  if (
      relative
    && location.slashes
    && url.pathname.charAt(0) !== '/'
    && (url.pathname !== '' || location.pathname !== '')
  ) {
    url.pathname = resolve(url.pathname, location.pathname);
  }

  //
  // We should not add port numbers if they are already the default port number
  // for a given protocol. As the host also contains the port number we're going
  // override it with the hostname which contains no port number.
  //
  if (!required(url.port, url.protocol)) {
    url.host = url.hostname;
    url.port = '';
  }

  //
  // Parse down the `auth` for the username and password.
  //
  url.username = url.password = '';
  if (url.auth) {
    instruction = url.auth.split(':');
    url.username = instruction[0] || '';
    url.password = instruction[1] || '';
  }

  url.origin = url.protocol && url.host && url.protocol !== 'file:'
    ? url.protocol +'//'+ url.host
    : 'null';

  //
  // The href is just the compiled result.
  //
  url.href = url.toString();
}

/**
 * This is convenience method for changing properties in the URL instance to
 * insure that they all propagate correctly.
 *
 * @param {String} part          Property we need to adjust.
 * @param {Mixed} value          The newly assigned value.
 * @param {Boolean|Function} fn  When setting the query, it will be the function
 *                               used to parse the query.
 *                               When setting the protocol, double slash will be
 *                               removed from the final url if it is true.
 * @returns {URL} URL instance for chaining.
 * @public
 */
function set(part, value, fn) {
  var url = this;

  switch (part) {
    case 'query':
      if ('string' === typeof value && value.length) {
        value = (fn || qs.parse)(value);
      }

      url[part] = value;
      break;

    case 'port':
      url[part] = value;

      if (!required(value, url.protocol)) {
        url.host = url.hostname;
        url[part] = '';
      } else if (value) {
        url.host = url.hostname +':'+ value;
      }

      break;

    case 'hostname':
      url[part] = value;

      if (url.port) value += ':'+ url.port;
      url.host = value;
      break;

    case 'host':
      url[part] = value;

      if (/:\d+$/.test(value)) {
        value = value.split(':');
        url.port = value.pop();
        url.hostname = value.join(':');
      } else {
        url.hostname = value;
        url.port = '';
      }

      break;

    case 'protocol':
      url.protocol = value.toLowerCase();
      url.slashes = !fn;
      break;

    case 'pathname':
    case 'hash':
      if (value) {
        var char = part === 'pathname' ? '/' : '#';
        url[part] = value.charAt(0) !== char ? char + value : value;
      } else {
        url[part] = value;
      }
      break;

    default:
      url[part] = value;
  }

  for (var i = 0; i < rules.length; i++) {
    var ins = rules[i];

    if (ins[4]) url[ins[1]] = url[ins[1]].toLowerCase();
  }

  url.origin = url.protocol && url.host && url.protocol !== 'file:'
    ? url.protocol +'//'+ url.host
    : 'null';

  url.href = url.toString();

  return url;
}

/**
 * Transform the properties back in to a valid and full URL string.
 *
 * @param {Function} stringify Optional query stringify function.
 * @returns {String} Compiled version of the URL.
 * @public
 */
function toString(stringify) {
  if (!stringify || 'function' !== typeof stringify) stringify = qs.stringify;

  var query
    , url = this
    , protocol = url.protocol;

  if (protocol && protocol.charAt(protocol.length - 1) !== ':') protocol += ':';

  var result = protocol + (url.slashes ? '//' : '');

  if (url.username) {
    result += url.username;
    if (url.password) result += ':'+ url.password;
    result += '@';
  }

  result += url.host + url.pathname;

  query = 'object' === typeof url.query ? stringify(url.query) : url.query;
  if (query) result += '?' !== query.charAt(0) ? '?'+ query : query;

  if (url.hash) result += url.hash;

  return result;
}

Url.prototype = { set: set, toString: toString };

//
// Expose the URL parser and some additional properties that might be useful for
// others or testing.
//
Url.extractProtocol = extractProtocol;
Url.location = lolcation;
Url.trimLeft = trimLeft;
Url.qs = qs;

module.exports = Url;

}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})

},{"querystringify":14,"requires-port":15}],17:[function(require,module,exports){
(function (global, factory) {
  typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
  typeof define === 'function' && define.amd ? define(['exports'], factory) :
  (factory((global.WHATWGFetch = {})));
}(this, (function (exports) { 'use strict';

  var support = {
    searchParams: 'URLSearchParams' in self,
    iterable: 'Symbol' in self && 'iterator' in Symbol,
    blob:
      'FileReader' in self &&
      'Blob' in self &&
      (function() {
        try {
          new Blob();
          return true
        } catch (e) {
          return false
        }
      })(),
    formData: 'FormData' in self,
    arrayBuffer: 'ArrayBuffer' in self
  };

  function isDataView(obj) {
    return obj && DataView.prototype.isPrototypeOf(obj)
  }

  if (support.arrayBuffer) {
    var viewClasses = [
      '[object Int8Array]',
      '[object Uint8Array]',
      '[object Uint8ClampedArray]',
      '[object Int16Array]',
      '[object Uint16Array]',
      '[object Int32Array]',
      '[object Uint32Array]',
      '[object Float32Array]',
      '[object Float64Array]'
    ];

    var isArrayBufferView =
      ArrayBuffer.isView ||
      function(obj) {
        return obj && viewClasses.indexOf(Object.prototype.toString.call(obj)) > -1
      };
  }

  function normalizeName(name) {
    if (typeof name !== 'string') {
      name = String(name);
    }
    if (/[^a-z0-9\-#$%&'*+.^_`|~]/i.test(name)) {
      throw new TypeError('Invalid character in header field name')
    }
    return name.toLowerCase()
  }

  function normalizeValue(value) {
    if (typeof value !== 'string') {
      value = String(value);
    }
    return value
  }

  // Build a destructive iterator for the value list
  function iteratorFor(items) {
    var iterator = {
      next: function() {
        var value = items.shift();
        return {done: value === undefined, value: value}
      }
    };

    if (support.iterable) {
      iterator[Symbol.iterator] = function() {
        return iterator
      };
    }

    return iterator
  }

  function Headers(headers) {
    this.map = {};

    if (headers instanceof Headers) {
      headers.forEach(function(value, name) {
        this.append(name, value);
      }, this);
    } else if (Array.isArray(headers)) {
      headers.forEach(function(header) {
        this.append(header[0], header[1]);
      }, this);
    } else if (headers) {
      Object.getOwnPropertyNames(headers).forEach(function(name) {
        this.append(name, headers[name]);
      }, this);
    }
  }

  Headers.prototype.append = function(name, value) {
    name = normalizeName(name);
    value = normalizeValue(value);
    var oldValue = this.map[name];
    this.map[name] = oldValue ? oldValue + ', ' + value : value;
  };

  Headers.prototype['delete'] = function(name) {
    delete this.map[normalizeName(name)];
  };

  Headers.prototype.get = function(name) {
    name = normalizeName(name);
    return this.has(name) ? this.map[name] : null
  };

  Headers.prototype.has = function(name) {
    return this.map.hasOwnProperty(normalizeName(name))
  };

  Headers.prototype.set = function(name, value) {
    this.map[normalizeName(name)] = normalizeValue(value);
  };

  Headers.prototype.forEach = function(callback, thisArg) {
    for (var name in this.map) {
      if (this.map.hasOwnProperty(name)) {
        callback.call(thisArg, this.map[name], name, this);
      }
    }
  };

  Headers.prototype.keys = function() {
    var items = [];
    this.forEach(function(value, name) {
      items.push(name);
    });
    return iteratorFor(items)
  };

  Headers.prototype.values = function() {
    var items = [];
    this.forEach(function(value) {
      items.push(value);
    });
    return iteratorFor(items)
  };

  Headers.prototype.entries = function() {
    var items = [];
    this.forEach(function(value, name) {
      items.push([name, value]);
    });
    return iteratorFor(items)
  };

  if (support.iterable) {
    Headers.prototype[Symbol.iterator] = Headers.prototype.entries;
  }

  function consumed(body) {
    if (body.bodyUsed) {
      return Promise.reject(new TypeError('Already read'))
    }
    body.bodyUsed = true;
  }

  function fileReaderReady(reader) {
    return new Promise(function(resolve, reject) {
      reader.onload = function() {
        resolve(reader.result);
      };
      reader.onerror = function() {
        reject(reader.error);
      };
    })
  }

  function readBlobAsArrayBuffer(blob) {
    var reader = new FileReader();
    var promise = fileReaderReady(reader);
    reader.readAsArrayBuffer(blob);
    return promise
  }

  function readBlobAsText(blob) {
    var reader = new FileReader();
    var promise = fileReaderReady(reader);
    reader.readAsText(blob);
    return promise
  }

  function readArrayBufferAsText(buf) {
    var view = new Uint8Array(buf);
    var chars = new Array(view.length);

    for (var i = 0; i < view.length; i++) {
      chars[i] = String.fromCharCode(view[i]);
    }
    return chars.join('')
  }

  function bufferClone(buf) {
    if (buf.slice) {
      return buf.slice(0)
    } else {
      var view = new Uint8Array(buf.byteLength);
      view.set(new Uint8Array(buf));
      return view.buffer
    }
  }

  function Body() {
    this.bodyUsed = false;

    this._initBody = function(body) {
      this._bodyInit = body;
      if (!body) {
        this._bodyText = '';
      } else if (typeof body === 'string') {
        this._bodyText = body;
      } else if (support.blob && Blob.prototype.isPrototypeOf(body)) {
        this._bodyBlob = body;
      } else if (support.formData && FormData.prototype.isPrototypeOf(body)) {
        this._bodyFormData = body;
      } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
        this._bodyText = body.toString();
      } else if (support.arrayBuffer && support.blob && isDataView(body)) {
        this._bodyArrayBuffer = bufferClone(body.buffer);
        // IE 10-11 can't handle a DataView body.
        this._bodyInit = new Blob([this._bodyArrayBuffer]);
      } else if (support.arrayBuffer && (ArrayBuffer.prototype.isPrototypeOf(body) || isArrayBufferView(body))) {
        this._bodyArrayBuffer = bufferClone(body);
      } else {
        this._bodyText = body = Object.prototype.toString.call(body);
      }

      if (!this.headers.get('content-type')) {
        if (typeof body === 'string') {
          this.headers.set('content-type', 'text/plain;charset=UTF-8');
        } else if (this._bodyBlob && this._bodyBlob.type) {
          this.headers.set('content-type', this._bodyBlob.type);
        } else if (support.searchParams && URLSearchParams.prototype.isPrototypeOf(body)) {
          this.headers.set('content-type', 'application/x-www-form-urlencoded;charset=UTF-8');
        }
      }
    };

    if (support.blob) {
      this.blob = function() {
        var rejected = consumed(this);
        if (rejected) {
          return rejected
        }

        if (this._bodyBlob) {
          return Promise.resolve(this._bodyBlob)
        } else if (this._bodyArrayBuffer) {
          return Promise.resolve(new Blob([this._bodyArrayBuffer]))
        } else if (this._bodyFormData) {
          throw new Error('could not read FormData body as blob')
        } else {
          return Promise.resolve(new Blob([this._bodyText]))
        }
      };

      this.arrayBuffer = function() {
        if (this._bodyArrayBuffer) {
          return consumed(this) || Promise.resolve(this._bodyArrayBuffer)
        } else {
          return this.blob().then(readBlobAsArrayBuffer)
        }
      };
    }

    this.text = function() {
      var rejected = consumed(this);
      if (rejected) {
        return rejected
      }

      if (this._bodyBlob) {
        return readBlobAsText(this._bodyBlob)
      } else if (this._bodyArrayBuffer) {
        return Promise.resolve(readArrayBufferAsText(this._bodyArrayBuffer))
      } else if (this._bodyFormData) {
        throw new Error('could not read FormData body as text')
      } else {
        return Promise.resolve(this._bodyText)
      }
    };

    if (support.formData) {
      this.formData = function() {
        return this.text().then(decode)
      };
    }

    this.json = function() {
      return this.text().then(JSON.parse)
    };

    return this
  }

  // HTTP methods whose capitalization should be normalized
  var methods = ['DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST', 'PUT'];

  function normalizeMethod(method) {
    var upcased = method.toUpperCase();
    return methods.indexOf(upcased) > -1 ? upcased : method
  }

  function Request(input, options) {
    options = options || {};
    var body = options.body;

    if (input instanceof Request) {
      if (input.bodyUsed) {
        throw new TypeError('Already read')
      }
      this.url = input.url;
      this.credentials = input.credentials;
      if (!options.headers) {
        this.headers = new Headers(input.headers);
      }
      this.method = input.method;
      this.mode = input.mode;
      this.signal = input.signal;
      if (!body && input._bodyInit != null) {
        body = input._bodyInit;
        input.bodyUsed = true;
      }
    } else {
      this.url = String(input);
    }

    this.credentials = options.credentials || this.credentials || 'same-origin';
    if (options.headers || !this.headers) {
      this.headers = new Headers(options.headers);
    }
    this.method = normalizeMethod(options.method || this.method || 'GET');
    this.mode = options.mode || this.mode || null;
    this.signal = options.signal || this.signal;
    this.referrer = null;

    if ((this.method === 'GET' || this.method === 'HEAD') && body) {
      throw new TypeError('Body not allowed for GET or HEAD requests')
    }
    this._initBody(body);
  }

  Request.prototype.clone = function() {
    return new Request(this, {body: this._bodyInit})
  };

  function decode(body) {
    var form = new FormData();
    body
      .trim()
      .split('&')
      .forEach(function(bytes) {
        if (bytes) {
          var split = bytes.split('=');
          var name = split.shift().replace(/\+/g, ' ');
          var value = split.join('=').replace(/\+/g, ' ');
          form.append(decodeURIComponent(name), decodeURIComponent(value));
        }
      });
    return form
  }

  function parseHeaders(rawHeaders) {
    var headers = new Headers();
    // Replace instances of \r\n and \n followed by at least one space or horizontal tab with a space
    // https://tools.ietf.org/html/rfc7230#section-3.2
    var preProcessedHeaders = rawHeaders.replace(/\r?\n[\t ]+/g, ' ');
    preProcessedHeaders.split(/\r?\n/).forEach(function(line) {
      var parts = line.split(':');
      var key = parts.shift().trim();
      if (key) {
        var value = parts.join(':').trim();
        headers.append(key, value);
      }
    });
    return headers
  }

  Body.call(Request.prototype);

  function Response(bodyInit, options) {
    if (!options) {
      options = {};
    }

    this.type = 'default';
    this.status = options.status === undefined ? 200 : options.status;
    this.ok = this.status >= 200 && this.status < 300;
    this.statusText = 'statusText' in options ? options.statusText : 'OK';
    this.headers = new Headers(options.headers);
    this.url = options.url || '';
    this._initBody(bodyInit);
  }

  Body.call(Response.prototype);

  Response.prototype.clone = function() {
    return new Response(this._bodyInit, {
      status: this.status,
      statusText: this.statusText,
      headers: new Headers(this.headers),
      url: this.url
    })
  };

  Response.error = function() {
    var response = new Response(null, {status: 0, statusText: ''});
    response.type = 'error';
    return response
  };

  var redirectStatuses = [301, 302, 303, 307, 308];

  Response.redirect = function(url, status) {
    if (redirectStatuses.indexOf(status) === -1) {
      throw new RangeError('Invalid status code')
    }

    return new Response(null, {status: status, headers: {location: url}})
  };

  exports.DOMException = self.DOMException;
  try {
    new exports.DOMException();
  } catch (err) {
    exports.DOMException = function(message, name) {
      this.message = message;
      this.name = name;
      var error = Error(message);
      this.stack = error.stack;
    };
    exports.DOMException.prototype = Object.create(Error.prototype);
    exports.DOMException.prototype.constructor = exports.DOMException;
  }

  function fetch(input, init) {
    return new Promise(function(resolve, reject) {
      var request = new Request(input, init);

      if (request.signal && request.signal.aborted) {
        return reject(new exports.DOMException('Aborted', 'AbortError'))
      }

      var xhr = new XMLHttpRequest();

      function abortXhr() {
        xhr.abort();
      }

      xhr.onload = function() {
        var options = {
          status: xhr.status,
          statusText: xhr.statusText,
          headers: parseHeaders(xhr.getAllResponseHeaders() || '')
        };
        options.url = 'responseURL' in xhr ? xhr.responseURL : options.headers.get('X-Request-URL');
        var body = 'response' in xhr ? xhr.response : xhr.responseText;
        resolve(new Response(body, options));
      };

      xhr.onerror = function() {
        reject(new TypeError('Network request failed'));
      };

      xhr.ontimeout = function() {
        reject(new TypeError('Network request failed'));
      };

      xhr.onabort = function() {
        reject(new exports.DOMException('Aborted', 'AbortError'));
      };

      xhr.open(request.method, request.url, true);

      if (request.credentials === 'include') {
        xhr.withCredentials = true;
      } else if (request.credentials === 'omit') {
        xhr.withCredentials = false;
      }

      if ('responseType' in xhr && support.blob) {
        xhr.responseType = 'blob';
      }

      request.headers.forEach(function(value, name) {
        xhr.setRequestHeader(name, value);
      });

      if (request.signal) {
        request.signal.addEventListener('abort', abortXhr);

        xhr.onreadystatechange = function() {
          // DONE (success or failure)
          if (xhr.readyState === 4) {
            request.signal.removeEventListener('abort', abortXhr);
          }
        };
      }

      xhr.send(typeof request._bodyInit === 'undefined' ? null : request._bodyInit);
    })
  }

  fetch.polyfill = true;

  if (!self.fetch) {
    self.fetch = fetch;
    self.Headers = Headers;
    self.Request = Request;
    self.Response = Response;
  }

  exports.Headers = Headers;
  exports.Request = Request;
  exports.Response = Response;
  exports.fetch = fetch;

  Object.defineProperty(exports, '__esModule', { value: true });

})));

},{}],18:[function(require,module,exports){
/* jshint node: true */
'use strict';

/**
  # wildcard

  Very simple wildcard matching, which is designed to provide the same
  functionality that is found in the
  [eve](https://github.com/adobe-webplatform/eve) eventing library.

  ## Usage

  It works with strings:

  <<< examples/strings.js

  Arrays:

  <<< examples/arrays.js

  Objects (matching against keys):

  <<< examples/objects.js

  While the library works in Node, if you are are looking for file-based
  wildcard matching then you should have a look at:

  <https://github.com/isaacs/node-glob>
**/

function WildcardMatcher(text, separator) {
  this.text = text = text || '';
  this.hasWild = ~text.indexOf('*');
  this.separator = separator;
  this.parts = text.split(separator);
}

WildcardMatcher.prototype.match = function(input) {
  var matches = true;
  var parts = this.parts;
  var ii;
  var partsCount = parts.length;
  var testParts;

  if (typeof input == 'string' || input instanceof String) {
    if (!this.hasWild && this.text != input) {
      matches = false;
    } else {
      testParts = (input || '').split(this.separator);
      for (ii = 0; matches && ii < partsCount; ii++) {
        if (parts[ii] === '*')  {
          continue;
        } else if (ii < testParts.length) {
          matches = parts[ii] === testParts[ii];
        } else {
          matches = false;
        }
      }

      // If matches, then return the component parts
      matches = matches && testParts;
    }
  }
  else if (typeof input.splice == 'function') {
    matches = [];

    for (ii = input.length; ii--; ) {
      if (this.match(input[ii])) {
        matches[matches.length] = input[ii];
      }
    }
  }
  else if (typeof input == 'object') {
    matches = {};

    for (var key in input) {
      if (this.match(key)) {
        matches[key] = input[key];
      }
    }
  }

  return matches;
};

module.exports = function(text, test, separator) {
  var matcher = new WildcardMatcher(text, separator || /[\/\.]/);
  if (typeof test != 'undefined') {
    return matcher.match(test);
  }

  return matcher;
};

},{}],19:[function(require,module,exports){
module.exports={
  "name": "@uppy/companion-client",
  "description": "Client library for communication with Companion. Intended for use in Uppy plugins.",
  "version": "1.4.1",
  "license": "MIT",
  "main": "lib/index.js",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-plugin",
    "companion",
    "provider"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  },
  "dependencies": {
    "namespace-emitter": "^2.0.1"
  }
}

},{}],20:[function(require,module,exports){
'use strict';

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

function _wrapNativeSuper(Class) { var _cache = typeof Map === "function" ? new Map() : undefined; _wrapNativeSuper = function _wrapNativeSuper(Class) { if (Class === null || !_isNativeFunction(Class)) return Class; if (typeof Class !== "function") { throw new TypeError("Super expression must either be null or a function"); } if (typeof _cache !== "undefined") { if (_cache.has(Class)) return _cache.get(Class); _cache.set(Class, Wrapper); } function Wrapper() { return _construct(Class, arguments, _getPrototypeOf(this).constructor); } Wrapper.prototype = Object.create(Class.prototype, { constructor: { value: Wrapper, enumerable: false, writable: true, configurable: true } }); return _setPrototypeOf(Wrapper, Class); }; return _wrapNativeSuper(Class); }

function isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _construct(Parent, args, Class) { if (isNativeReflectConstruct()) { _construct = Reflect.construct; } else { _construct = function _construct(Parent, args, Class) { var a = [null]; a.push.apply(a, args); var Constructor = Function.bind.apply(Parent, a); var instance = new Constructor(); if (Class) _setPrototypeOf(instance, Class.prototype); return instance; }; } return _construct.apply(null, arguments); }

function _isNativeFunction(fn) { return Function.toString.call(fn).indexOf("[native code]") !== -1; }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

var AuthError =
/*#__PURE__*/
function (_Error) {
  _inheritsLoose(AuthError, _Error);

  function AuthError() {
    var _this;

    _this = _Error.call(this, 'Authorization required') || this;
    _this.name = 'AuthError';
    _this.isAuthError = true;
    return _this;
  }

  return AuthError;
}(_wrapNativeSuper(Error));

module.exports = AuthError;

},{}],21:[function(require,module,exports){
'use strict';

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

var RequestClient = require('./RequestClient');

var tokenStorage = require('./tokenStorage');

var _getName = function _getName(id) {
  return id.split('-').map(function (s) {
    return s.charAt(0).toUpperCase() + s.slice(1);
  }).join(' ');
};

module.exports =
/*#__PURE__*/
function (_RequestClient) {
  _inheritsLoose(Provider, _RequestClient);

  function Provider(uppy, opts) {
    var _this;

    _this = _RequestClient.call(this, uppy, opts) || this;
    _this.provider = opts.provider;
    _this.id = _this.provider;
    _this.authProvider = opts.authProvider || _this.provider;
    _this.name = _this.opts.name || _getName(_this.id);
    _this.pluginId = _this.opts.pluginId;
    _this.tokenKey = "companion-" + _this.pluginId + "-auth-token";
    return _this;
  }

  var _proto = Provider.prototype;

  _proto.headers = function headers() {
    var _this2 = this;

    return new Promise(function (resolve, reject) {
      _RequestClient.prototype.headers.call(_this2).then(function (headers) {
        _this2.getAuthToken().then(function (token) {
          resolve(_extends({}, headers, {
            'uppy-auth-token': token
          }));
        });
      }).catch(reject);
    });
  };

  _proto.onReceiveResponse = function onReceiveResponse(response) {
    response = _RequestClient.prototype.onReceiveResponse.call(this, response);
    var plugin = this.uppy.getPlugin(this.pluginId);
    var oldAuthenticated = plugin.getPluginState().authenticated;
    var authenticated = oldAuthenticated ? response.status !== 401 : response.status < 400;
    plugin.setPluginState({
      authenticated: authenticated
    });
    return response;
  } // @todo(i.olarewaju) consider whether or not this method should be exposed
  ;

  _proto.setAuthToken = function setAuthToken(token) {
    return this.uppy.getPlugin(this.pluginId).storage.setItem(this.tokenKey, token);
  };

  _proto.getAuthToken = function getAuthToken() {
    return this.uppy.getPlugin(this.pluginId).storage.getItem(this.tokenKey);
  };

  _proto.authUrl = function authUrl() {
    return this.hostname + "/" + this.id + "/connect";
  };

  _proto.fileUrl = function fileUrl(id) {
    return this.hostname + "/" + this.id + "/get/" + id;
  };

  _proto.list = function list(directory) {
    return this.get(this.id + "/list/" + (directory || ''));
  };

  _proto.logout = function logout() {
    var _this3 = this;

    return new Promise(function (resolve, reject) {
      _this3.get(_this3.id + "/logout").then(function (res) {
        _this3.uppy.getPlugin(_this3.pluginId).storage.removeItem(_this3.tokenKey).then(function () {
          return resolve(res);
        }).catch(reject);
      }).catch(reject);
    });
  };

  Provider.initPlugin = function initPlugin(plugin, opts, defaultOpts) {
    plugin.type = 'acquirer';
    plugin.files = [];

    if (defaultOpts) {
      plugin.opts = _extends({}, defaultOpts, opts);
    }

    if (opts.serverUrl || opts.serverPattern) {
      throw new Error('`serverUrl` and `serverPattern` have been renamed to `companionUrl` and `companionAllowedHosts` respectively in the 0.30.5 release. Please consult the docs (for example, https://uppy.io/docs/instagram/ for the Instagram plugin) and use the updated options.`');
    }

    if (opts.companionAllowedHosts) {
      var pattern = opts.companionAllowedHosts; // validate companionAllowedHosts param

      if (typeof pattern !== 'string' && !Array.isArray(pattern) && !(pattern instanceof RegExp)) {
        throw new TypeError(plugin.id + ": the option \"companionAllowedHosts\" must be one of string, Array, RegExp");
      }

      plugin.opts.companionAllowedHosts = pattern;
    } else {
      // does not start with https://
      if (/^(?!https?:\/\/).*$/i.test(opts.companionUrl)) {
        plugin.opts.companionAllowedHosts = "https://" + opts.companionUrl.replace(/^\/\//, '');
      } else {
        plugin.opts.companionAllowedHosts = opts.companionUrl;
      }
    }

    plugin.storage = plugin.opts.storage || tokenStorage;
  };

  return Provider;
}(RequestClient);

},{"./RequestClient":22,"./tokenStorage":25}],22:[function(require,module,exports){
'use strict';

var _class, _temp;

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

var AuthError = require('./AuthError'); // Remove the trailing slash so we can always safely append /xyz.


function stripSlash(url) {
  return url.replace(/\/$/, '');
}

module.exports = (_temp = _class =
/*#__PURE__*/
function () {
  function RequestClient(uppy, opts) {
    this.uppy = uppy;
    this.opts = opts;
    this.onReceiveResponse = this.onReceiveResponse.bind(this);
    this.allowedHeaders = ['accept', 'content-type', 'uppy-auth-token'];
    this.preflightDone = false;
  }

  var _proto = RequestClient.prototype;

  _proto.headers = function headers() {
    var userHeaders = this.opts.companionHeaders || this.opts.serverHeaders || {};
    return Promise.resolve(_extends({}, this.defaultHeaders, {}, userHeaders));
  };

  _proto._getPostResponseFunc = function _getPostResponseFunc(skip) {
    var _this = this;

    return function (response) {
      if (!skip) {
        return _this.onReceiveResponse(response);
      }

      return response;
    };
  };

  _proto.onReceiveResponse = function onReceiveResponse(response) {
    var state = this.uppy.getState();
    var companion = state.companion || {};
    var host = this.opts.companionUrl;
    var headers = response.headers; // Store the self-identified domain name for the Companion instance we just hit.

    if (headers.has('i-am') && headers.get('i-am') !== companion[host]) {
      var _extends2;

      this.uppy.setState({
        companion: _extends({}, companion, (_extends2 = {}, _extends2[host] = headers.get('i-am'), _extends2))
      });
    }

    return response;
  };

  _proto._getUrl = function _getUrl(url) {
    if (/^(https?:|)\/\//.test(url)) {
      return url;
    }

    return this.hostname + "/" + url;
  };

  _proto._json = function _json(res) {
    if (res.status === 401) {
      throw new AuthError();
    }

    if (res.status < 200 || res.status > 300) {
      var errMsg = "Failed request with status: " + res.status + ". " + res.statusText;
      return res.json().then(function (errData) {
        errMsg = errData.message ? errMsg + " message: " + errData.message : errMsg;
        errMsg = errData.requestId ? errMsg + " request-Id: " + errData.requestId : errMsg;
        throw new Error(errMsg);
      }).catch(function () {
        throw new Error(errMsg);
      });
    }

    return res.json();
  };

  _proto.preflight = function preflight(path) {
    var _this2 = this;

    return new Promise(function (resolve, reject) {
      if (_this2.preflightDone) {
        return resolve(_this2.allowedHeaders.slice());
      }

      fetch(_this2._getUrl(path), {
        method: 'OPTIONS'
      }).then(function (response) {
        if (response.headers.has('access-control-allow-headers')) {
          _this2.allowedHeaders = response.headers.get('access-control-allow-headers').split(',').map(function (headerName) {
            return headerName.trim().toLowerCase();
          });
        }

        _this2.preflightDone = true;
        resolve(_this2.allowedHeaders.slice());
      }).catch(function (err) {
        _this2.uppy.log("[CompanionClient] unable to make preflight request " + err, 'warning');

        _this2.preflightDone = true;
        resolve(_this2.allowedHeaders.slice());
      });
    });
  };

  _proto.preflightAndHeaders = function preflightAndHeaders(path) {
    var _this3 = this;

    return Promise.all([this.preflight(path), this.headers()]).then(function (_ref) {
      var allowedHeaders = _ref[0],
          headers = _ref[1];
      // filter to keep only allowed Headers
      Object.keys(headers).forEach(function (header) {
        if (allowedHeaders.indexOf(header.toLowerCase()) === -1) {
          _this3.uppy.log("[CompanionClient] excluding unallowed header " + header);

          delete headers[header];
        }
      });
      return headers;
    });
  };

  _proto.get = function get(path, skipPostResponse) {
    var _this4 = this;

    return new Promise(function (resolve, reject) {
      _this4.preflightAndHeaders(path).then(function (headers) {
        fetch(_this4._getUrl(path), {
          method: 'get',
          headers: headers,
          credentials: 'same-origin'
        }).then(_this4._getPostResponseFunc(skipPostResponse)).then(function (res) {
          return _this4._json(res).then(resolve);
        }).catch(function (err) {
          err = err.isAuthError ? err : new Error("Could not get " + _this4._getUrl(path) + ". " + err);
          reject(err);
        });
      }).catch(reject);
    });
  };

  _proto.post = function post(path, data, skipPostResponse) {
    var _this5 = this;

    return new Promise(function (resolve, reject) {
      _this5.preflightAndHeaders(path).then(function (headers) {
        fetch(_this5._getUrl(path), {
          method: 'post',
          headers: headers,
          credentials: 'same-origin',
          body: JSON.stringify(data)
        }).then(_this5._getPostResponseFunc(skipPostResponse)).then(function (res) {
          return _this5._json(res).then(resolve);
        }).catch(function (err) {
          err = err.isAuthError ? err : new Error("Could not post " + _this5._getUrl(path) + ". " + err);
          reject(err);
        });
      }).catch(reject);
    });
  };

  _proto.delete = function _delete(path, data, skipPostResponse) {
    var _this6 = this;

    return new Promise(function (resolve, reject) {
      _this6.preflightAndHeaders(path).then(function (headers) {
        fetch(_this6.hostname + "/" + path, {
          method: 'delete',
          headers: headers,
          credentials: 'same-origin',
          body: data ? JSON.stringify(data) : null
        }).then(_this6._getPostResponseFunc(skipPostResponse)).then(function (res) {
          return _this6._json(res).then(resolve);
        }).catch(function (err) {
          err = err.isAuthError ? err : new Error("Could not delete " + _this6._getUrl(path) + ". " + err);
          reject(err);
        });
      }).catch(reject);
    });
  };

  _createClass(RequestClient, [{
    key: "hostname",
    get: function get() {
      var _this$uppy$getState = this.uppy.getState(),
          companion = _this$uppy$getState.companion;

      var host = this.opts.companionUrl;
      return stripSlash(companion && companion[host] ? companion[host] : host);
    }
  }, {
    key: "defaultHeaders",
    get: function get() {
      return {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'Uppy-Versions': "@uppy/companion-client=" + RequestClient.VERSION
      };
    }
  }]);

  return RequestClient;
}(), _class.VERSION = require('../package.json').version, _temp);

},{"../package.json":19,"./AuthError":20}],23:[function(require,module,exports){
var ee = require('namespace-emitter');

module.exports =
/*#__PURE__*/
function () {
  function UppySocket(opts) {
    this.opts = opts;
    this._queued = [];
    this.isOpen = false;
    this.emitter = ee();
    this._handleMessage = this._handleMessage.bind(this);
    this.close = this.close.bind(this);
    this.emit = this.emit.bind(this);
    this.on = this.on.bind(this);
    this.once = this.once.bind(this);
    this.send = this.send.bind(this);

    if (!opts || opts.autoOpen !== false) {
      this.open();
    }
  }

  var _proto = UppySocket.prototype;

  _proto.open = function open() {
    var _this = this;

    this.socket = new WebSocket(this.opts.target);

    this.socket.onopen = function (e) {
      _this.isOpen = true;

      while (_this._queued.length > 0 && _this.isOpen) {
        var first = _this._queued[0];

        _this.send(first.action, first.payload);

        _this._queued = _this._queued.slice(1);
      }
    };

    this.socket.onclose = function (e) {
      _this.isOpen = false;
    };

    this.socket.onmessage = this._handleMessage;
  };

  _proto.close = function close() {
    if (this.socket) {
      this.socket.close();
    }
  };

  _proto.send = function send(action, payload) {
    // attach uuid
    if (!this.isOpen) {
      this._queued.push({
        action: action,
        payload: payload
      });

      return;
    }

    this.socket.send(JSON.stringify({
      action: action,
      payload: payload
    }));
  };

  _proto.on = function on(action, handler) {
    this.emitter.on(action, handler);
  };

  _proto.emit = function emit(action, payload) {
    this.emitter.emit(action, payload);
  };

  _proto.once = function once(action, handler) {
    this.emitter.once(action, handler);
  };

  _proto._handleMessage = function _handleMessage(e) {
    try {
      var message = JSON.parse(e.data);
      this.emit(message.action, message.payload);
    } catch (err) {
      console.log(err);
    }
  };

  return UppySocket;
}();

},{"namespace-emitter":11}],24:[function(require,module,exports){
'use strict';
/**
 * Manages communications with Companion
 */

var RequestClient = require('./RequestClient');

var Provider = require('./Provider');

var Socket = require('./Socket');

module.exports = {
  RequestClient: RequestClient,
  Provider: Provider,
  Socket: Socket
};

},{"./Provider":21,"./RequestClient":22,"./Socket":23}],25:[function(require,module,exports){
'use strict';
/**
 * This module serves as an Async wrapper for LocalStorage
 */

module.exports.setItem = function (key, value) {
  return new Promise(function (resolve) {
    localStorage.setItem(key, value);
    resolve();
  });
};

module.exports.getItem = function (key) {
  return Promise.resolve(localStorage.getItem(key));
};

module.exports.removeItem = function (key) {
  return new Promise(function (resolve) {
    localStorage.removeItem(key);
    resolve();
  });
};

},{}],26:[function(require,module,exports){
module.exports={
  "name": "@uppy/core",
  "description": "Core module for the extensible JavaScript file upload widget with support for drag&drop, resumable uploads, previews, restrictions, file processing/encoding, remote providers like Instagram, Dropbox, Google Drive, S3 and more :dog:",
  "version": "1.5.1",
  "license": "MIT",
  "main": "lib/index.js",
  "style": "dist/style.min.css",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-plugin"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  },
  "dependencies": {
    "@uppy/store-default": "file:../store-default",
    "@uppy/utils": "file:../utils",
    "cuid": "^2.1.1",
    "lodash.throttle": "^4.1.1",
    "mime-match": "^1.0.2",
    "namespace-emitter": "^2.0.1",
    "preact": "8.2.9"
  }
}

},{}],27:[function(require,module,exports){
function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

var preact = require('preact');

var findDOMElement = require('./../../utils/lib/findDOMElement');
/**
 * Defer a frequent call to the microtask queue.
 */


function debounce(fn) {
  var calling = null;
  var latestArgs = null;
  return function () {
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    latestArgs = args;

    if (!calling) {
      calling = Promise.resolve().then(function () {
        calling = null; // At this point `args` may be different from the most
        // recent state, if multiple calls happened since this task
        // was queued. So we use the `latestArgs`, which definitely
        // is the most recent call.

        return fn.apply(void 0, latestArgs);
      });
    }

    return calling;
  };
}
/**
 * Boilerplate that all Plugins share - and should not be used
 * directly. It also shows which methods final plugins should implement/override,
 * this deciding on structure.
 *
 * @param {object} main Uppy core object
 * @param {object} object with plugin options
 * @returns {Array|string} files or success/fail message
 */


module.exports =
/*#__PURE__*/
function () {
  function Plugin(uppy, opts) {
    this.uppy = uppy;
    this.opts = opts || {};
    this.update = this.update.bind(this);
    this.mount = this.mount.bind(this);
    this.install = this.install.bind(this);
    this.uninstall = this.uninstall.bind(this);
  }

  var _proto = Plugin.prototype;

  _proto.getPluginState = function getPluginState() {
    var _this$uppy$getState = this.uppy.getState(),
        plugins = _this$uppy$getState.plugins;

    return plugins[this.id] || {};
  };

  _proto.setPluginState = function setPluginState(update) {
    var _extends2;

    var _this$uppy$getState2 = this.uppy.getState(),
        plugins = _this$uppy$getState2.plugins;

    this.uppy.setState({
      plugins: _extends({}, plugins, (_extends2 = {}, _extends2[this.id] = _extends({}, plugins[this.id], {}, update), _extends2))
    });
  };

  _proto.update = function update(state) {
    if (typeof this.el === 'undefined') {
      return;
    }

    if (this._updateUI) {
      this._updateUI(state);
    }
  } // Called after every state update, after everything's mounted. Debounced.
  ;

  _proto.afterUpdate = function afterUpdate() {}
  /**
   * Called when plugin is mounted, whether in DOM or into another plugin.
   * Needed because sometimes plugins are mounted separately/after `install`,
   * so this.el and this.parent might not be available in `install`.
   * This is the case with @uppy/react plugins, for example.
   */
  ;

  _proto.onMount = function onMount() {}
  /**
   * Check if supplied `target` is a DOM element or an `object`.
   * If it’s an object — target is a plugin, and we search `plugins`
   * for a plugin with same name and return its target.
   *
   * @param {string|object} target
   *
   */
  ;

  _proto.mount = function mount(target, plugin) {
    var _this = this;

    var callerPluginName = plugin.id;
    var targetElement = findDOMElement(target);

    if (targetElement) {
      this.isTargetDOMEl = true; // API for plugins that require a synchronous rerender.

      this.rerender = function (state) {
        // plugin could be removed, but this.rerender is debounced below,
        // so it could still be called even after uppy.removePlugin or uppy.close
        // hence the check
        if (!_this.uppy.getPlugin(_this.id)) return;
        _this.el = preact.render(_this.render(state), targetElement, _this.el);

        _this.afterUpdate();
      };

      this._updateUI = debounce(this.rerender);
      this.uppy.log("Installing " + callerPluginName + " to a DOM element '" + target + "'"); // clear everything inside the target container

      if (this.opts.replaceTargetContent) {
        targetElement.innerHTML = '';
      }

      this.el = preact.render(this.render(this.uppy.getState()), targetElement);
      this.onMount();
      return this.el;
    }

    var targetPlugin;

    if (typeof target === 'object' && target instanceof Plugin) {
      // Targeting a plugin *instance*
      targetPlugin = target;
    } else if (typeof target === 'function') {
      // Targeting a plugin type
      var Target = target; // Find the target plugin instance.

      this.uppy.iteratePlugins(function (plugin) {
        if (plugin instanceof Target) {
          targetPlugin = plugin;
          return false;
        }
      });
    }

    if (targetPlugin) {
      this.uppy.log("Installing " + callerPluginName + " to " + targetPlugin.id);
      this.parent = targetPlugin;
      this.el = targetPlugin.addTarget(plugin);
      this.onMount();
      return this.el;
    }

    this.uppy.log("Not installing " + callerPluginName);
    throw new Error("Invalid target option given to " + callerPluginName + ". Please make sure that the element\n      exists on the page, or that the plugin you are targeting has been installed. Check that the <script> tag initializing Uppy\n      comes at the bottom of the page, before the closing </body> tag (see https://github.com/transloadit/uppy/issues/1042).");
  };

  _proto.render = function render(state) {
    throw new Error('Extend the render method to add your plugin to a DOM element');
  };

  _proto.addTarget = function addTarget(plugin) {
    throw new Error('Extend the addTarget method to add your plugin to another plugin\'s target');
  };

  _proto.unmount = function unmount() {
    if (this.isTargetDOMEl && this.el && this.el.parentNode) {
      this.el.parentNode.removeChild(this.el);
    }
  };

  _proto.install = function install() {};

  _proto.uninstall = function uninstall() {
    this.unmount();
  };

  return Plugin;
}();

},{"./../../utils/lib/findDOMElement":55,"preact":12}],28:[function(require,module,exports){
function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } }

function _createClass(Constructor, protoProps, staticProps) { if (protoProps) _defineProperties(Constructor.prototype, protoProps); if (staticProps) _defineProperties(Constructor, staticProps); return Constructor; }

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

function _wrapNativeSuper(Class) { var _cache = typeof Map === "function" ? new Map() : undefined; _wrapNativeSuper = function _wrapNativeSuper(Class) { if (Class === null || !_isNativeFunction(Class)) return Class; if (typeof Class !== "function") { throw new TypeError("Super expression must either be null or a function"); } if (typeof _cache !== "undefined") { if (_cache.has(Class)) return _cache.get(Class); _cache.set(Class, Wrapper); } function Wrapper() { return _construct(Class, arguments, _getPrototypeOf(this).constructor); } Wrapper.prototype = Object.create(Class.prototype, { constructor: { value: Wrapper, enumerable: false, writable: true, configurable: true } }); return _setPrototypeOf(Wrapper, Class); }; return _wrapNativeSuper(Class); }

function isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

function _construct(Parent, args, Class) { if (isNativeReflectConstruct()) { _construct = Reflect.construct; } else { _construct = function _construct(Parent, args, Class) { var a = [null]; a.push.apply(a, args); var Constructor = Function.bind.apply(Parent, a); var instance = new Constructor(); if (Class) _setPrototypeOf(instance, Class.prototype); return instance; }; } return _construct.apply(null, arguments); }

function _isNativeFunction(fn) { return Function.toString.call(fn).indexOf("[native code]") !== -1; }

function _setPrototypeOf(o, p) { _setPrototypeOf = Object.setPrototypeOf || function _setPrototypeOf(o, p) { o.__proto__ = p; return o; }; return _setPrototypeOf(o, p); }

function _getPrototypeOf(o) { _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf : function _getPrototypeOf(o) { return o.__proto__ || Object.getPrototypeOf(o); }; return _getPrototypeOf(o); }

var Translator = require('./../../utils/lib/Translator');

var ee = require('namespace-emitter');

var cuid = require('cuid');

var throttle = require('lodash.throttle');

var prettyBytes = require('./../../utils/lib/prettyBytes');

var match = require('mime-match');

var DefaultStore = require('./../../store-default');

var getFileType = require('./../../utils/lib/getFileType');

var getFileNameAndExtension = require('./../../utils/lib/getFileNameAndExtension');

var generateFileID = require('./../../utils/lib/generateFileID');

var supportsUploadProgress = require('./supportsUploadProgress');

var _require = require('./loggers'),
    nullLogger = _require.nullLogger,
    debugLogger = _require.debugLogger;

var Plugin = require('./Plugin'); // Exported from here.


var RestrictionError =
/*#__PURE__*/
function (_Error) {
  _inheritsLoose(RestrictionError, _Error);

  function RestrictionError() {
    var _this;

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    _this = _Error.call.apply(_Error, [this].concat(args)) || this;
    _this.isRestriction = true;
    return _this;
  }

  return RestrictionError;
}(_wrapNativeSuper(Error));
/**
 * Uppy Core module.
 * Manages plugins, state updates, acts as an event bus,
 * adds/removes files and metadata.
 */


var Uppy =
/*#__PURE__*/
function () {
  /**
   * Instantiate Uppy
   *
   * @param {object} opts — Uppy options
   */
  function Uppy(opts) {
    var _this2 = this;

    this.defaultLocale = {
      strings: {
        youCanOnlyUploadX: {
          0: 'You can only upload %{smart_count} file',
          1: 'You can only upload %{smart_count} files',
          2: 'You can only upload %{smart_count} files'
        },
        youHaveToAtLeastSelectX: {
          0: 'You have to select at least %{smart_count} file',
          1: 'You have to select at least %{smart_count} files',
          2: 'You have to select at least %{smart_count} files'
        },
        exceedsSize: 'This file exceeds maximum allowed size of',
        youCanOnlyUploadFileTypes: 'You can only upload: %{types}',
        companionError: 'Connection with Companion failed',
        companionAuthError: 'Authorization required',
        companionUnauthorizeHint: 'To unauthorize to your %{provider} account, please go to %{url}',
        failedToUpload: 'Failed to upload %{file}',
        noInternetConnection: 'No Internet connection',
        connectedToInternet: 'Connected to the Internet',
        // Strings for remote providers
        noFilesFound: 'You have no files or folders here',
        selectX: {
          0: 'Select %{smart_count}',
          1: 'Select %{smart_count}',
          2: 'Select %{smart_count}'
        },
        selectAllFilesFromFolderNamed: 'Select all files from folder %{name}',
        unselectAllFilesFromFolderNamed: 'Unselect all files from folder %{name}',
        selectFileNamed: 'Select file %{name}',
        unselectFileNamed: 'Unselect file %{name}',
        openFolderNamed: 'Open folder %{name}',
        cancel: 'Cancel',
        logOut: 'Log out',
        filter: 'Filter',
        resetFilter: 'Reset filter',
        loading: 'Loading...',
        authenticateWithTitle: 'Please authenticate with %{pluginName} to select files',
        authenticateWith: 'Connect to %{pluginName}',
        emptyFolderAdded: 'No files were added from empty folder',
        folderAdded: {
          0: 'Added %{smart_count} file from %{folder}',
          1: 'Added %{smart_count} files from %{folder}',
          2: 'Added %{smart_count} files from %{folder}'
        }
      } // set default options

    };
    var defaultOptions = {
      id: 'uppy',
      autoProceed: false,
      allowMultipleUploads: true,
      debug: false,
      restrictions: {
        maxFileSize: null,
        maxNumberOfFiles: null,
        minNumberOfFiles: null,
        allowedFileTypes: null
      },
      meta: {},
      onBeforeFileAdded: function onBeforeFileAdded(currentFile, files) {
        return currentFile;
      },
      onBeforeUpload: function onBeforeUpload(files) {
        return files;
      },
      store: DefaultStore(),
      logger: nullLogger // Merge default options with the ones set by user

    };
    this.opts = _extends({}, defaultOptions, opts);
    this.opts.restrictions = _extends({}, defaultOptions.restrictions, this.opts.restrictions); // Support debug: true for backwards-compatability, unless logger is set in opts
    // opts instead of this.opts to avoid comparing objects — we set logger: nullLogger in defaultOptions

    if (opts && opts.logger && opts.debug) {
      this.log('You are using a custom `logger`, but also set `debug: true`, which uses built-in logger to output logs to console. Ignoring `debug: true` and using your custom `logger`.', 'warning');
    } else if (opts && opts.debug) {
      this.opts.logger = debugLogger;
    }

    this.log("Using Core v" + this.constructor.VERSION);

    if (this.opts.restrictions.allowedFileTypes && this.opts.restrictions.allowedFileTypes !== null && !Array.isArray(this.opts.restrictions.allowedFileTypes)) {
      throw new TypeError('`restrictions.allowedFileTypes` must be an array');
    } // i18n


    this.translator = new Translator([this.defaultLocale, this.opts.locale]);
    this.locale = this.translator.locale;
    this.i18n = this.translator.translate.bind(this.translator);
    this.i18nArray = this.translator.translateArray.bind(this.translator); // Container for different types of plugins

    this.plugins = {};
    this.getState = this.getState.bind(this);
    this.getPlugin = this.getPlugin.bind(this);
    this.setFileMeta = this.setFileMeta.bind(this);
    this.setFileState = this.setFileState.bind(this);
    this.log = this.log.bind(this);
    this.info = this.info.bind(this);
    this.hideInfo = this.hideInfo.bind(this);
    this.addFile = this.addFile.bind(this);
    this.removeFile = this.removeFile.bind(this);
    this.pauseResume = this.pauseResume.bind(this); // ___Why throttle at 500ms?
    //    - We must throttle at >250ms for superfocus in Dashboard to work well (because animation takes 0.25s, and we want to wait for all animations to be over before refocusing).
    //    [Practical Check]: if thottle is at 100ms, then if you are uploading a file, and click 'ADD MORE FILES', - focus won't activate in Firefox.
    //    - We must throttle at around >500ms to avoid performance lags.
    //    [Practical Check] Firefox, try to upload a big file for a prolonged period of time. Laptop will start to heat up.

    this._calculateProgress = throttle(this._calculateProgress.bind(this), 500, {
      leading: true,
      trailing: true
    });
    this.updateOnlineStatus = this.updateOnlineStatus.bind(this);
    this.resetProgress = this.resetProgress.bind(this);
    this.pauseAll = this.pauseAll.bind(this);
    this.resumeAll = this.resumeAll.bind(this);
    this.retryAll = this.retryAll.bind(this);
    this.cancelAll = this.cancelAll.bind(this);
    this.retryUpload = this.retryUpload.bind(this);
    this.upload = this.upload.bind(this);
    this.emitter = ee();
    this.on = this.on.bind(this);
    this.off = this.off.bind(this);
    this.once = this.emitter.once.bind(this.emitter);
    this.emit = this.emitter.emit.bind(this.emitter);
    this.preProcessors = [];
    this.uploaders = [];
    this.postProcessors = [];
    this.store = this.opts.store;
    this.setState({
      plugins: {},
      files: {},
      currentUploads: {},
      allowNewUpload: true,
      capabilities: {
        uploadProgress: supportsUploadProgress(),
        individualCancellation: true,
        resumableUploads: false
      },
      totalProgress: 0,
      meta: _extends({}, this.opts.meta),
      info: {
        isHidden: true,
        type: 'info',
        message: ''
      }
    });
    this._storeUnsubscribe = this.store.subscribe(function (prevState, nextState, patch) {
      _this2.emit('state-update', prevState, nextState, patch);

      _this2.updateAll(nextState);
    }); // Exposing uppy object on window for debugging and testing

    if (this.opts.debug && typeof window !== 'undefined') {
      window[this.opts.id] = this;
    }

    this._addListeners();
  }

  var _proto = Uppy.prototype;

  _proto.on = function on(event, callback) {
    this.emitter.on(event, callback);
    return this;
  };

  _proto.off = function off(event, callback) {
    this.emitter.off(event, callback);
    return this;
  }
  /**
   * Iterate on all plugins and run `update` on them.
   * Called each time state changes.
   *
   */
  ;

  _proto.updateAll = function updateAll(state) {
    this.iteratePlugins(function (plugin) {
      plugin.update(state);
    });
  }
  /**
   * Updates state with a patch
   *
   * @param {object} patch {foo: 'bar'}
   */
  ;

  _proto.setState = function setState(patch) {
    this.store.setState(patch);
  }
  /**
   * Returns current state.
   *
   * @returns {object}
   */
  ;

  _proto.getState = function getState() {
    return this.store.getState();
  }
  /**
   * Back compat for when uppy.state is used instead of uppy.getState().
   */
  ;

  /**
   * Shorthand to set state for a specific file.
   */
  _proto.setFileState = function setFileState(fileID, state) {
    var _extends2;

    if (!this.getState().files[fileID]) {
      throw new Error("Can\u2019t set state for " + fileID + " (the file could have been removed)");
    }

    this.setState({
      files: _extends({}, this.getState().files, (_extends2 = {}, _extends2[fileID] = _extends({}, this.getState().files[fileID], state), _extends2))
    });
  };

  _proto.resetProgress = function resetProgress() {
    var defaultProgress = {
      percentage: 0,
      bytesUploaded: 0,
      uploadComplete: false,
      uploadStarted: null
    };

    var files = _extends({}, this.getState().files);

    var updatedFiles = {};
    Object.keys(files).forEach(function (fileID) {
      var updatedFile = _extends({}, files[fileID]);

      updatedFile.progress = _extends({}, updatedFile.progress, defaultProgress);
      updatedFiles[fileID] = updatedFile;
    });
    this.setState({
      files: updatedFiles,
      totalProgress: 0
    }); // TODO Document on the website

    this.emit('reset-progress');
  };

  _proto.addPreProcessor = function addPreProcessor(fn) {
    this.preProcessors.push(fn);
  };

  _proto.removePreProcessor = function removePreProcessor(fn) {
    var i = this.preProcessors.indexOf(fn);

    if (i !== -1) {
      this.preProcessors.splice(i, 1);
    }
  };

  _proto.addPostProcessor = function addPostProcessor(fn) {
    this.postProcessors.push(fn);
  };

  _proto.removePostProcessor = function removePostProcessor(fn) {
    var i = this.postProcessors.indexOf(fn);

    if (i !== -1) {
      this.postProcessors.splice(i, 1);
    }
  };

  _proto.addUploader = function addUploader(fn) {
    this.uploaders.push(fn);
  };

  _proto.removeUploader = function removeUploader(fn) {
    var i = this.uploaders.indexOf(fn);

    if (i !== -1) {
      this.uploaders.splice(i, 1);
    }
  };

  _proto.setMeta = function setMeta(data) {
    var updatedMeta = _extends({}, this.getState().meta, data);

    var updatedFiles = _extends({}, this.getState().files);

    Object.keys(updatedFiles).forEach(function (fileID) {
      updatedFiles[fileID] = _extends({}, updatedFiles[fileID], {
        meta: _extends({}, updatedFiles[fileID].meta, data)
      });
    });
    this.log('Adding metadata:');
    this.log(data);
    this.setState({
      meta: updatedMeta,
      files: updatedFiles
    });
  };

  _proto.setFileMeta = function setFileMeta(fileID, data) {
    var updatedFiles = _extends({}, this.getState().files);

    if (!updatedFiles[fileID]) {
      this.log('Was trying to set metadata for a file that has been removed: ', fileID);
      return;
    }

    var newMeta = _extends({}, updatedFiles[fileID].meta, data);

    updatedFiles[fileID] = _extends({}, updatedFiles[fileID], {
      meta: newMeta
    });
    this.setState({
      files: updatedFiles
    });
  }
  /**
   * Get a file object.
   *
   * @param {string} fileID The ID of the file object to return.
   */
  ;

  _proto.getFile = function getFile(fileID) {
    return this.getState().files[fileID];
  }
  /**
   * Get all files in an array.
   */
  ;

  _proto.getFiles = function getFiles() {
    var _this$getState = this.getState(),
        files = _this$getState.files;

    return Object.keys(files).map(function (fileID) {
      return files[fileID];
    });
  }
  /**
   * Check if minNumberOfFiles restriction is reached before uploading.
   *
   * @private
   */
  ;

  _proto._checkMinNumberOfFiles = function _checkMinNumberOfFiles(files) {
    var minNumberOfFiles = this.opts.restrictions.minNumberOfFiles;

    if (Object.keys(files).length < minNumberOfFiles) {
      throw new RestrictionError("" + this.i18n('youHaveToAtLeastSelectX', {
        smart_count: minNumberOfFiles
      }));
    }
  }
  /**
   * Check if file passes a set of restrictions set in options: maxFileSize,
   * maxNumberOfFiles and allowedFileTypes.
   *
   * @param {object} file object to check
   * @private
   */
  ;

  _proto._checkRestrictions = function _checkRestrictions(file) {
    var _this$opts$restrictio = this.opts.restrictions,
        maxFileSize = _this$opts$restrictio.maxFileSize,
        maxNumberOfFiles = _this$opts$restrictio.maxNumberOfFiles,
        allowedFileTypes = _this$opts$restrictio.allowedFileTypes;

    if (maxNumberOfFiles) {
      if (Object.keys(this.getState().files).length + 1 > maxNumberOfFiles) {
        throw new RestrictionError("" + this.i18n('youCanOnlyUploadX', {
          smart_count: maxNumberOfFiles
        }));
      }
    }

    if (allowedFileTypes) {
      var isCorrectFileType = allowedFileTypes.some(function (type) {
        // is this is a mime-type
        if (type.indexOf('/') > -1) {
          if (!file.type) return false;
          return match(file.type, type);
        } // otherwise this is likely an extension


        if (type[0] === '.') {
          return file.extension.toLowerCase() === type.substr(1).toLowerCase();
        }

        return false;
      });

      if (!isCorrectFileType) {
        var allowedFileTypesString = allowedFileTypes.join(', ');
        throw new RestrictionError(this.i18n('youCanOnlyUploadFileTypes', {
          types: allowedFileTypesString
        }));
      }
    } // We can't check maxFileSize if the size is unknown.


    if (maxFileSize && file.data.size != null) {
      if (file.data.size > maxFileSize) {
        throw new RestrictionError(this.i18n('exceedsSize') + " " + prettyBytes(maxFileSize));
      }
    }
  };

  _proto._showOrLogErrorAndThrow = function _showOrLogErrorAndThrow(err, _temp) {
    var _ref = _temp === void 0 ? {} : _temp,
        _ref$showInformer = _ref.showInformer,
        showInformer = _ref$showInformer === void 0 ? true : _ref$showInformer,
        _ref$file = _ref.file,
        file = _ref$file === void 0 ? null : _ref$file;

    var message = typeof err === 'object' ? err.message : err;
    var details = typeof err === 'object' && err.details ? err.details : ''; // Restriction errors should be logged, but not as errors,
    // as they are expected and shown in the UI.

    if (err.isRestriction) {
      this.log(message + " " + details);
      this.emit('restriction-failed', file, err);
    } else {
      this.log(message + " " + details, 'error');
    } // Sometimes informer has to be shown manually by the developer,
    // for example, in `onBeforeFileAdded`.


    if (showInformer) {
      this.info({
        message: message,
        details: details
      }, 'error', 5000);
    }

    throw typeof err === 'object' ? err : new Error(err);
  }
  /**
   * Add a new file to `state.files`. This will run `onBeforeFileAdded`,
   * try to guess file type in a clever way, check file against restrictions,
   * and start an upload if `autoProceed === true`.
   *
   * @param {object} file object to add
   * @returns {string} id for the added file
   */
  ;

  _proto.addFile = function addFile(file) {
    var _extends3,
        _this3 = this;

    var _this$getState2 = this.getState(),
        files = _this$getState2.files,
        allowNewUpload = _this$getState2.allowNewUpload;

    if (allowNewUpload === false) {
      this._showOrLogErrorAndThrow(new RestrictionError('Cannot add new files: already uploading.'), {
        file: file
      });
    }

    var fileType = getFileType(file);
    file.type = fileType;
    var onBeforeFileAddedResult = this.opts.onBeforeFileAdded(file, files);

    if (onBeforeFileAddedResult === false) {
      // Don’t show UI info for this error, as it should be done by the developer
      this._showOrLogErrorAndThrow(new RestrictionError('Cannot add the file because onBeforeFileAdded returned false.'), {
        showInformer: false,
        file: file
      });
    }

    if (typeof onBeforeFileAddedResult === 'object' && onBeforeFileAddedResult) {
      file = onBeforeFileAddedResult;
    }

    var fileName;

    if (file.name) {
      fileName = file.name;
    } else if (fileType.split('/')[0] === 'image') {
      fileName = fileType.split('/')[0] + '.' + fileType.split('/')[1];
    } else {
      fileName = 'noname';
    }

    var fileExtension = getFileNameAndExtension(fileName).extension;
    var isRemote = file.isRemote || false;
    var fileID = generateFileID(file);

    if (files[fileID]) {
      this._showOrLogErrorAndThrow(new RestrictionError("Cannot add the duplicate file '" + fileName + "', it already exists."), {
        file: file
      });
    }

    var meta = file.meta || {};
    meta.name = fileName;
    meta.type = fileType; // `null` means the size is unknown.

    var size = isFinite(file.data.size) ? file.data.size : null;
    var newFile = {
      source: file.source || '',
      id: fileID,
      name: fileName,
      extension: fileExtension || '',
      meta: _extends({}, this.getState().meta, meta),
      type: fileType,
      data: file.data,
      progress: {
        percentage: 0,
        bytesUploaded: 0,
        bytesTotal: size,
        uploadComplete: false,
        uploadStarted: null
      },
      size: size,
      isRemote: isRemote,
      remote: file.remote || '',
      preview: file.preview
    };

    try {
      this._checkRestrictions(newFile);
    } catch (err) {
      this._showOrLogErrorAndThrow(err, {
        file: newFile
      });
    }

    this.setState({
      files: _extends({}, files, (_extends3 = {}, _extends3[fileID] = newFile, _extends3))
    });
    this.emit('file-added', newFile);
    this.log("Added file: " + fileName + ", " + fileID + ", mime type: " + fileType);

    if (this.opts.autoProceed && !this.scheduledAutoProceed) {
      this.scheduledAutoProceed = setTimeout(function () {
        _this3.scheduledAutoProceed = null;

        _this3.upload().catch(function (err) {
          if (!err.isRestriction) {
            _this3.log(err.stack || err.message || err);
          }
        });
      }, 4);
    }

    return fileID;
  };

  _proto.removeFile = function removeFile(fileID) {
    var _this4 = this;

    var _this$getState3 = this.getState(),
        files = _this$getState3.files,
        currentUploads = _this$getState3.currentUploads;

    var updatedFiles = _extends({}, files);

    var removedFile = updatedFiles[fileID];
    delete updatedFiles[fileID]; // Remove this file from its `currentUpload`.

    var updatedUploads = _extends({}, currentUploads);

    var removeUploads = [];
    Object.keys(updatedUploads).forEach(function (uploadID) {
      var newFileIDs = currentUploads[uploadID].fileIDs.filter(function (uploadFileID) {
        return uploadFileID !== fileID;
      }); // Remove the upload if no files are associated with it anymore.

      if (newFileIDs.length === 0) {
        removeUploads.push(uploadID);
        return;
      }

      updatedUploads[uploadID] = _extends({}, currentUploads[uploadID], {
        fileIDs: newFileIDs
      });
    });
    this.setState(_extends({
      currentUploads: updatedUploads,
      files: updatedFiles
    }, // If this is the last file we just removed - allow new uploads!
    Object.keys(updatedFiles).length === 0 && {
      allowNewUpload: true
    }));
    removeUploads.forEach(function (uploadID) {
      _this4._removeUpload(uploadID);
    });

    this._calculateTotalProgress();

    this.emit('file-removed', removedFile);
    this.log("File removed: " + removedFile.id);
  };

  _proto.pauseResume = function pauseResume(fileID) {
    if (!this.getState().capabilities.resumableUploads || this.getFile(fileID).uploadComplete) {
      return;
    }

    var wasPaused = this.getFile(fileID).isPaused || false;
    var isPaused = !wasPaused;
    this.setFileState(fileID, {
      isPaused: isPaused
    });
    this.emit('upload-pause', fileID, isPaused);
    return isPaused;
  };

  _proto.pauseAll = function pauseAll() {
    var updatedFiles = _extends({}, this.getState().files);

    var inProgressUpdatedFiles = Object.keys(updatedFiles).filter(function (file) {
      return !updatedFiles[file].progress.uploadComplete && updatedFiles[file].progress.uploadStarted;
    });
    inProgressUpdatedFiles.forEach(function (file) {
      var updatedFile = _extends({}, updatedFiles[file], {
        isPaused: true
      });

      updatedFiles[file] = updatedFile;
    });
    this.setState({
      files: updatedFiles
    });
    this.emit('pause-all');
  };

  _proto.resumeAll = function resumeAll() {
    var updatedFiles = _extends({}, this.getState().files);

    var inProgressUpdatedFiles = Object.keys(updatedFiles).filter(function (file) {
      return !updatedFiles[file].progress.uploadComplete && updatedFiles[file].progress.uploadStarted;
    });
    inProgressUpdatedFiles.forEach(function (file) {
      var updatedFile = _extends({}, updatedFiles[file], {
        isPaused: false,
        error: null
      });

      updatedFiles[file] = updatedFile;
    });
    this.setState({
      files: updatedFiles
    });
    this.emit('resume-all');
  };

  _proto.retryAll = function retryAll() {
    var updatedFiles = _extends({}, this.getState().files);

    var filesToRetry = Object.keys(updatedFiles).filter(function (file) {
      return updatedFiles[file].error;
    });
    filesToRetry.forEach(function (file) {
      var updatedFile = _extends({}, updatedFiles[file], {
        isPaused: false,
        error: null
      });

      updatedFiles[file] = updatedFile;
    });
    this.setState({
      files: updatedFiles,
      error: null
    });
    this.emit('retry-all', filesToRetry);

    var uploadID = this._createUpload(filesToRetry);

    return this._runUpload(uploadID);
  };

  _proto.cancelAll = function cancelAll() {
    var _this5 = this;

    this.emit('cancel-all');
    var files = Object.keys(this.getState().files);
    files.forEach(function (fileID) {
      _this5.removeFile(fileID);
    });
    this.setState({
      totalProgress: 0,
      error: null
    });
  };

  _proto.retryUpload = function retryUpload(fileID) {
    this.setFileState(fileID, {
      error: null,
      isPaused: false
    });
    this.emit('upload-retry', fileID);

    var uploadID = this._createUpload([fileID]);

    return this._runUpload(uploadID);
  };

  _proto.reset = function reset() {
    this.cancelAll();
  };

  _proto._calculateProgress = function _calculateProgress(file, data) {
    if (!this.getFile(file.id)) {
      this.log("Not setting progress for a file that has been removed: " + file.id);
      return;
    } // bytesTotal may be null or zero; in that case we can't divide by it


    var canHavePercentage = isFinite(data.bytesTotal) && data.bytesTotal > 0;
    this.setFileState(file.id, {
      progress: _extends({}, this.getFile(file.id).progress, {
        bytesUploaded: data.bytesUploaded,
        bytesTotal: data.bytesTotal,
        percentage: canHavePercentage // TODO(goto-bus-stop) flooring this should probably be the choice of the UI?
        // we get more accurate calculations if we don't round this at all.
        ? Math.round(data.bytesUploaded / data.bytesTotal * 100) : 0
      })
    });

    this._calculateTotalProgress();
  };

  _proto._calculateTotalProgress = function _calculateTotalProgress() {
    // calculate total progress, using the number of files currently uploading,
    // multiplied by 100 and the summ of individual progress of each file
    var files = this.getFiles();
    var inProgress = files.filter(function (file) {
      return file.progress.uploadStarted;
    });

    if (inProgress.length === 0) {
      this.emit('progress', 0);
      this.setState({
        totalProgress: 0
      });
      return;
    }

    var sizedFiles = inProgress.filter(function (file) {
      return file.progress.bytesTotal != null;
    });
    var unsizedFiles = inProgress.filter(function (file) {
      return file.progress.bytesTotal == null;
    });

    if (sizedFiles.length === 0) {
      var progressMax = inProgress.length * 100;
      var currentProgress = unsizedFiles.reduce(function (acc, file) {
        return acc + file.progress.percentage;
      }, 0);

      var _totalProgress = Math.round(currentProgress / progressMax * 100);

      this.setState({
        totalProgress: _totalProgress
      });
      return;
    }

    var totalSize = sizedFiles.reduce(function (acc, file) {
      return acc + file.progress.bytesTotal;
    }, 0);
    var averageSize = totalSize / sizedFiles.length;
    totalSize += averageSize * unsizedFiles.length;
    var uploadedSize = 0;
    sizedFiles.forEach(function (file) {
      uploadedSize += file.progress.bytesUploaded;
    });
    unsizedFiles.forEach(function (file) {
      uploadedSize += averageSize * (file.progress.percentage || 0) / 100;
    });
    var totalProgress = totalSize === 0 ? 0 : Math.round(uploadedSize / totalSize * 100); // hot fix, because:
    // uploadedSize ended up larger than totalSize, resulting in 1325% total

    if (totalProgress > 100) {
      totalProgress = 100;
    }

    this.setState({
      totalProgress: totalProgress
    });
    this.emit('progress', totalProgress);
  }
  /**
   * Registers listeners for all global actions, like:
   * `error`, `file-removed`, `upload-progress`
   */
  ;

  _proto._addListeners = function _addListeners() {
    var _this6 = this;

    this.on('error', function (error) {
      _this6.setState({
        error: error.message || 'Unknown error'
      });
    });
    this.on('upload-error', function (file, error, response) {
      _this6.setFileState(file.id, {
        error: error.message || 'Unknown error',
        response: response
      });

      _this6.setState({
        error: error.message
      });

      var message = _this6.i18n('failedToUpload', {
        file: file.name
      });

      if (typeof error === 'object' && error.message) {
        message = {
          message: message,
          details: error.message
        };
      }

      _this6.info(message, 'error', 5000);
    });
    this.on('upload', function () {
      _this6.setState({
        error: null
      });
    });
    this.on('upload-started', function (file, upload) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      _this6.setFileState(file.id, {
        progress: {
          uploadStarted: Date.now(),
          uploadComplete: false,
          percentage: 0,
          bytesUploaded: 0,
          bytesTotal: file.size
        }
      });
    });
    this.on('upload-progress', this._calculateProgress);
    this.on('upload-success', function (file, uploadResp) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      var currentProgress = _this6.getFile(file.id).progress;

      _this6.setFileState(file.id, {
        progress: _extends({}, currentProgress, {
          uploadComplete: true,
          percentage: 100,
          bytesUploaded: currentProgress.bytesTotal
        }),
        response: uploadResp,
        uploadURL: uploadResp.uploadURL,
        isPaused: false
      });

      _this6._calculateTotalProgress();
    });
    this.on('preprocess-progress', function (file, progress) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      _this6.setFileState(file.id, {
        progress: _extends({}, _this6.getFile(file.id).progress, {
          preprocess: progress
        })
      });
    });
    this.on('preprocess-complete', function (file) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      var files = _extends({}, _this6.getState().files);

      files[file.id] = _extends({}, files[file.id], {
        progress: _extends({}, files[file.id].progress)
      });
      delete files[file.id].progress.preprocess;

      _this6.setState({
        files: files
      });
    });
    this.on('postprocess-progress', function (file, progress) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      _this6.setFileState(file.id, {
        progress: _extends({}, _this6.getState().files[file.id].progress, {
          postprocess: progress
        })
      });
    });
    this.on('postprocess-complete', function (file) {
      if (!_this6.getFile(file.id)) {
        _this6.log("Not setting progress for a file that has been removed: " + file.id);

        return;
      }

      var files = _extends({}, _this6.getState().files);

      files[file.id] = _extends({}, files[file.id], {
        progress: _extends({}, files[file.id].progress)
      });
      delete files[file.id].progress.postprocess; // TODO should we set some kind of `fullyComplete` property on the file object
      // so it's easier to see that the file is upload…fully complete…rather than
      // what we have to do now (`uploadComplete && !postprocess`)

      _this6.setState({
        files: files
      });
    });
    this.on('restored', function () {
      // Files may have changed--ensure progress is still accurate.
      _this6._calculateTotalProgress();
    }); // show informer if offline

    if (typeof window !== 'undefined' && window.addEventListener) {
      window.addEventListener('online', function () {
        return _this6.updateOnlineStatus();
      });
      window.addEventListener('offline', function () {
        return _this6.updateOnlineStatus();
      });
      setTimeout(function () {
        return _this6.updateOnlineStatus();
      }, 3000);
    }
  };

  _proto.updateOnlineStatus = function updateOnlineStatus() {
    var online = typeof window.navigator.onLine !== 'undefined' ? window.navigator.onLine : true;

    if (!online) {
      this.emit('is-offline');
      this.info(this.i18n('noInternetConnection'), 'error', 0);
      this.wasOffline = true;
    } else {
      this.emit('is-online');

      if (this.wasOffline) {
        this.emit('back-online');
        this.info(this.i18n('connectedToInternet'), 'success', 3000);
        this.wasOffline = false;
      }
    }
  };

  _proto.getID = function getID() {
    return this.opts.id;
  }
  /**
   * Registers a plugin with Core.
   *
   * @param {object} Plugin object
   * @param {object} [opts] object with options to be passed to Plugin
   * @returns {object} self for chaining
   */
  ;

  _proto.use = function use(Plugin, opts) {
    if (typeof Plugin !== 'function') {
      var msg = "Expected a plugin class, but got " + (Plugin === null ? 'null' : typeof Plugin) + "." + ' Please verify that the plugin was imported and spelled correctly.';
      throw new TypeError(msg);
    } // Instantiate


    var plugin = new Plugin(this, opts);
    var pluginId = plugin.id;
    this.plugins[plugin.type] = this.plugins[plugin.type] || [];

    if (!pluginId) {
      throw new Error('Your plugin must have an id');
    }

    if (!plugin.type) {
      throw new Error('Your plugin must have a type');
    }

    var existsPluginAlready = this.getPlugin(pluginId);

    if (existsPluginAlready) {
      var _msg = "Already found a plugin named '" + existsPluginAlready.id + "'. " + ("Tried to use: '" + pluginId + "'.\n") + 'Uppy plugins must have unique `id` options. See https://uppy.io/docs/plugins/#id.';

      throw new Error(_msg);
    }

    if (Plugin.VERSION) {
      this.log("Using " + pluginId + " v" + Plugin.VERSION);
    }

    this.plugins[plugin.type].push(plugin);
    plugin.install();
    return this;
  }
  /**
   * Find one Plugin by name.
   *
   * @param {string} id plugin id
   * @returns {object|boolean}
   */
  ;

  _proto.getPlugin = function getPlugin(id) {
    var foundPlugin = null;
    this.iteratePlugins(function (plugin) {
      if (plugin.id === id) {
        foundPlugin = plugin;
        return false;
      }
    });
    return foundPlugin;
  }
  /**
   * Iterate through all `use`d plugins.
   *
   * @param {Function} method that will be run on each plugin
   */
  ;

  _proto.iteratePlugins = function iteratePlugins(method) {
    var _this7 = this;

    Object.keys(this.plugins).forEach(function (pluginType) {
      _this7.plugins[pluginType].forEach(method);
    });
  }
  /**
   * Uninstall and remove a plugin.
   *
   * @param {object} instance The plugin instance to remove.
   */
  ;

  _proto.removePlugin = function removePlugin(instance) {
    this.log("Removing plugin " + instance.id);
    this.emit('plugin-remove', instance);

    if (instance.uninstall) {
      instance.uninstall();
    }

    var list = this.plugins[instance.type].slice();
    var index = list.indexOf(instance);

    if (index !== -1) {
      list.splice(index, 1);
      this.plugins[instance.type] = list;
    }

    var updatedState = this.getState();
    delete updatedState.plugins[instance.id];
    this.setState(updatedState);
  }
  /**
   * Uninstall all plugins and close down this Uppy instance.
   */
  ;

  _proto.close = function close() {
    var _this8 = this;

    this.log("Closing Uppy instance " + this.opts.id + ": removing all files and uninstalling plugins");
    this.reset();

    this._storeUnsubscribe();

    this.iteratePlugins(function (plugin) {
      _this8.removePlugin(plugin);
    });
  }
  /**
   * Set info message in `state.info`, so that UI plugins like `Informer`
   * can display the message.
   *
   * @param {string | object} message Message to be displayed by the informer
   * @param {string} [type]
   * @param {number} [duration]
   */
  ;

  _proto.info = function info(message, type, duration) {
    if (type === void 0) {
      type = 'info';
    }

    if (duration === void 0) {
      duration = 3000;
    }

    var isComplexMessage = typeof message === 'object';
    this.setState({
      info: {
        isHidden: false,
        type: type,
        message: isComplexMessage ? message.message : message,
        details: isComplexMessage ? message.details : null
      }
    });
    this.emit('info-visible');
    clearTimeout(this.infoTimeoutID);

    if (duration === 0) {
      this.infoTimeoutID = undefined;
      return;
    } // hide the informer after `duration` milliseconds


    this.infoTimeoutID = setTimeout(this.hideInfo, duration);
  };

  _proto.hideInfo = function hideInfo() {
    var newInfo = _extends({}, this.getState().info, {
      isHidden: true
    });

    this.setState({
      info: newInfo
    });
    this.emit('info-hidden');
  }
  /**
   * Passes messages to a function, provided in `opts.logger`.
   * If `opts.logger: Uppy.debugLogger` or `opts.debug: true`, logs to the browser console.
   *
   * @param {string|object} message to log
   * @param {string} [type] optional `error` or `warning`
   */
  ;

  _proto.log = function log(message, type) {
    var logger = this.opts.logger;

    switch (type) {
      case 'error':
        logger.error(message);
        break;

      case 'warning':
        logger.warn(message);
        break;

      default:
        logger.debug(message);
        break;
    }
  }
  /**
   * Obsolete, event listeners are now added in the constructor.
   */
  ;

  _proto.run = function run() {
    this.log('Calling run() is no longer necessary.', 'warning');
    return this;
  }
  /**
   * Restore an upload by its ID.
   */
  ;

  _proto.restore = function restore(uploadID) {
    this.log("Core: attempting to restore upload \"" + uploadID + "\"");

    if (!this.getState().currentUploads[uploadID]) {
      this._removeUpload(uploadID);

      return Promise.reject(new Error('Nonexistent upload'));
    }

    return this._runUpload(uploadID);
  }
  /**
   * Create an upload for a bunch of files.
   *
   * @param {Array<string>} fileIDs File IDs to include in this upload.
   * @returns {string} ID of this upload.
   */
  ;

  _proto._createUpload = function _createUpload(fileIDs) {
    var _extends4;

    var _this$getState4 = this.getState(),
        allowNewUpload = _this$getState4.allowNewUpload,
        currentUploads = _this$getState4.currentUploads;

    if (!allowNewUpload) {
      throw new Error('Cannot create a new upload: already uploading.');
    }

    var uploadID = cuid();
    this.emit('upload', {
      id: uploadID,
      fileIDs: fileIDs
    });
    this.setState({
      allowNewUpload: this.opts.allowMultipleUploads !== false,
      currentUploads: _extends({}, currentUploads, (_extends4 = {}, _extends4[uploadID] = {
        fileIDs: fileIDs,
        step: 0,
        result: {}
      }, _extends4))
    });
    return uploadID;
  };

  _proto._getUpload = function _getUpload(uploadID) {
    var _this$getState5 = this.getState(),
        currentUploads = _this$getState5.currentUploads;

    return currentUploads[uploadID];
  }
  /**
   * Add data to an upload's result object.
   *
   * @param {string} uploadID The ID of the upload.
   * @param {object} data Data properties to add to the result object.
   */
  ;

  _proto.addResultData = function addResultData(uploadID, data) {
    var _extends5;

    if (!this._getUpload(uploadID)) {
      this.log("Not setting result for an upload that has been removed: " + uploadID);
      return;
    }

    var currentUploads = this.getState().currentUploads;

    var currentUpload = _extends({}, currentUploads[uploadID], {
      result: _extends({}, currentUploads[uploadID].result, data)
    });

    this.setState({
      currentUploads: _extends({}, currentUploads, (_extends5 = {}, _extends5[uploadID] = currentUpload, _extends5))
    });
  }
  /**
   * Remove an upload, eg. if it has been canceled or completed.
   *
   * @param {string} uploadID The ID of the upload.
   */
  ;

  _proto._removeUpload = function _removeUpload(uploadID) {
    var currentUploads = _extends({}, this.getState().currentUploads);

    delete currentUploads[uploadID];
    this.setState({
      currentUploads: currentUploads
    });
  }
  /**
   * Run an upload. This picks up where it left off in case the upload is being restored.
   *
   * @private
   */
  ;

  _proto._runUpload = function _runUpload(uploadID) {
    var _this9 = this;

    var uploadData = this.getState().currentUploads[uploadID];
    var restoreStep = uploadData.step;
    var steps = [].concat(this.preProcessors, this.uploaders, this.postProcessors);
    var lastStep = Promise.resolve();
    steps.forEach(function (fn, step) {
      // Skip this step if we are restoring and have already completed this step before.
      if (step < restoreStep) {
        return;
      }

      lastStep = lastStep.then(function () {
        var _extends6;

        var _this9$getState = _this9.getState(),
            currentUploads = _this9$getState.currentUploads;

        var currentUpload = currentUploads[uploadID];

        if (!currentUpload) {
          return;
        }

        var updatedUpload = _extends({}, currentUpload, {
          step: step
        });

        _this9.setState({
          currentUploads: _extends({}, currentUploads, (_extends6 = {}, _extends6[uploadID] = updatedUpload, _extends6))
        }); // TODO give this the `updatedUpload` object as its only parameter maybe?
        // Otherwise when more metadata may be added to the upload this would keep getting more parameters


        return fn(updatedUpload.fileIDs, uploadID);
      }).then(function (result) {
        return null;
      });
    }); // Not returning the `catch`ed promise, because we still want to return a rejected
    // promise from this method if the upload failed.

    lastStep.catch(function (err) {
      _this9.emit('error', err, uploadID);

      _this9._removeUpload(uploadID);
    });
    return lastStep.then(function () {
      // Set result data.
      var _this9$getState2 = _this9.getState(),
          currentUploads = _this9$getState2.currentUploads;

      var currentUpload = currentUploads[uploadID];

      if (!currentUpload) {
        return;
      }

      var files = currentUpload.fileIDs.map(function (fileID) {
        return _this9.getFile(fileID);
      });
      var successful = files.filter(function (file) {
        return !file.error;
      });
      var failed = files.filter(function (file) {
        return file.error;
      });

      _this9.addResultData(uploadID, {
        successful: successful,
        failed: failed,
        uploadID: uploadID
      });
    }).then(function () {
      // Emit completion events.
      // This is in a separate function so that the `currentUploads` variable
      // always refers to the latest state. In the handler right above it refers
      // to an outdated object without the `.result` property.
      var _this9$getState3 = _this9.getState(),
          currentUploads = _this9$getState3.currentUploads;

      if (!currentUploads[uploadID]) {
        return;
      }

      var currentUpload = currentUploads[uploadID];
      var result = currentUpload.result;

      _this9.emit('complete', result);

      _this9._removeUpload(uploadID);

      return result;
    }).then(function (result) {
      if (result == null) {
        _this9.log("Not setting result for an upload that has been removed: " + uploadID);
      }

      return result;
    });
  }
  /**
   * Start an upload for all the files that are not currently being uploaded.
   *
   * @returns {Promise}
   */
  ;

  _proto.upload = function upload() {
    var _this10 = this;

    if (!this.plugins.uploader) {
      this.log('No uploader type plugins are used', 'warning');
    }

    var files = this.getState().files;
    var onBeforeUploadResult = this.opts.onBeforeUpload(files);

    if (onBeforeUploadResult === false) {
      return Promise.reject(new Error('Not starting the upload because onBeforeUpload returned false'));
    }

    if (onBeforeUploadResult && typeof onBeforeUploadResult === 'object') {
      files = onBeforeUploadResult;
    }

    return Promise.resolve().then(function () {
      return _this10._checkMinNumberOfFiles(files);
    }).then(function () {
      var _this10$getState = _this10.getState(),
          currentUploads = _this10$getState.currentUploads; // get a list of files that are currently assigned to uploads


      var currentlyUploadingFiles = Object.keys(currentUploads).reduce(function (prev, curr) {
        return prev.concat(currentUploads[curr].fileIDs);
      }, []);
      var waitingFileIDs = [];
      Object.keys(files).forEach(function (fileID) {
        var file = _this10.getFile(fileID); // if the file hasn't started uploading and hasn't already been assigned to an upload..


        if (!file.progress.uploadStarted && currentlyUploadingFiles.indexOf(fileID) === -1) {
          waitingFileIDs.push(file.id);
        }
      });

      var uploadID = _this10._createUpload(waitingFileIDs);

      return _this10._runUpload(uploadID);
    }).catch(function (err) {
      _this10._showOrLogErrorAndThrow(err);
    });
  };

  _createClass(Uppy, [{
    key: "state",
    get: function get() {
      return this.getState();
    }
  }]);

  return Uppy;
}();

Uppy.VERSION = require('../package.json').version;

module.exports = function (opts) {
  return new Uppy(opts);
}; // Expose class constructor.


module.exports.Uppy = Uppy;
module.exports.Plugin = Plugin;
module.exports.debugLogger = debugLogger;

},{"../package.json":26,"./../../store-default":36,"./../../utils/lib/Translator":53,"./../../utils/lib/generateFileID":56,"./../../utils/lib/getFileNameAndExtension":62,"./../../utils/lib/getFileType":63,"./../../utils/lib/prettyBytes":70,"./Plugin":27,"./loggers":29,"./supportsUploadProgress":30,"cuid":1,"lodash.throttle":9,"mime-match":10,"namespace-emitter":11}],29:[function(require,module,exports){
var getTimeStamp = require('./../../utils/lib/getTimeStamp'); // Swallow logs, default if logger is not set or debug: false


var nullLogger = {
  debug: function debug() {},
  warn: function warn() {},
  error: function error() {} // Print logs to console with namespace + timestamp,
  // set by logger: Uppy.debugLogger or debug: true

};
var debugLogger = {
  debug: function debug() {
    // IE 10 doesn’t support console.debug
    var debug = console.debug || console.log;

    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    debug.call.apply(debug, [console, "[Uppy] [" + getTimeStamp() + "]"].concat(args));
  },
  warn: function warn() {
    var _console;

    for (var _len2 = arguments.length, args = new Array(_len2), _key2 = 0; _key2 < _len2; _key2++) {
      args[_key2] = arguments[_key2];
    }

    return (_console = console).warn.apply(_console, ["[Uppy] [" + getTimeStamp() + "]"].concat(args));
  },
  error: function error() {
    var _console2;

    for (var _len3 = arguments.length, args = new Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
      args[_key3] = arguments[_key3];
    }

    return (_console2 = console).error.apply(_console2, ["[Uppy] [" + getTimeStamp() + "]"].concat(args));
  }
};
module.exports = {
  nullLogger: nullLogger,
  debugLogger: debugLogger
};

},{"./../../utils/lib/getTimeStamp":65}],30:[function(require,module,exports){
// Edge 15.x does not fire 'progress' events on uploads.
// See https://github.com/transloadit/uppy/issues/945
// And https://developer.microsoft.com/en-us/microsoft-edge/platform/issues/12224510/
module.exports = function supportsUploadProgress(userAgent) {
  // Allow passing in userAgent for tests
  if (userAgent == null) {
    userAgent = typeof navigator !== 'undefined' ? navigator.userAgent : null;
  } // Assume it works because basically everything supports progress events.


  if (!userAgent) return true;
  var m = /Edge\/(\d+\.\d+)/.exec(userAgent);
  if (!m) return true;
  var edgeVersion = m[1];

  var _edgeVersion$split = edgeVersion.split('.'),
      major = _edgeVersion$split[0],
      minor = _edgeVersion$split[1];

  major = parseInt(major, 10);
  minor = parseInt(minor, 10); // Worked before:
  // Edge 40.15063.0.0
  // Microsoft EdgeHTML 15.15063

  if (major < 15 || major === 15 && minor < 15063) {
    return true;
  } // Fixed in:
  // Microsoft EdgeHTML 18.18218


  if (major > 18 || major === 18 && minor >= 18218) {
    return true;
  } // other versions don't work.


  return false;
};

},{}],31:[function(require,module,exports){
module.exports={
  "name": "@uppy/drag-drop",
  "description": "Droppable zone UI for Uppy. Drag and drop files into it to upload.",
  "version": "1.3.1",
  "license": "MIT",
  "main": "lib/index.js",
  "style": "dist/style.min.css",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-plugin",
    "drag-drop",
    "drag",
    "drop",
    "dropzone",
    "upload"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  },
  "dependencies": {
    "@uppy/utils": "file:../utils",
    "preact": "8.2.9"
  },
  "peerDependencies": {
    "@uppy/core": "^1.0.0"
  }
}

},{}],32:[function(require,module,exports){
var _class, _temp;

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

var _require = require('./../../core'),
    Plugin = _require.Plugin;

var Translator = require('./../../utils/lib/Translator');

var toArray = require('./../../utils/lib/toArray');

var isDragDropSupported = require('./../../utils/lib/isDragDropSupported');

var getDroppedFiles = require('./../../utils/lib/getDroppedFiles');

var _require2 = require('preact'),
    h = _require2.h;
/**
 * Drag & Drop plugin
 *
 */


module.exports = (_temp = _class =
/*#__PURE__*/
function (_Plugin) {
  _inheritsLoose(DragDrop, _Plugin);

  function DragDrop(uppy, opts) {
    var _this;

    _this = _Plugin.call(this, uppy, opts) || this;
    _this.type = 'acquirer';
    _this.id = _this.opts.id || 'DragDrop';
    _this.title = 'Drag & Drop';
    _this.defaultLocale = {
      strings: {
        dropHereOr: 'Drop files here or %{browse}',
        browse: 'browse'
      } // Default options

    };
    var defaultOpts = {
      target: null,
      inputName: 'files[]',
      width: '100%',
      height: '100%',
      note: null // Merge default options with the ones set by user

    };
    _this.opts = _extends({}, defaultOpts, opts); // Check for browser dragDrop support

    _this.isDragDropSupported = isDragDropSupported();
    _this.removeDragOverClassTimeout = null; // i18n

    _this.translator = new Translator([_this.defaultLocale, _this.uppy.locale, _this.opts.locale]);
    _this.i18n = _this.translator.translate.bind(_this.translator);
    _this.i18nArray = _this.translator.translateArray.bind(_this.translator); // Bind `this` to class methods

    _this.handleInputChange = _this.handleInputChange.bind(_assertThisInitialized(_this));
    _this.handleDragOver = _this.handleDragOver.bind(_assertThisInitialized(_this));
    _this.handleDragLeave = _this.handleDragLeave.bind(_assertThisInitialized(_this));
    _this.handleDrop = _this.handleDrop.bind(_assertThisInitialized(_this));
    _this.addFile = _this.addFile.bind(_assertThisInitialized(_this));
    _this.render = _this.render.bind(_assertThisInitialized(_this));
    return _this;
  }

  var _proto = DragDrop.prototype;

  _proto.addFile = function addFile(file) {
    try {
      this.uppy.addFile({
        source: this.id,
        name: file.name,
        type: file.type,
        data: file,
        meta: {
          relativePath: file.relativePath || null
        }
      });
    } catch (err) {
      if (!err.isRestriction) {
        this.uppy.log(err);
      }
    }
  };

  _proto.handleInputChange = function handleInputChange(event) {
    this.uppy.log('[DragDrop] Files selected through input');
    var files = toArray(event.target.files);
    files.forEach(this.addFile); // We clear the input after a file is selected, because otherwise
    // change event is not fired in Chrome and Safari when a file
    // with the same name is selected.
    // ___Why not use value="" on <input/> instead?
    //    Because if we use that method of clearing the input,
    //    Chrome will not trigger change if we drop the same file twice (Issue #768).

    event.target.value = null;
  };

  _proto.handleDrop = function handleDrop(event, dropCategory) {
    var _this2 = this;

    event.preventDefault();
    event.stopPropagation();
    clearTimeout(this.removeDragOverClassTimeout); // 1. Add a small (+) icon on drop

    event.dataTransfer.dropEffect = 'copy'; // 2. Remove dragover class

    this.setPluginState({
      isDraggingOver: false
    }); // 3. Add all dropped files

    this.uppy.log('[DragDrop] Files were dropped');

    var logDropError = function logDropError(error) {
      _this2.uppy.log(error, 'error');
    };

    getDroppedFiles(event.dataTransfer, {
      logDropError: logDropError
    }).then(function (files) {
      files.forEach(_this2.addFile);
    });
  };

  _proto.handleDragOver = function handleDragOver(event) {
    event.preventDefault();
    event.stopPropagation();
    clearTimeout(this.removeDragOverClassTimeout);
    this.setPluginState({
      isDraggingOver: true
    });
  };

  _proto.handleDragLeave = function handleDragLeave(event) {
    var _this3 = this;

    event.preventDefault();
    event.stopPropagation();
    clearTimeout(this.removeDragOverClassTimeout); // Timeout against flickering, this solution is taken from drag-drop library. Solution with 'pointer-events: none' didn't work across browsers.

    this.removeDragOverClassTimeout = setTimeout(function () {
      _this3.setPluginState({
        isDraggingOver: false
      });
    }, 50);
  };

  _proto.renderHiddenFileInput = function renderHiddenFileInput() {
    var _this4 = this;

    var restrictions = this.uppy.opts.restrictions;
    return h("input", {
      class: "uppy-DragDrop-input",
      type: "file",
      tabindex: -1,
      focusable: "false",
      ref: function ref(_ref) {
        _this4.fileInputRef = _ref;
      },
      name: this.opts.inputName,
      multiple: restrictions.maxNumberOfFiles !== 1,
      accept: restrictions.allowedFileTypes,
      onchange: this.handleInputChange
    });
  };

  _proto.renderArrowSvg = function renderArrowSvg() {
    return h("svg", {
      "aria-hidden": "true",
      focusable: "false",
      class: "UppyIcon uppy-DragDrop-arrow",
      width: "16",
      height: "16",
      viewBox: "0 0 16 16"
    }, h("path", {
      d: "M11 10V0H5v10H2l6 6 6-6h-3zm0 0",
      "fill-rule": "evenodd"
    }));
  };

  _proto.renderLabel = function renderLabel() {
    return h("div", {
      class: "uppy-DragDrop-label"
    }, this.i18nArray('dropHereOr', {
      browse: h("span", {
        class: "uppy-DragDrop-browse"
      }, this.i18n('browse'))
    }));
  };

  _proto.renderNote = function renderNote() {
    return h("span", {
      class: "uppy-DragDrop-note"
    }, this.opts.note);
  };

  _proto.render = function render(state) {
    var _this5 = this;

    var dragDropClass = "\n      uppy-Root\n      uppy-u-reset\n      uppy-DragDrop-container\n      " + (this.isDragDropSupported ? 'uppy-DragDrop--is-dragdrop-supported' : '') + "\n      " + (this.getPluginState().isDraggingOver ? 'uppy-DragDrop--isDraggingOver' : '') + "\n    ";
    var dragDropStyle = {
      width: this.opts.width,
      height: this.opts.height
    };
    return h("button", {
      type: "button",
      class: dragDropClass,
      style: dragDropStyle,
      onClick: function onClick() {
        return _this5.fileInputRef.click();
      },
      onDragOver: this.handleDragOver,
      onDragLeave: this.handleDragLeave,
      onDrop: this.handleDrop
    }, this.renderHiddenFileInput(), h("div", {
      class: "uppy-DragDrop-inner"
    }, this.renderArrowSvg(), this.renderLabel(), this.renderNote()));
  };

  _proto.install = function install() {
    this.setPluginState({
      isDraggingOver: false
    });
    var target = this.opts.target;

    if (target) {
      this.mount(target, this);
    }
  };

  _proto.uninstall = function uninstall() {
    this.unmount();
  };

  return DragDrop;
}(Plugin), _class.VERSION = require('../package.json').version, _temp);

},{"../package.json":31,"./../../core":28,"./../../utils/lib/Translator":53,"./../../utils/lib/getDroppedFiles":57,"./../../utils/lib/isDragDropSupported":68,"./../../utils/lib/toArray":72,"preact":12}],33:[function(require,module,exports){
module.exports={
  "name": "@uppy/progress-bar",
  "description": "A progress bar UI for Uppy",
  "version": "1.3.1",
  "license": "MIT",
  "main": "lib/index.js",
  "style": "dist/style.min.css",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-plugin",
    "progress",
    "progress bar",
    "upload progress"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  },
  "dependencies": {
    "@uppy/utils": "file:../utils",
    "preact": "8.2.9"
  },
  "peerDependencies": {
    "@uppy/core": "^1.0.0"
  }
}

},{}],34:[function(require,module,exports){
var _class, _temp;

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

var _require = require('./../../core'),
    Plugin = _require.Plugin;

var _require2 = require('preact'),
    h = _require2.h;
/**
 * Progress bar
 *
 */


module.exports = (_temp = _class =
/*#__PURE__*/
function (_Plugin) {
  _inheritsLoose(ProgressBar, _Plugin);

  function ProgressBar(uppy, opts) {
    var _this;

    _this = _Plugin.call(this, uppy, opts) || this;
    _this.id = _this.opts.id || 'ProgressBar';
    _this.title = 'Progress Bar';
    _this.type = 'progressindicator'; // set default options

    var defaultOptions = {
      target: 'body',
      replaceTargetContent: false,
      fixed: false,
      hideAfterFinish: true // merge default options with the ones set by user

    };
    _this.opts = _extends({}, defaultOptions, opts);
    _this.render = _this.render.bind(_assertThisInitialized(_this));
    return _this;
  }

  var _proto = ProgressBar.prototype;

  _proto.render = function render(state) {
    var progress = state.totalProgress || 0;
    var isHidden = progress === 100 && this.opts.hideAfterFinish;
    return h("div", {
      class: "uppy uppy-ProgressBar",
      style: {
        position: this.opts.fixed ? 'fixed' : 'initial'
      },
      "aria-hidden": isHidden
    }, h("div", {
      class: "uppy-ProgressBar-inner",
      style: {
        width: progress + '%'
      }
    }), h("div", {
      class: "uppy-ProgressBar-percentage"
    }, progress));
  };

  _proto.install = function install() {
    var target = this.opts.target;

    if (target) {
      this.mount(target, this);
    }
  };

  _proto.uninstall = function uninstall() {
    this.unmount();
  };

  return ProgressBar;
}(Plugin), _class.VERSION = require('../package.json').version, _temp);

},{"../package.json":33,"./../../core":28,"preact":12}],35:[function(require,module,exports){
module.exports={
  "name": "@uppy/store-default",
  "description": "The default simple object-based store for Uppy.",
  "version": "1.2.0",
  "license": "MIT",
  "main": "lib/index.js",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-store"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  }
}

},{}],36:[function(require,module,exports){
function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

/**
 * Default store that keeps state in a simple object.
 */
var DefaultStore =
/*#__PURE__*/
function () {
  function DefaultStore() {
    this.state = {};
    this.callbacks = [];
  }

  var _proto = DefaultStore.prototype;

  _proto.getState = function getState() {
    return this.state;
  };

  _proto.setState = function setState(patch) {
    var prevState = _extends({}, this.state);

    var nextState = _extends({}, this.state, patch);

    this.state = nextState;

    this._publish(prevState, nextState, patch);
  };

  _proto.subscribe = function subscribe(listener) {
    var _this = this;

    this.callbacks.push(listener);
    return function () {
      // Remove the listener.
      _this.callbacks.splice(_this.callbacks.indexOf(listener), 1);
    };
  };

  _proto._publish = function _publish() {
    for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
      args[_key] = arguments[_key];
    }

    this.callbacks.forEach(function (listener) {
      listener.apply(void 0, args);
    });
  };

  return DefaultStore;
}();

DefaultStore.VERSION = require('../package.json').version;

module.exports = function defaultStore() {
  return new DefaultStore();
};

},{"../package.json":35}],37:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = fingerprint;

var _isReactNative = require("./isReactNative");

var _isReactNative2 = _interopRequireDefault(_isReactNative);

var _isCordova = require("./isCordova");

var _isCordova2 = _interopRequireDefault(_isCordova);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

/**
 * Generate a fingerprint for a file which will be used the store the endpoint
 *
 * @param {File} file
 * @param {Object} options
 * @param {Function} callback
 */
function fingerprint(file, options, callback) {
  if ((0, _isCordova2.default)()) {
    return callback(new Error("Fingerprint cannot be computed for file input type"));
  }

  if ((0, _isReactNative2.default)()) {
    return callback(null, reactNativeFingerprint(file, options));
  }

  return callback(null, ["tus-br", file.name, file.type, file.size, file.lastModified, options.endpoint].join("-"));
}

function reactNativeFingerprint(file, options) {
  var exifHash = file.exif ? hashCode(JSON.stringify(file.exif)) : "noexif";
  return ["tus-rn", file.name || "noname", file.size || "nosize", exifHash, options.endpoint].join("/");
}

function hashCode(str) {
  // from https://stackoverflow.com/a/8831937/151666
  var hash = 0;
  if (str.length === 0) {
    return hash;
  }
  for (var i = 0; i < str.length; i++) {
    var char = str.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return hash;
}
},{"./isCordova":38,"./isReactNative":39}],38:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var isCordova = function isCordova() {
  return typeof window != "undefined" && (typeof window.PhoneGap != "undefined" || typeof window.Cordova != "undefined" || typeof window.cordova != "undefined");
};

exports.default = isCordova;
},{}],39:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var isReactNative = function isReactNative() {
  return typeof navigator !== "undefined" && typeof navigator.product === "string" && navigator.product.toLowerCase() === "reactnative";
};

exports.default = isReactNative;
},{}],40:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
/**
 * readAsByteArray converts a File object to a Uint8Array.
 * This function is only used on the Apache Cordova platform.
 * See https://cordova.apache.org/docs/en/latest/reference/cordova-plugin-file/index.html#read-a-file
 */
function readAsByteArray(chunk, callback) {
  var reader = new FileReader();
  reader.onload = function () {
    callback(null, new Uint8Array(reader.result));
  };
  reader.onerror = function (err) {
    callback(err);
  };
  reader.readAsArrayBuffer(chunk);
}

exports.default = readAsByteArray;
},{}],41:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.newRequest = newRequest;
exports.resolveUrl = resolveUrl;

var _urlParse = require("url-parse");

var _urlParse2 = _interopRequireDefault(_urlParse);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function newRequest() {
  return new window.XMLHttpRequest();
} /* global window */
function resolveUrl(origin, link) {
  return new _urlParse2.default(link, origin).toString();
}
},{"url-parse":16}],42:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

exports.getSource = getSource;

var _isReactNative = require("./isReactNative");

var _isReactNative2 = _interopRequireDefault(_isReactNative);

var _uriToBlob = require("./uriToBlob");

var _uriToBlob2 = _interopRequireDefault(_uriToBlob);

var _isCordova = require("./isCordova");

var _isCordova2 = _interopRequireDefault(_isCordova);

var _readAsByteArray = require("./readAsByteArray");

var _readAsByteArray2 = _interopRequireDefault(_readAsByteArray);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var FileSource = function () {
  function FileSource(file) {
    _classCallCheck(this, FileSource);

    this._file = file;
    this.size = file.size;
  }

  _createClass(FileSource, [{
    key: "slice",
    value: function slice(start, end, callback) {
      // In Apache Cordova applications, a File must be resolved using
      // FileReader instances, see
      // https://cordova.apache.org/docs/en/8.x/reference/cordova-plugin-file/index.html#read-a-file
      if ((0, _isCordova2.default)()) {
        (0, _readAsByteArray2.default)(this._file.slice(start, end), function (err, chunk) {
          if (err) return callback(err);

          callback(null, chunk);
        });
        return;
      }

      callback(null, this._file.slice(start, end));
    }
  }, {
    key: "close",
    value: function close() {}
  }]);

  return FileSource;
}();

var StreamSource = function () {
  function StreamSource(reader, chunkSize) {
    _classCallCheck(this, StreamSource);

    this._chunkSize = chunkSize;
    this._buffer = undefined;
    this._bufferOffset = 0;
    this._reader = reader;
    this._done = false;
  }

  _createClass(StreamSource, [{
    key: "slice",
    value: function slice(start, end, callback) {
      if (start < this._bufferOffset) {
        callback(new Error("Requested data is before the reader's current offset"));
        return;
      }

      return this._readUntilEnoughDataOrDone(start, end, callback);
    }
  }, {
    key: "_readUntilEnoughDataOrDone",
    value: function _readUntilEnoughDataOrDone(start, end, callback) {
      var _this = this;

      var hasEnoughData = end <= this._bufferOffset + len(this._buffer);
      if (this._done || hasEnoughData) {
        var value = this._getDataFromBuffer(start, end);
        callback(null, value, value == null ? this._done : false);
        return;
      }
      this._reader.read().then(function (_ref) {
        var value = _ref.value,
            done = _ref.done;

        if (done) {
          _this._done = true;
        } else if (_this._buffer === undefined) {
          _this._buffer = value;
        } else {
          _this._buffer = concat(_this._buffer, value);
        }

        _this._readUntilEnoughDataOrDone(start, end, callback);
      }).catch(function (err) {
        callback(new Error("Error during read: " + err));
      });
    }
  }, {
    key: "_getDataFromBuffer",
    value: function _getDataFromBuffer(start, end) {
      // Remove data from buffer before `start`.
      // Data might be reread from the buffer if an upload fails, so we can only
      // safely delete data when it comes *before* what is currently being read.
      if (start > this._bufferOffset) {
        this._buffer = this._buffer.slice(start - this._bufferOffset);
        this._bufferOffset = start;
      }
      // If the buffer is empty after removing old data, all data has been read.
      var hasAllDataBeenRead = len(this._buffer) === 0;
      if (this._done && hasAllDataBeenRead) {
        return null;
      }
      // We already removed data before `start`, so we just return the first
      // chunk from the buffer.
      return this._buffer.slice(0, end - start);
    }
  }, {
    key: "close",
    value: function close() {
      if (this._reader.cancel) {
        this._reader.cancel();
      }
    }
  }]);

  return StreamSource;
}();

function len(blobOrArray) {
  if (blobOrArray === undefined) return 0;
  if (blobOrArray.size !== undefined) return blobOrArray.size;
  return blobOrArray.length;
}

/*
  Typed arrays and blobs don't have a concat method.
  This function helps StreamSource accumulate data to reach chunkSize.
*/
function concat(a, b) {
  if (a.concat) {
    // Is `a` an Array?
    return a.concat(b);
  }
  if (a instanceof Blob) {
    return new Blob([a, b], { type: a.type });
  }
  if (a.set) {
    // Is `a` a typed array?
    var c = new a.constructor(a.length + b.length);
    c.set(a);
    c.set(b, a.length);
    return c;
  }
  throw new Error("Unknown data type");
}

function getSource(input, chunkSize, callback) {
  // In React Native, when user selects a file, instead of a File or Blob,
  // you usually get a file object {} with a uri property that contains
  // a local path to the file. We use XMLHttpRequest to fetch
  // the file blob, before uploading with tus.
  if ((0, _isReactNative2.default)() && input && typeof input.uri !== "undefined") {
    (0, _uriToBlob2.default)(input.uri, function (err, blob) {
      if (err) {
        return callback(new Error("tus: cannot fetch `file.uri` as Blob, make sure the uri is correct and accessible. " + err));
      }
      callback(null, new FileSource(blob));
    });
    return;
  }

  // Since we emulate the Blob type in our tests (not all target browsers
  // support it), we cannot use `instanceof` for testing whether the input value
  // can be handled. Instead, we simply check is the slice() function and the
  // size property are available.
  if (typeof input.slice === "function" && typeof input.size !== "undefined") {
    callback(null, new FileSource(input));
    return;
  }

  if (typeof input.read === "function") {
    chunkSize = +chunkSize;
    if (!isFinite(chunkSize)) {
      callback(new Error("cannot create source for stream without a finite value for the `chunkSize` option"));
      return;
    }
    callback(null, new StreamSource(input, chunkSize));
    return;
  }

  callback(new Error("source object may only be an instance of File, Blob, or Reader in this environment"));
}
},{"./isCordova":38,"./isReactNative":39,"./readAsByteArray":40,"./uriToBlob":44}],43:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

exports.getStorage = getStorage;

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

/* global window, localStorage */

var hasStorage = false;
try {
  hasStorage = "localStorage" in window;

  // Attempt to store and read entries from the local storage to detect Private
  // Mode on Safari on iOS (see #49)
  var key = "tusSupport";
  localStorage.setItem(key, localStorage.getItem(key));
} catch (e) {
  // If we try to access localStorage inside a sandboxed iframe, a SecurityError
  // is thrown. When in private mode on iOS Safari, a QuotaExceededError is
  // thrown (see #49)
  if (e.code === e.SECURITY_ERR || e.code === e.QUOTA_EXCEEDED_ERR) {
    hasStorage = false;
  } else {
    throw e;
  }
}

var canStoreURLs = exports.canStoreURLs = hasStorage;

var LocalStorage = function () {
  function LocalStorage() {
    _classCallCheck(this, LocalStorage);
  }

  _createClass(LocalStorage, [{
    key: "setItem",
    value: function setItem(key, value, cb) {
      cb(null, localStorage.setItem(key, value));
    }
  }, {
    key: "getItem",
    value: function getItem(key, cb) {
      cb(null, localStorage.getItem(key));
    }
  }, {
    key: "removeItem",
    value: function removeItem(key, cb) {
      cb(null, localStorage.removeItem(key));
    }
  }]);

  return LocalStorage;
}();

function getStorage() {
  return hasStorage ? new LocalStorage() : null;
}
},{}],44:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
/**
 * uriToBlob resolves a URI to a Blob object. This is used for
 * React Native to retrieve a file (identified by a file://
 * URI) as a blob.
 */
function uriToBlob(uri, done) {
  var xhr = new XMLHttpRequest();
  xhr.responseType = "blob";
  xhr.onload = function () {
    var blob = xhr.response;
    done(null, blob);
  };
  xhr.onerror = function (err) {
    done(err);
  };
  xhr.open("GET", uri);
  xhr.send();
}

exports.default = uriToBlob;
},{}],45:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var DetailedError = function (_Error) {
  _inherits(DetailedError, _Error);

  function DetailedError(error) {
    var causingErr = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : null;
    var xhr = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : null;

    _classCallCheck(this, DetailedError);

    var _this = _possibleConstructorReturn(this, (DetailedError.__proto__ || Object.getPrototypeOf(DetailedError)).call(this, error.message));

    _this.originalRequest = xhr;
    _this.causingError = causingErr;

    var message = error.message;
    if (causingErr != null) {
      message += ", caused by " + causingErr.toString();
    }
    if (xhr != null) {
      message += ", originated from request (response code: " + xhr.status + ", response text: " + xhr.responseText + ")";
    }
    _this.message = message;
    return _this;
  }

  return DetailedError;
}(Error);

exports.default = DetailedError;
},{}],46:[function(require,module,exports){
"use strict";

var _upload = require("./upload");

var _upload2 = _interopRequireDefault(_upload);

var _storage = require("./node/storage");

var storage = _interopRequireWildcard(_storage);

function _interopRequireWildcard(obj) { if (obj && obj.__esModule) { return obj; } else { var newObj = {}; if (obj != null) { for (var key in obj) { if (Object.prototype.hasOwnProperty.call(obj, key)) newObj[key] = obj[key]; } } newObj.default = obj; return newObj; } }

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

/* global window */
var defaultOptions = _upload2.default.defaultOptions;


var moduleExport = {
  Upload: _upload2.default,
  canStoreURLs: storage.canStoreURLs,
  defaultOptions: defaultOptions
};

if (typeof window !== "undefined") {
  // Browser environment using XMLHttpRequest
  var _window = window,
      XMLHttpRequest = _window.XMLHttpRequest,
      Blob = _window.Blob;


  moduleExport.isSupported = XMLHttpRequest && Blob && typeof Blob.prototype.slice === "function";
} else {
  // Node.js environment using http module
  moduleExport.isSupported = true;
  // make FileStorage module available as it will not be set by default.
  moduleExport.FileStorage = storage.FileStorage;
}

// The usage of the commonjs exporting syntax instead of the new ECMAScript
// one is actually inteded and prevents weird behaviour if we are trying to
// import this module in another module using Babel.
module.exports = moduleExport;
},{"./node/storage":43,"./upload":47}],47:[function(require,module,exports){
"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }(); /* global window */


// We import the files used inside the Node environment which are rewritten
// for browsers using the rules defined in the package.json


var _error = require("./error");

var _error2 = _interopRequireDefault(_error);

var _extend = require("extend");

var _extend2 = _interopRequireDefault(_extend);

var _jsBase = require("js-base64");

var _request = require("./node/request");

var _source = require("./node/source");

var _storage = require("./node/storage");

var _fingerprint = require("./node/fingerprint");

var _fingerprint2 = _interopRequireDefault(_fingerprint);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var defaultOptions = {
  endpoint: null,
  fingerprint: _fingerprint2.default,
  resume: true,
  onProgress: null,
  onChunkComplete: null,
  onSuccess: null,
  onError: null,
  headers: {},
  chunkSize: Infinity,
  withCredentials: false,
  uploadUrl: null,
  uploadSize: null,
  overridePatchMethod: false,
  retryDelays: null,
  removeFingerprintOnSuccess: false,
  uploadLengthDeferred: false,
  urlStorage: null,
  fileReader: null
};

var Upload = function () {
  function Upload(file, options) {
    _classCallCheck(this, Upload);

    this.options = (0, _extend2.default)(true, {}, defaultOptions, options);

    // The storage module used to store URLs
    this._storage = this.options.urlStorage;

    // The underlying File/Blob object
    this.file = file;

    // The URL against which the file will be uploaded
    this.url = null;

    // The underlying XHR object for the current PATCH request
    this._xhr = null;

    // The fingerpinrt for the current file (set after start())
    this._fingerprint = null;

    // The offset used in the current PATCH request
    this._offset = null;

    // True if the current PATCH request has been aborted
    this._aborted = false;

    // The file's size in bytes
    this._size = null;

    // The Source object which will wrap around the given file and provides us
    // with a unified interface for getting its size and slice chunks from its
    // content allowing us to easily handle Files, Blobs, Buffers and Streams.
    this._source = null;

    // The current count of attempts which have been made. Null indicates none.
    this._retryAttempt = 0;

    // The timeout's ID which is used to delay the next retry
    this._retryTimeout = null;

    // The offset of the remote upload before the latest attempt was started.
    this._offsetBeforeRetry = 0;
  }

  _createClass(Upload, [{
    key: "start",
    value: function start() {
      var _this = this;

      var file = this.file;

      if (!file) {
        this._emitError(new Error("tus: no file or stream to upload provided"));
        return;
      }

      if (!this.options.endpoint && !this.options.uploadUrl) {
        this._emitError(new Error("tus: neither an endpoint or an upload URL is provided"));
        return;
      }

      if (this.options.resume && this._storage == null) {
        this._storage = (0, _storage.getStorage)();
      }

      if (this._source) {
        this._start(this._source);
      } else {
        var fileReader = this.options.fileReader || _source.getSource;
        fileReader(file, this.options.chunkSize, function (err, source) {
          if (err) {
            _this._emitError(err);
            return;
          }

          _this._source = source;
          _this._start(source);
        });
      }
    }
  }, {
    key: "_start",
    value: function _start(source) {
      var _this2 = this;

      var file = this.file;

      // First, we look at the uploadLengthDeferred option.
      // Next, we check if the caller has supplied a manual upload size.
      // Finally, we try to use the calculated size from the source object.
      if (this.options.uploadLengthDeferred) {
        this._size = null;
      } else if (this.options.uploadSize != null) {
        this._size = +this.options.uploadSize;
        if (isNaN(this._size)) {
          this._emitError(new Error("tus: cannot convert `uploadSize` option into a number"));
          return;
        }
      } else {
        this._size = source.size;
        if (this._size == null) {
          this._emitError(new Error("tus: cannot automatically derive upload's size from input and must be specified manually using the `uploadSize` option"));
          return;
        }
      }

      var retryDelays = this.options.retryDelays;
      if (retryDelays != null) {
        if (Object.prototype.toString.call(retryDelays) !== "[object Array]") {
          this._emitError(new Error("tus: the `retryDelays` option must either be an array or null"));
          return;
        } else {
          var errorCallback = this.options.onError;
          this.options.onError = function (err) {
            // Restore the original error callback which may have been set.
            _this2.options.onError = errorCallback;

            // We will reset the attempt counter if
            // - we were already able to connect to the server (offset != null) and
            // - we were able to upload a small chunk of data to the server
            var shouldResetDelays = _this2._offset != null && _this2._offset > _this2._offsetBeforeRetry;
            if (shouldResetDelays) {
              _this2._retryAttempt = 0;
            }

            var isOnline = true;
            if (typeof window !== "undefined" && "navigator" in window && window.navigator.onLine === false) {
              isOnline = false;
            }

            // We only attempt a retry if
            // - we didn't exceed the maxium number of retries, yet, and
            // - this error was caused by a request or it's response and
            // - the error is server error (i.e. no a status 4xx or a 409 or 423) and
            // - the browser does not indicate that we are offline
            var status = err.originalRequest ? err.originalRequest.status : 0;
            var isServerError = !inStatusCategory(status, 400) || status === 409 || status === 423;
            var shouldRetry = _this2._retryAttempt < retryDelays.length && err.originalRequest != null && isServerError && isOnline;

            if (!shouldRetry) {
              _this2._emitError(err);
              return;
            }

            var delay = retryDelays[_this2._retryAttempt++];

            _this2._offsetBeforeRetry = _this2._offset;
            _this2.options.uploadUrl = _this2.url;

            _this2._retryTimeout = setTimeout(function () {
              _this2.start();
            }, delay);
          };
        }
      }

      // Reset the aborted flag when the upload is started or else the
      // _startUpload will stop before sending a request if the upload has been
      // aborted previously.
      this._aborted = false;

      // The upload had been started previously and we should reuse this URL.
      if (this.url != null) {
        this._resumeUpload();
        return;
      }

      // A URL has manually been specified, so we try to resume
      if (this.options.uploadUrl != null) {
        this.url = this.options.uploadUrl;
        this._resumeUpload();
        return;
      }

      // Try to find the endpoint for the file in the storage
      if (this._hasStorage()) {
        this.options.fingerprint(file, this.options, function (err, fingerprintValue) {
          if (err) {
            _this2._emitError(err);
            return;
          }

          _this2._fingerprint = fingerprintValue;
          _this2._storage.getItem(_this2._fingerprint, function (err, resumedUrl) {
            if (err) {
              _this2._emitError(err);
              return;
            }

            if (resumedUrl != null) {
              _this2.url = resumedUrl;
              _this2._resumeUpload();
            } else {
              _this2._createUpload();
            }
          });
        });
      } else {
        // An upload has not started for the file yet, so we start a new one
        this._createUpload();
      }
    }
  }, {
    key: "abort",
    value: function abort(shouldTerminate, cb) {
      var _this3 = this;

      if (this._xhr !== null) {
        this._xhr.abort();
        this._source.close();
      }
      this._aborted = true;

      if (this._retryTimeout != null) {
        clearTimeout(this._retryTimeout);
        this._retryTimeout = null;
      }

      cb = cb || function () {};
      if (shouldTerminate) {
        Upload.terminate(this.url, this.options, function (err, xhr) {
          if (err) {
            return cb(err, xhr);
          }

          _this3._hasStorage() ? _this3._storage.removeItem(_this3._fingerprint, cb) : cb();
        });
      } else {
        cb();
      }
    }
  }, {
    key: "_hasStorage",
    value: function _hasStorage() {
      return this.options.resume && this._storage;
    }
  }, {
    key: "_emitXhrError",
    value: function _emitXhrError(xhr, err, causingErr) {
      this._emitError(new _error2.default(err, causingErr, xhr));
    }
  }, {
    key: "_emitError",
    value: function _emitError(err) {
      if (typeof this.options.onError === "function") {
        this.options.onError(err);
      } else {
        throw err;
      }
    }
  }, {
    key: "_emitSuccess",
    value: function _emitSuccess() {
      if (typeof this.options.onSuccess === "function") {
        this.options.onSuccess();
      }
    }

    /**
     * Publishes notification when data has been sent to the server. This
     * data may not have been accepted by the server yet.
     * @param  {number} bytesSent  Number of bytes sent to the server.
     * @param  {number} bytesTotal Total number of bytes to be sent to the server.
     */

  }, {
    key: "_emitProgress",
    value: function _emitProgress(bytesSent, bytesTotal) {
      if (typeof this.options.onProgress === "function") {
        this.options.onProgress(bytesSent, bytesTotal);
      }
    }

    /**
     * Publishes notification when a chunk of data has been sent to the server
     * and accepted by the server.
     * @param  {number} chunkSize  Size of the chunk that was accepted by the
     *                             server.
     * @param  {number} bytesAccepted Total number of bytes that have been
     *                                accepted by the server.
     * @param  {number} bytesTotal Total number of bytes to be sent to the server.
     */

  }, {
    key: "_emitChunkComplete",
    value: function _emitChunkComplete(chunkSize, bytesAccepted, bytesTotal) {
      if (typeof this.options.onChunkComplete === "function") {
        this.options.onChunkComplete(chunkSize, bytesAccepted, bytesTotal);
      }
    }

    /**
     * Set the headers used in the request and the withCredentials property
     * as defined in the options
     *
     * @param {XMLHttpRequest} xhr
     */

  }, {
    key: "_setupXHR",
    value: function _setupXHR(xhr) {
      this._xhr = xhr;
      setupXHR(xhr, this.options);
    }

    /**
     * Create a new upload using the creation extension by sending a POST
     * request to the endpoint. After successful creation the file will be
     * uploaded
     *
     * @api private
     */

  }, {
    key: "_createUpload",
    value: function _createUpload() {
      var _this4 = this;

      if (!this.options.endpoint) {
        this._emitError(new Error("tus: unable to create upload because no endpoint is provided"));
        return;
      }

      var xhr = (0, _request.newRequest)();
      xhr.open("POST", this.options.endpoint, true);

      xhr.onload = function () {
        if (!inStatusCategory(xhr.status, 200)) {
          _this4._emitXhrError(xhr, new Error("tus: unexpected response while creating upload"));
          return;
        }

        var location = xhr.getResponseHeader("Location");
        if (location == null) {
          _this4._emitXhrError(xhr, new Error("tus: invalid or missing Location header"));
          return;
        }

        _this4.url = (0, _request.resolveUrl)(_this4.options.endpoint, location);

        if (_this4._size === 0) {
          // Nothing to upload and file was successfully created
          _this4._emitSuccess();
          _this4._source.close();
          return;
        }

        if (_this4._hasStorage()) {
          _this4._storage.setItem(_this4._fingerprint, _this4.url, function (err) {
            if (err) {
              _this4._emitError(err);
            }
          });
        }

        _this4._offset = 0;
        _this4._startUpload();
      };

      xhr.onerror = function (err) {
        _this4._emitXhrError(xhr, new Error("tus: failed to create upload"), err);
      };

      this._setupXHR(xhr);
      if (this.options.uploadLengthDeferred) {
        xhr.setRequestHeader("Upload-Defer-Length", 1);
      } else {
        xhr.setRequestHeader("Upload-Length", this._size);
      }

      // Add metadata if values have been added
      var metadata = encodeMetadata(this.options.metadata);
      if (metadata !== "") {
        xhr.setRequestHeader("Upload-Metadata", metadata);
      }

      xhr.send(null);
    }

    /*
     * Try to resume an existing upload. First a HEAD request will be sent
     * to retrieve the offset. If the request fails a new upload will be
     * created. In the case of a successful response the file will be uploaded.
     *
     * @api private
     */

  }, {
    key: "_resumeUpload",
    value: function _resumeUpload() {
      var _this5 = this;

      var xhr = (0, _request.newRequest)();
      xhr.open("HEAD", this.url, true);

      xhr.onload = function () {
        if (!inStatusCategory(xhr.status, 200)) {
          if (_this5._hasStorage() && inStatusCategory(xhr.status, 400)) {
            // Remove stored fingerprint and corresponding endpoint,
            // on client errors since the file can not be found
            _this5._storage.removeItem(_this5._fingerprint, function (err) {
              if (err) {
                _this5._emitError(err);
              }
            });
          }

          // If the upload is locked (indicated by the 423 Locked status code), we
          // emit an error instead of directly starting a new upload. This way the
          // retry logic can catch the error and will retry the upload. An upload
          // is usually locked for a short period of time and will be available
          // afterwards.
          if (xhr.status === 423) {
            _this5._emitXhrError(xhr, new Error("tus: upload is currently locked; retry later"));
            return;
          }

          if (!_this5.options.endpoint) {
            // Don't attempt to create a new upload if no endpoint is provided.
            _this5._emitXhrError(xhr, new Error("tus: unable to resume upload (new upload cannot be created without an endpoint)"));
            return;
          }

          // Try to create a new upload
          _this5.url = null;
          _this5._createUpload();
          return;
        }

        var offset = parseInt(xhr.getResponseHeader("Upload-Offset"), 10);
        if (isNaN(offset)) {
          _this5._emitXhrError(xhr, new Error("tus: invalid or missing offset value"));
          return;
        }

        var length = parseInt(xhr.getResponseHeader("Upload-Length"), 10);
        if (isNaN(length) && !_this5.options.uploadLengthDeferred) {
          _this5._emitXhrError(xhr, new Error("tus: invalid or missing length value"));
          return;
        }

        // Upload has already been completed and we do not need to send additional
        // data to the server
        if (offset === length) {
          _this5._emitProgress(length, length);
          _this5._emitSuccess();
          return;
        }

        _this5._offset = offset;
        _this5._startUpload();
      };

      xhr.onerror = function (err) {
        _this5._emitXhrError(xhr, new Error("tus: failed to resume upload"), err);
      };

      this._setupXHR(xhr);
      xhr.send(null);
    }

    /**
     * Start uploading the file using PATCH requests. The file will be divided
     * into chunks as specified in the chunkSize option. During the upload
     * the onProgress event handler may be invoked multiple times.
     *
     * @api private
     */

  }, {
    key: "_startUpload",
    value: function _startUpload() {
      var _this6 = this;

      // If the upload has been aborted, we will not send the next PATCH request.
      // This is important if the abort method was called during a callback, such
      // as onChunkComplete or onProgress.
      if (this._aborted) {
        return;
      }

      var xhr = (0, _request.newRequest)();

      // Some browser and servers may not support the PATCH method. For those
      // cases, you can tell tus-js-client to use a POST request with the
      // X-HTTP-Method-Override header for simulating a PATCH request.
      if (this.options.overridePatchMethod) {
        xhr.open("POST", this.url, true);
        xhr.setRequestHeader("X-HTTP-Method-Override", "PATCH");
      } else {
        xhr.open("PATCH", this.url, true);
      }

      xhr.onload = function () {
        if (!inStatusCategory(xhr.status, 200)) {
          _this6._emitXhrError(xhr, new Error("tus: unexpected response while uploading chunk"));
          return;
        }

        var offset = parseInt(xhr.getResponseHeader("Upload-Offset"), 10);
        if (isNaN(offset)) {
          _this6._emitXhrError(xhr, new Error("tus: invalid or missing offset value"));
          return;
        }

        _this6._emitProgress(offset, _this6._size);
        _this6._emitChunkComplete(offset - _this6._offset, offset, _this6._size);

        _this6._offset = offset;

        if (offset == _this6._size) {
          if (_this6.options.removeFingerprintOnSuccess && _this6.options.resume) {
            // Remove stored fingerprint and corresponding endpoint. This causes
            // new upload of the same file must be treated as a different file.
            _this6._storage.removeItem(_this6._fingerprint, function (err) {
              if (err) {
                _this6._emitError(err);
              }
            });
          }

          // Yay, finally done :)
          _this6._emitSuccess();
          _this6._source.close();
          return;
        }

        _this6._startUpload();
      };

      xhr.onerror = function (err) {
        // Don't emit an error if the upload was aborted manually
        if (_this6._aborted) {
          return;
        }

        _this6._emitXhrError(xhr, new Error("tus: failed to upload chunk at offset " + _this6._offset), err);
      };

      // Test support for progress events before attaching an event listener
      if ("upload" in xhr) {
        xhr.upload.onprogress = function (e) {
          if (!e.lengthComputable) {
            return;
          }

          _this6._emitProgress(start + e.loaded, _this6._size);
        };
      }

      this._setupXHR(xhr);

      xhr.setRequestHeader("Upload-Offset", this._offset);
      xhr.setRequestHeader("Content-Type", "application/offset+octet-stream");

      var start = this._offset;
      var end = this._offset + this.options.chunkSize;

      // The specified chunkSize may be Infinity or the calcluated end position
      // may exceed the file's size. In both cases, we limit the end position to
      // the input's total size for simpler calculations and correctness.
      if ((end === Infinity || end > this._size) && !this.options.uploadLengthDeferred) {
        end = this._size;
      }

      this._source.slice(start, end, function (err, value, complete) {
        if (err) {
          _this6._emitError(err);
          return;
        }

        if (_this6.options.uploadLengthDeferred) {
          if (complete) {
            _this6._size = _this6._offset + (value && value.size ? value.size : 0);
            xhr.setRequestHeader("Upload-Length", _this6._size);
          }
        }

        if (value === null) {
          xhr.send();
        } else {
          xhr.send(value);
          _this6._emitProgress(_this6._offset, _this6._size);
        }
      });
    }
  }], [{
    key: "terminate",
    value: function terminate(url, options, cb) {
      if (typeof options !== "function" && typeof cb !== "function") {
        throw new Error("tus: a callback function must be specified");
      }

      if (typeof options === "function") {
        cb = options;
        options = {};
      }

      var xhr = (0, _request.newRequest)();
      xhr.open("DELETE", url, true);

      xhr.onload = function () {
        if (xhr.status !== 204) {
          cb(new _error2.default(new Error("tus: unexpected response while terminating upload"), null, xhr));
          return;
        }

        cb();
      };

      xhr.onerror = function (err) {
        cb(new _error2.default(err, new Error("tus: failed to terminate upload"), xhr));
      };

      setupXHR(xhr, options);
      xhr.send(null);
    }
  }]);

  return Upload;
}();

function encodeMetadata(metadata) {
  var encoded = [];

  for (var key in metadata) {
    encoded.push(key + " " + _jsBase.Base64.encode(metadata[key]));
  }

  return encoded.join(",");
}

/**
 * Checks whether a given status is in the range of the expected category.
 * For example, only a status between 200 and 299 will satisfy the category 200.
 *
 * @api private
 */
function inStatusCategory(status, category) {
  return status >= category && status < category + 100;
}

function setupXHR(xhr, options) {
  xhr.setRequestHeader("Tus-Resumable", "1.0.0");
  var headers = options.headers || {};

  for (var name in headers) {
    xhr.setRequestHeader(name, headers[name]);
  }

  xhr.withCredentials = options.withCredentials;
}

Upload.defaultOptions = defaultOptions;

exports.default = Upload;
},{"./error":45,"./node/fingerprint":37,"./node/request":41,"./node/source":42,"./node/storage":43,"extend":7,"js-base64":8}],48:[function(require,module,exports){
module.exports={
  "name": "@uppy/tus",
  "description": "Resumable uploads for Uppy using Tus.io",
  "version": "1.4.2",
  "license": "MIT",
  "main": "lib/index.js",
  "types": "types/index.d.ts",
  "keywords": [
    "file uploader",
    "uppy",
    "uppy-plugin",
    "upload",
    "resumable",
    "tus"
  ],
  "homepage": "https://uppy.io",
  "bugs": {
    "url": "https://github.com/transloadit/uppy/issues"
  },
  "repository": {
    "type": "git",
    "url": "git+https://github.com/transloadit/uppy.git"
  },
  "dependencies": {
    "@uppy/companion-client": "file:../companion-client",
    "@uppy/utils": "file:../utils",
    "tus-js-client": "^1.8.0-2"
  },
  "peerDependencies": {
    "@uppy/core": "^1.0.0"
  }
}

},{}],49:[function(require,module,exports){
var tus = require('tus-js-client');

function isCordova() {
  return typeof window !== 'undefined' && (typeof window.PhoneGap !== 'undefined' || typeof window.Cordova !== 'undefined' || typeof window.cordova !== 'undefined');
}

function isReactNative() {
  return typeof navigator !== 'undefined' && typeof navigator.product === 'string' && navigator.product.toLowerCase() === 'reactnative';
} // We override tus fingerprint to uppy’s `file.id`, since the `file.id`
// now also includes `relativePath` for files added from folders.
// This means you can add 2 identical files, if one is in folder a,
// the other in folder b — `a/file.jpg` and `b/file.jpg`, when added
// together with a folder, will be treated as 2 separate files.
//
// For React Native and Cordova, we let tus-js-client’s default
// fingerprint handling take charge.


module.exports = function getFingerprint(uppyFileObj) {
  return function (file, options, callback) {
    if (isCordova() || isReactNative()) {
      return tus.Upload.defaultOptions.fingerprint(file, options, callback);
    }

    var uppyFingerprint = ['tus', uppyFileObj.id, options.endpoint].join('-');
    return callback(null, uppyFingerprint);
  };
};

},{"tus-js-client":46}],50:[function(require,module,exports){
var _class, _temp;

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

function _assertThisInitialized(self) { if (self === void 0) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return self; }

function _inheritsLoose(subClass, superClass) { subClass.prototype = Object.create(superClass.prototype); subClass.prototype.constructor = subClass; subClass.__proto__ = superClass; }

var _require = require('./../../core'),
    Plugin = _require.Plugin;

var tus = require('tus-js-client');

var _require2 = require('./../../companion-client'),
    Provider = _require2.Provider,
    RequestClient = _require2.RequestClient,
    Socket = _require2.Socket;

var emitSocketProgress = require('./../../utils/lib/emitSocketProgress');

var getSocketHost = require('./../../utils/lib/getSocketHost');

var settle = require('./../../utils/lib/settle');

var EventTracker = require('./../../utils/lib/EventTracker');

var RateLimitedQueue = require('./../../utils/lib/RateLimitedQueue');

var getFingerprint = require('./getFingerprint');
/** @typedef {import('..').TusOptions} TusOptions */

/** @typedef {import('@uppy/core').Uppy} Uppy */

/** @typedef {import('@uppy/core').UppyFile} UppyFile */

/** @typedef {import('@uppy/core').FailedUppyFile<{}>} FailedUppyFile */

/**
 * Extracted from https://github.com/tus/tus-js-client/blob/master/lib/upload.js#L13
 * excepted we removed 'fingerprint' key to avoid adding more dependencies
 *
 * @type {TusOptions}
 */


var tusDefaultOptions = {
  endpoint: '',
  resume: true,
  onProgress: null,
  onChunkComplete: null,
  onSuccess: null,
  onError: null,
  headers: {},
  chunkSize: Infinity,
  withCredentials: false,
  uploadUrl: null,
  uploadSize: null,
  overridePatchMethod: false,
  retryDelays: null
  /**
   * Tus resumable file uploader
   */

};
module.exports = (_temp = _class =
/*#__PURE__*/
function (_Plugin) {
  _inheritsLoose(Tus, _Plugin);

  /**
   * @param {Uppy} uppy
   * @param {TusOptions} opts
   */
  function Tus(uppy, opts) {
    var _this;

    _this = _Plugin.call(this, uppy, opts) || this;
    _this.type = 'uploader';
    _this.id = _this.opts.id || 'Tus';
    _this.title = 'Tus'; // set default options

    var defaultOptions = {
      resume: true,
      autoRetry: true,
      useFastRemoteRetry: true,
      limit: 0,
      retryDelays: [0, 1000, 3000, 5000] // merge default options with the ones set by user

      /** @type {import("..").TusOptions} */

    };
    _this.opts = _extends({}, defaultOptions, opts);
    /**
     * Simultaneous upload limiting is shared across all uploads with this plugin.
     *
     * @type {RateLimitedQueue}
     */

    _this.requests = new RateLimitedQueue(_this.opts.limit);
    _this.uploaders = Object.create(null);
    _this.uploaderEvents = Object.create(null);
    _this.uploaderSockets = Object.create(null);
    _this.handleResetProgress = _this.handleResetProgress.bind(_assertThisInitialized(_this));
    _this.handleUpload = _this.handleUpload.bind(_assertThisInitialized(_this));
    return _this;
  }

  var _proto = Tus.prototype;

  _proto.handleResetProgress = function handleResetProgress() {
    var files = _extends({}, this.uppy.getState().files);

    Object.keys(files).forEach(function (fileID) {
      // Only clone the file object if it has a Tus `uploadUrl` attached.
      if (files[fileID].tus && files[fileID].tus.uploadUrl) {
        var tusState = _extends({}, files[fileID].tus);

        delete tusState.uploadUrl;
        files[fileID] = _extends({}, files[fileID], {
          tus: tusState
        });
      }
    });
    this.uppy.setState({
      files: files
    });
  }
  /**
   * Clean up all references for a file's upload: the tus.Upload instance,
   * any events related to the file, and the Companion WebSocket connection.
   *
   * @param {string} fileID
   */
  ;

  _proto.resetUploaderReferences = function resetUploaderReferences(fileID, opts) {
    if (opts === void 0) {
      opts = {};
    }

    if (this.uploaders[fileID]) {
      var uploader = this.uploaders[fileID];
      uploader.abort();

      if (opts.abort) {
        // to avoid 423 error from tus server, we wait
        // to be sure the previous request has been aborted before terminating the upload
        // @todo remove the timeout when this "wait" is handled in tus-js-client internally
        setTimeout(function () {
          return uploader.abort(true);
        }, 1000);
      }

      this.uploaders[fileID] = null;
    }

    if (this.uploaderEvents[fileID]) {
      this.uploaderEvents[fileID].remove();
      this.uploaderEvents[fileID] = null;
    }

    if (this.uploaderSockets[fileID]) {
      this.uploaderSockets[fileID].close();
      this.uploaderSockets[fileID] = null;
    }
  }
  /**
   * Create a new Tus upload.
   *
   * A lot can happen during an upload, so this is quite hard to follow!
   * - First, the upload is started. If the file was already paused by the time the upload starts, nothing should happen.
   *   If the `limit` option is used, the upload must be queued onto the `this.requests` queue.
   *   When an upload starts, we store the tus.Upload instance, and an EventTracker instance that manages the event listeners
   *   for pausing, cancellation, removal, etc.
   * - While the upload is in progress, it may be paused or cancelled.
   *   Pausing aborts the underlying tus.Upload, and removes the upload from the `this.requests` queue. All other state is
   *   maintained.
   *   Cancelling removes the upload from the `this.requests` queue, and completely aborts the upload--the tus.Upload instance
   *   is aborted and discarded, the EventTracker instance is destroyed (removing all listeners).
   *   Resuming the upload uses the `this.requests` queue as well, to prevent selectively pausing and resuming uploads from
   *   bypassing the limit.
   * - After completing an upload, the tus.Upload and EventTracker instances are cleaned up, and the upload is marked as done
   *   in the `this.requests` queue.
   * - When an upload completed with an error, the same happens as on successful completion, but the `upload()` promise is rejected.
   *
   * When working on this function, keep in mind:
   *  - When an upload is completed or cancelled for any reason, the tus.Upload and EventTracker instances need to be cleaned up using this.resetUploaderReferences().
   *  - When an upload is cancelled or paused, for any reason, it needs to be removed from the `this.requests` queue using `queuedRequest.abort()`.
   *  - When an upload is completed for any reason, including errors, it needs to be marked as such using `queuedRequest.done()`.
   *  - When an upload is started or resumed, it needs to go through the `this.requests` queue. The `queuedRequest` variable must be updated so the other uses of it are valid.
   *  - Before replacing the `queuedRequest` variable, the previous `queuedRequest` must be aborted, else it will keep taking up a spot in the queue.
   *
   * @param {UppyFile} file for use with upload
   * @param {number} current file in a queue
   * @param {number} total number of files in a queue
   * @returns {Promise<void>}
   */
  ;

  _proto.upload = function upload(file, current, total) {
    var _this2 = this;

    this.resetUploaderReferences(file.id); // Create a new tus upload

    return new Promise(function (resolve, reject) {
      _this2.uppy.emit('upload-started', file);

      var optsTus = _extends({}, tusDefaultOptions, _this2.opts, // Install file-specific upload overrides.
      file.tus || {}); // We override tus fingerprint to uppy’s `file.id`, since the `file.id`
      // now also includes `relativePath` for files added from folders.
      // This means you can add 2 identical files, if one is in folder a,
      // the other in folder b.


      optsTus.fingerprint = getFingerprint(file);

      optsTus.onError = function (err) {
        _this2.uppy.log(err);

        _this2.uppy.emit('upload-error', file, err);

        err.message = "Failed because: " + err.message;

        _this2.resetUploaderReferences(file.id);

        queuedRequest.done();
        reject(err);
      };

      optsTus.onProgress = function (bytesUploaded, bytesTotal) {
        _this2.onReceiveUploadUrl(file, upload.url);

        _this2.uppy.emit('upload-progress', file, {
          uploader: _this2,
          bytesUploaded: bytesUploaded,
          bytesTotal: bytesTotal
        });
      };

      optsTus.onSuccess = function () {
        var uploadResp = {
          uploadURL: upload.url
        };

        _this2.uppy.emit('upload-success', file, uploadResp);

        if (upload.url) {
          _this2.uppy.log('Download ' + upload.file.name + ' from ' + upload.url);
        }

        _this2.resetUploaderReferences(file.id);

        queuedRequest.done();
        resolve(upload);
      };

      var copyProp = function copyProp(obj, srcProp, destProp) {
        if (Object.prototype.hasOwnProperty.call(obj, srcProp) && !Object.prototype.hasOwnProperty.call(obj, destProp)) {
          obj[destProp] = obj[srcProp];
        }
      };

      var meta = {};
      var metaFields = Array.isArray(optsTus.metaFields) ? optsTus.metaFields // Send along all fields by default.
      : Object.keys(file.meta);
      metaFields.forEach(function (item) {
        meta[item] = file.meta[item];
      }); // tusd uses metadata fields 'filetype' and 'filename'

      copyProp(meta, 'type', 'filetype');
      copyProp(meta, 'name', 'filename');
      optsTus.metadata = meta;
      var upload = new tus.Upload(file.data, optsTus);
      _this2.uploaders[file.id] = upload;
      _this2.uploaderEvents[file.id] = new EventTracker(_this2.uppy);

      var queuedRequest = _this2.requests.run(function () {
        if (!file.isPaused) {
          upload.start();
        } // Don't do anything here, the caller will take care of cancelling the upload itself
        // using resetUploaderReferences(). This is because resetUploaderReferences() has to be
        // called when this request is still in the queue, and has not been started yet, too. At
        // that point this cancellation function is not going to be called.
        // Also, we need to remove the request from the queue _without_ destroying everything
        // related to this upload to handle pauses.


        return function () {};
      });

      _this2.onFileRemove(file.id, function (targetFileID) {
        queuedRequest.abort();

        _this2.resetUploaderReferences(file.id, {
          abort: !!upload.url
        });

        resolve("upload " + targetFileID + " was removed");
      });

      _this2.onPause(file.id, function (isPaused) {
        if (isPaused) {
          // Remove this file from the queue so another file can start in its place.
          queuedRequest.abort();
          upload.abort();
        } else {
          // Resuming an upload should be queued, else you could pause and then resume a queued upload to make it skip the queue.
          queuedRequest.abort();
          queuedRequest = _this2.requests.run(function () {
            upload.start();
            return function () {};
          });
        }
      });

      _this2.onPauseAll(file.id, function () {
        queuedRequest.abort();
        upload.abort();
      });

      _this2.onCancelAll(file.id, function () {
        queuedRequest.abort();

        _this2.resetUploaderReferences(file.id, {
          abort: !!upload.url
        });

        resolve("upload " + file.id + " was canceled");
      });

      _this2.onResumeAll(file.id, function () {
        queuedRequest.abort();

        if (file.error) {
          upload.abort();
        }

        queuedRequest = _this2.requests.run(function () {
          upload.start();
          return function () {};
        });
      });
    }).catch(function (err) {
      _this2.uppy.emit('upload-error', file, err);

      throw err;
    });
  }
  /**
   * @param {UppyFile} file for use with upload
   * @param {number} current file in a queue
   * @param {number} total number of files in a queue
   * @returns {Promise<void>}
   */
  ;

  _proto.uploadRemote = function uploadRemote(file, current, total) {
    var _this3 = this;

    this.resetUploaderReferences(file.id);

    var opts = _extends({}, this.opts);

    if (file.tus) {
      // Install file-specific upload overrides.
      _extends(opts, file.tus);
    }

    this.uppy.emit('upload-started', file);
    this.uppy.log(file.remote.url);

    if (file.serverToken) {
      return this.connectToServerSocket(file);
    }

    return new Promise(function (resolve, reject) {
      var Client = file.remote.providerOptions.provider ? Provider : RequestClient;
      var client = new Client(_this3.uppy, file.remote.providerOptions); // !! cancellation is NOT supported at this stage yet

      client.post(file.remote.url, _extends({}, file.remote.body, {
        endpoint: opts.endpoint,
        uploadUrl: opts.uploadUrl,
        protocol: 'tus',
        size: file.data.size,
        metadata: file.meta
      })).then(function (res) {
        _this3.uppy.setFileState(file.id, {
          serverToken: res.token
        });

        file = _this3.uppy.getFile(file.id);
        return _this3.connectToServerSocket(file);
      }).then(function () {
        resolve();
      }).catch(function (err) {
        reject(new Error(err));
      });
    });
  }
  /**
   * See the comment on the upload() method.
   *
   * Additionally, when an upload is removed, completed, or cancelled, we need to close the WebSocket connection. This is handled by the resetUploaderReferences() function, so the same guidelines apply as in upload().
   *
   * @param {UppyFile} file
   */
  ;

  _proto.connectToServerSocket = function connectToServerSocket(file) {
    var _this4 = this;

    return new Promise(function (resolve, reject) {
      var token = file.serverToken;
      var host = getSocketHost(file.remote.companionUrl);
      var socket = new Socket({
        target: host + "/api/" + token,
        autoOpen: false
      });
      _this4.uploaderSockets[file.id] = socket;
      _this4.uploaderEvents[file.id] = new EventTracker(_this4.uppy);

      _this4.onFileRemove(file.id, function () {
        queuedRequest.abort(); // still send pause event in case we are dealing with older version of companion
        // @todo don't send pause event in the next major release.

        socket.send('pause', {});
        socket.send('cancel', {});

        _this4.resetUploaderReferences(file.id);

        resolve("upload " + file.id + " was removed");
      });

      _this4.onPause(file.id, function (isPaused) {
        if (isPaused) {
          // Remove this file from the queue so another file can start in its place.
          queuedRequest.abort();
          socket.send('pause', {});
        } else {
          // Resuming an upload should be queued, else you could pause and then resume a queued upload to make it skip the queue.
          queuedRequest.abort();
          queuedRequest = _this4.requests.run(function () {
            socket.send('resume', {});
            return function () {};
          });
        }
      });

      _this4.onPauseAll(file.id, function () {
        queuedRequest.abort();
        socket.send('pause', {});
      });

      _this4.onCancelAll(file.id, function () {
        queuedRequest.abort(); // still send pause event in case we are dealing with older version of companion
        // @todo don't send pause event in the next major release.

        socket.send('pause', {});
        socket.send('cancel', {});

        _this4.resetUploaderReferences(file.id);

        resolve("upload " + file.id + " was canceled");
      });

      _this4.onResumeAll(file.id, function () {
        queuedRequest.abort();

        if (file.error) {
          socket.send('pause', {});
        }

        queuedRequest = _this4.requests.run(function () {
          socket.send('resume', {});
          return function () {};
        });
      });

      _this4.onRetry(file.id, function () {
        // Only do the retry if the upload is actually in progress;
        // else we could try to send these messages when the upload is still queued.
        // We may need a better check for this since the socket may also be closed
        // for other reasons, like network failures.
        if (socket.isOpen) {
          socket.send('pause', {});
          socket.send('resume', {});
        }
      });

      _this4.onRetryAll(file.id, function () {
        // See the comment in the onRetry() call
        if (socket.isOpen) {
          socket.send('pause', {});
          socket.send('resume', {});
        }
      });

      socket.on('progress', function (progressData) {
        return emitSocketProgress(_this4, progressData, file);
      });
      socket.on('error', function (errData) {
        var message = errData.error.message;

        var error = _extends(new Error(message), {
          cause: errData.error
        }); // If the remote retry optimisation should not be used,
        // close the socket—this will tell companion to clear state and delete the file.


        if (!_this4.opts.useFastRemoteRetry) {
          _this4.resetUploaderReferences(file.id); // Remove the serverToken so that a new one will be created for the retry.


          _this4.uppy.setFileState(file.id, {
            serverToken: null
          });
        } else {
          socket.close();
        }

        _this4.uppy.emit('upload-error', file, error);

        queuedRequest.done();
        reject(error);
      });
      socket.on('success', function (data) {
        var uploadResp = {
          uploadURL: data.url
        };

        _this4.uppy.emit('upload-success', file, uploadResp);

        _this4.resetUploaderReferences(file.id);

        queuedRequest.done();
        resolve();
      });

      var queuedRequest = _this4.requests.run(function () {
        socket.open();

        if (file.isPaused) {
          socket.send('pause', {});
        } // Don't do anything here, the caller will take care of cancelling the upload itself
        // using resetUploaderReferences(). This is because resetUploaderReferences() has to be
        // called when this request is still in the queue, and has not been started yet, too. At
        // that point this cancellation function is not going to be called.
        // Also, we need to remove the request from the queue _without_ destroying everything
        // related to this upload to handle pauses.


        return function () {};
      });
    });
  }
  /**
   * Store the uploadUrl on the file options, so that when Golden Retriever
   * restores state, we will continue uploading to the correct URL.
   *
   * @param {UppyFile} file
   * @param {string} uploadURL
   */
  ;

  _proto.onReceiveUploadUrl = function onReceiveUploadUrl(file, uploadURL) {
    var currentFile = this.uppy.getFile(file.id);
    if (!currentFile) return; // Only do the update if we didn't have an upload URL yet.

    if (!currentFile.tus || currentFile.tus.uploadUrl !== uploadURL) {
      this.uppy.log('[Tus] Storing upload url');
      this.uppy.setFileState(currentFile.id, {
        tus: _extends({}, currentFile.tus, {
          uploadUrl: uploadURL
        })
      });
    }
  }
  /**
   * @param {string} fileID
   * @param {function(string): void} cb
   */
  ;

  _proto.onFileRemove = function onFileRemove(fileID, cb) {
    this.uploaderEvents[fileID].on('file-removed', function (file) {
      if (fileID === file.id) cb(file.id);
    });
  }
  /**
   * @param {string} fileID
   * @param {function(boolean): void} cb
   */
  ;

  _proto.onPause = function onPause(fileID, cb) {
    this.uploaderEvents[fileID].on('upload-pause', function (targetFileID, isPaused) {
      if (fileID === targetFileID) {
        // const isPaused = this.uppy.pauseResume(fileID)
        cb(isPaused);
      }
    });
  }
  /**
   * @param {string} fileID
   * @param {function(): void} cb
   */
  ;

  _proto.onRetry = function onRetry(fileID, cb) {
    this.uploaderEvents[fileID].on('upload-retry', function (targetFileID) {
      if (fileID === targetFileID) {
        cb();
      }
    });
  }
  /**
   * @param {string} fileID
   * @param {function(): void} cb
   */
  ;

  _proto.onRetryAll = function onRetryAll(fileID, cb) {
    var _this5 = this;

    this.uploaderEvents[fileID].on('retry-all', function (filesToRetry) {
      if (!_this5.uppy.getFile(fileID)) return;
      cb();
    });
  }
  /**
   * @param {string} fileID
   * @param {function(): void} cb
   */
  ;

  _proto.onPauseAll = function onPauseAll(fileID, cb) {
    var _this6 = this;

    this.uploaderEvents[fileID].on('pause-all', function () {
      if (!_this6.uppy.getFile(fileID)) return;
      cb();
    });
  }
  /**
   * @param {string} fileID
   * @param {function(): void} cb
   */
  ;

  _proto.onCancelAll = function onCancelAll(fileID, cb) {
    var _this7 = this;

    this.uploaderEvents[fileID].on('cancel-all', function () {
      if (!_this7.uppy.getFile(fileID)) return;
      cb();
    });
  }
  /**
   * @param {string} fileID
   * @param {function(): void} cb
   */
  ;

  _proto.onResumeAll = function onResumeAll(fileID, cb) {
    var _this8 = this;

    this.uploaderEvents[fileID].on('resume-all', function () {
      if (!_this8.uppy.getFile(fileID)) return;
      cb();
    });
  }
  /**
   * @param {(UppyFile | FailedUppyFile)[]} files
   */
  ;

  _proto.uploadFiles = function uploadFiles(files) {
    var _this9 = this;

    var promises = files.map(function (file, i) {
      var current = i + 1;
      var total = files.length;

      if ('error' in file && file.error) {
        return Promise.reject(new Error(file.error));
      } else if (file.isRemote) {
        return _this9.uploadRemote(file, current, total);
      } else {
        return _this9.upload(file, current, total);
      }
    });
    return settle(promises);
  }
  /**
   * @param {string[]} fileIDs
   */
  ;

  _proto.handleUpload = function handleUpload(fileIDs) {
    var _this10 = this;

    if (fileIDs.length === 0) {
      this.uppy.log('[Tus] No files to upload');
      return Promise.resolve();
    }

    if (this.opts.limit === 0) {
      this.uppy.log('[Tus] When uploading multiple files at once, consider setting the `limit` option (to `10` for example), to limit the number of concurrent uploads, which helps prevent memory and network issues: https://uppy.io/docs/tus/#limit-0', 'warning');
    }

    this.uppy.log('[Tus] Uploading...');
    var filesToUpload = fileIDs.map(function (fileID) {
      return _this10.uppy.getFile(fileID);
    });
    return this.uploadFiles(filesToUpload).then(function () {
      return null;
    });
  };

  _proto.install = function install() {
    this.uppy.setState({
      capabilities: _extends({}, this.uppy.getState().capabilities, {
        resumableUploads: true
      })
    });
    this.uppy.addUploader(this.handleUpload);
    this.uppy.on('reset-progress', this.handleResetProgress);

    if (this.opts.autoRetry) {
      this.uppy.on('back-online', this.uppy.retryAll);
    }
  };

  _proto.uninstall = function uninstall() {
    this.uppy.setState({
      capabilities: _extends({}, this.uppy.getState().capabilities, {
        resumableUploads: false
      })
    });
    this.uppy.removeUploader(this.handleUpload);

    if (this.opts.autoRetry) {
      this.uppy.off('back-online', this.uppy.retryAll);
    }
  };

  return Tus;
}(Plugin), _class.VERSION = require('../package.json').version, _temp);

},{"../package.json":48,"./../../companion-client":24,"./../../core":28,"./../../utils/lib/EventTracker":51,"./../../utils/lib/RateLimitedQueue":52,"./../../utils/lib/emitSocketProgress":54,"./../../utils/lib/getSocketHost":64,"./../../utils/lib/settle":71,"./getFingerprint":49,"tus-js-client":46}],51:[function(require,module,exports){
/**
 * Create a wrapper around an event emitter with a `remove` method to remove
 * all events that were added using the wrapped emitter.
 */
module.exports =
/*#__PURE__*/
function () {
  function EventTracker(emitter) {
    this._events = [];
    this._emitter = emitter;
  }

  var _proto = EventTracker.prototype;

  _proto.on = function on(event, fn) {
    this._events.push([event, fn]);

    return this._emitter.on(event, fn);
  };

  _proto.remove = function remove() {
    var _this = this;

    this._events.forEach(function (_ref) {
      var event = _ref[0],
          fn = _ref[1];

      _this._emitter.off(event, fn);
    });
  };

  return EventTracker;
}();

},{}],52:[function(require,module,exports){
module.exports =
/*#__PURE__*/
function () {
  function RateLimitedQueue(limit) {
    if (typeof limit !== 'number' || limit === 0) {
      this.limit = Infinity;
    } else {
      this.limit = limit;
    }

    this.activeRequests = 0;
    this.queuedHandlers = [];
  }

  var _proto = RateLimitedQueue.prototype;

  _proto._call = function _call(fn) {
    var _this = this;

    this.activeRequests += 1;
    var _done = false;
    var cancelActive;

    try {
      cancelActive = fn();
    } catch (err) {
      this.activeRequests -= 1;
      throw err;
    }

    return {
      abort: function abort() {
        if (_done) return;
        _done = true;
        _this.activeRequests -= 1;
        cancelActive();

        _this._queueNext();
      },
      done: function done() {
        if (_done) return;
        _done = true;
        _this.activeRequests -= 1;

        _this._queueNext();
      }
    };
  };

  _proto._queueNext = function _queueNext() {
    var _this2 = this;

    // Do it soon but not immediately, this allows clearing out the entire queue synchronously
    // one by one without continuously _advancing_ it (and starting new tasks before immediately
    // aborting them)
    Promise.resolve().then(function () {
      _this2._next();
    });
  };

  _proto._next = function _next() {
    if (this.activeRequests >= this.limit) {
      return;
    }

    if (this.queuedHandlers.length === 0) {
      return;
    } // Dispatch the next request, and update the abort/done handlers
    // so that cancelling it does the Right Thing (and doesn't just try
    // to dequeue an already-running request).


    var next = this.queuedHandlers.shift();

    var handler = this._call(next.fn);

    next.abort = handler.abort;
    next.done = handler.done;
  };

  _proto._queue = function _queue(fn) {
    var _this3 = this;

    var handler = {
      fn: fn,
      abort: function abort() {
        _this3._dequeue(handler);
      },
      done: function done() {
        throw new Error('Cannot mark a queued request as done: this indicates a bug');
      }
    };
    this.queuedHandlers.push(handler);
    return handler;
  };

  _proto._dequeue = function _dequeue(handler) {
    var index = this.queuedHandlers.indexOf(handler);

    if (index !== -1) {
      this.queuedHandlers.splice(index, 1);
    }
  };

  _proto.run = function run(fn) {
    if (this.activeRequests < this.limit) {
      return this._call(fn);
    }

    return this._queue(fn);
  };

  _proto.wrapPromiseFunction = function wrapPromiseFunction(fn) {
    var _this4 = this;

    return function () {
      for (var _len = arguments.length, args = new Array(_len), _key = 0; _key < _len; _key++) {
        args[_key] = arguments[_key];
      }

      return new Promise(function (resolve, reject) {
        var queuedRequest = _this4.run(function () {
          var cancelError;
          var promise;

          try {
            promise = Promise.resolve(fn.apply(void 0, args));
          } catch (err) {
            promise = Promise.reject(err);
          }

          promise.then(function (result) {
            if (cancelError) {
              reject(cancelError);
            } else {
              queuedRequest.done();
              resolve(result);
            }
          }, function (err) {
            if (cancelError) {
              reject(cancelError);
            } else {
              queuedRequest.done();
              reject(err);
            }
          });
          return function () {
            cancelError = new Error('Cancelled');
          };
        });
      });
    };
  };

  return RateLimitedQueue;
}();

},{}],53:[function(require,module,exports){
function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

var has = require('./hasProperty');
/**
 * Translates strings with interpolation & pluralization support.
 * Extensible with custom dictionaries and pluralization functions.
 *
 * Borrows heavily from and inspired by Polyglot https://github.com/airbnb/polyglot.js,
 * basically a stripped-down version of it. Differences: pluralization functions are not hardcoded
 * and can be easily added among with dictionaries, nested objects are used for pluralization
 * as opposed to `||||` delimeter
 *
 * Usage example: `translator.translate('files_chosen', {smart_count: 3})`
 */


module.exports =
/*#__PURE__*/
function () {
  /**
   * @param {object|Array<object>} locales - locale or list of locales.
   */
  function Translator(locales) {
    var _this = this;

    this.locale = {
      strings: {},
      pluralize: function pluralize(n) {
        if (n === 1) {
          return 0;
        }

        return 1;
      }
    };

    if (Array.isArray(locales)) {
      locales.forEach(function (locale) {
        return _this._apply(locale);
      });
    } else {
      this._apply(locales);
    }
  }

  var _proto = Translator.prototype;

  _proto._apply = function _apply(locale) {
    if (!locale || !locale.strings) {
      return;
    }

    var prevLocale = this.locale;
    this.locale = _extends({}, prevLocale, {
      strings: _extends({}, prevLocale.strings, locale.strings)
    });
    this.locale.pluralize = locale.pluralize || prevLocale.pluralize;
  }
  /**
   * Takes a string with placeholder variables like `%{smart_count} file selected`
   * and replaces it with values from options `{smart_count: 5}`
   *
   * @license https://github.com/airbnb/polyglot.js/blob/master/LICENSE
   * taken from https://github.com/airbnb/polyglot.js/blob/master/lib/polyglot.js#L299
   *
   * @param {string} phrase that needs interpolation, with placeholders
   * @param {object} options with values that will be used to replace placeholders
   * @returns {string} interpolated
   */
  ;

  _proto.interpolate = function interpolate(phrase, options) {
    var _String$prototype = String.prototype,
        split = _String$prototype.split,
        replace = _String$prototype.replace;
    var dollarRegex = /\$/g;
    var dollarBillsYall = '$$$$';
    var interpolated = [phrase];

    for (var arg in options) {
      if (arg !== '_' && has(options, arg)) {
        // Ensure replacement value is escaped to prevent special $-prefixed
        // regex replace tokens. the "$$$$" is needed because each "$" needs to
        // be escaped with "$" itself, and we need two in the resulting output.
        var replacement = options[arg];

        if (typeof replacement === 'string') {
          replacement = replace.call(options[arg], dollarRegex, dollarBillsYall);
        } // We create a new `RegExp` each time instead of using a more-efficient
        // string replace so that the same argument can be replaced multiple times
        // in the same phrase.


        interpolated = insertReplacement(interpolated, new RegExp('%\\{' + arg + '\\}', 'g'), replacement);
      }
    }

    return interpolated;

    function insertReplacement(source, rx, replacement) {
      var newParts = [];
      source.forEach(function (chunk) {
        split.call(chunk, rx).forEach(function (raw, i, list) {
          if (raw !== '') {
            newParts.push(raw);
          } // Interlace with the `replacement` value


          if (i < list.length - 1) {
            newParts.push(replacement);
          }
        });
      });
      return newParts;
    }
  }
  /**
   * Public translate method
   *
   * @param {string} key
   * @param {object} options with values that will be used later to replace placeholders in string
   * @returns {string} translated (and interpolated)
   */
  ;

  _proto.translate = function translate(key, options) {
    return this.translateArray(key, options).join('');
  }
  /**
   * Get a translation and return the translated and interpolated parts as an array.
   *
   * @param {string} key
   * @param {object} options with values that will be used to replace placeholders
   * @returns {Array} The translated and interpolated parts, in order.
   */
  ;

  _proto.translateArray = function translateArray(key, options) {
    if (options && typeof options.smart_count !== 'undefined') {
      var plural = this.locale.pluralize(options.smart_count);
      return this.interpolate(this.locale.strings[key][plural], options);
    }

    return this.interpolate(this.locale.strings[key], options);
  };

  return Translator;
}();

},{"./hasProperty":66}],54:[function(require,module,exports){
var throttle = require('lodash.throttle');

function _emitSocketProgress(uploader, progressData, file) {
  var progress = progressData.progress,
      bytesUploaded = progressData.bytesUploaded,
      bytesTotal = progressData.bytesTotal;

  if (progress) {
    uploader.uppy.log("Upload progress: " + progress);
    uploader.uppy.emit('upload-progress', file, {
      uploader: uploader,
      bytesUploaded: bytesUploaded,
      bytesTotal: bytesTotal
    });
  }
}

module.exports = throttle(_emitSocketProgress, 300, {
  leading: true,
  trailing: true
});

},{"lodash.throttle":9}],55:[function(require,module,exports){
var isDOMElement = require('./isDOMElement');
/**
 * Find a DOM element.
 *
 * @param {Node|string} element
 * @returns {Node|null}
 */


module.exports = function findDOMElement(element, context) {
  if (context === void 0) {
    context = document;
  }

  if (typeof element === 'string') {
    return context.querySelector(element);
  }

  if (typeof element === 'object' && isDOMElement(element)) {
    return element;
  }
};

},{"./isDOMElement":67}],56:[function(require,module,exports){
/**
 * Takes a file object and turns it into fileID, by converting file.name to lowercase,
 * removing extra characters and adding type, size and lastModified
 *
 * @param {object} file
 * @returns {string} the fileID
 *
 */
module.exports = function generateFileID(file) {
  // filter is needed to not join empty values with `-`
  return ['uppy', file.name ? encodeFilename(file.name.toLowerCase()) : '', file.type, file.meta && file.meta.relativePath ? encodeFilename(file.meta.relativePath.toLowerCase()) : '', file.data.size, file.data.lastModified].filter(function (val) {
    return val;
  }).join('-');
};

function encodeFilename(name) {
  var suffix = '';
  return name.replace(/[^A-Z0-9]/ig, function (character) {
    suffix += '-' + encodeCharacter(character);
    return '/';
  }) + suffix;
}

function encodeCharacter(character) {
  return character.charCodeAt(0).toString(32);
}

},{}],57:[function(require,module,exports){
var webkitGetAsEntryApi = require('./utils/webkitGetAsEntryApi/index');

var fallbackApi = require('./utils/fallbackApi');
/**
 * Returns a promise that resolves to the array of dropped files (if a folder is dropped, and browser supports folder parsing - promise resolves to the flat array of all files in all directories).
 * Each file has .relativePath prop appended to it (e.g. "/docs/Prague/ticket_from_prague_to_ufa.pdf") if browser supports it. Otherwise it's undefined.
 *
 * @param {DataTransfer} dataTransfer
 * @param {Function} logDropError - a function that's called every time some folder or some file error out (e.g. because of the folder name being too long on Windows). Notice that resulting promise will always be resolved anyway.
 *
 * @returns {Promise} - Array<File>
 */


module.exports = function getDroppedFiles(dataTransfer, _temp) {
  var _ref = _temp === void 0 ? {} : _temp,
      _ref$logDropError = _ref.logDropError,
      logDropError = _ref$logDropError === void 0 ? function () {} : _ref$logDropError;

  // Get all files from all subdirs. Works (at least) in Chrome, Mozilla, and Safari
  if (dataTransfer.items && dataTransfer.items[0] && 'webkitGetAsEntry' in dataTransfer.items[0]) {
    return webkitGetAsEntryApi(dataTransfer, logDropError); // Otherwise just return all first-order files
  } else {
    return fallbackApi(dataTransfer);
  }
};

},{"./utils/fallbackApi":58,"./utils/webkitGetAsEntryApi/index":61}],58:[function(require,module,exports){
var toArray = require('../../toArray'); // .files fallback, should be implemented in any browser


module.exports = function fallbackApi(dataTransfer) {
  var files = toArray(dataTransfer.files);
  return Promise.resolve(files);
};

},{"../../toArray":72}],59:[function(require,module,exports){
/**
 * Recursive function, calls the original callback() when the directory is entirely parsed.
 *
 * @param {FileSystemDirectoryReader} directoryReader
 * @param {Array} oldEntries
 * @param {Function} logDropError
 * @param {Function} callback - called with ([ all files and directories in that directoryReader ])
 */
module.exports = function getFilesAndDirectoriesFromDirectory(directoryReader, oldEntries, logDropError, _ref) {
  var onSuccess = _ref.onSuccess;
  directoryReader.readEntries(function (entries) {
    var newEntries = [].concat(oldEntries, entries); // According to the FileSystem API spec, getFilesAndDirectoriesFromDirectory() must be called until it calls the onSuccess with an empty array.

    if (entries.length) {
      setTimeout(function () {
        getFilesAndDirectoriesFromDirectory(directoryReader, newEntries, logDropError, {
          onSuccess: onSuccess
        });
      }, 0); // Done iterating this particular directory
    } else {
      onSuccess(newEntries);
    }
  }, // Make sure we resolve on error anyway, it's fine if only one directory couldn't be parsed!
  function (error) {
    logDropError(error);
    onSuccess(oldEntries);
  });
};

},{}],60:[function(require,module,exports){
/**
 * Get the relative path from the FileEntry#fullPath, because File#webkitRelativePath is always '', at least onDrop.
 *
 * @param {FileEntry} fileEntry
 *
 * @returns {string|null} - if file is not in a folder - return null (this is to be consistent with .relativePath-s of files selected from My Device). If file is in a folder - return its fullPath, e.g. '/simpsons/hi.jpeg'.
 */
module.exports = function getRelativePath(fileEntry) {
  // fileEntry.fullPath - "/simpsons/hi.jpeg" or undefined (for browsers that don't support it)
  // fileEntry.name - "hi.jpeg"
  if (!fileEntry.fullPath || fileEntry.fullPath === '/' + fileEntry.name) {
    return null;
  } else {
    return fileEntry.fullPath;
  }
};

},{}],61:[function(require,module,exports){
var toArray = require('../../../toArray');

var getRelativePath = require('./getRelativePath');

var getFilesAndDirectoriesFromDirectory = require('./getFilesAndDirectoriesFromDirectory');

module.exports = function webkitGetAsEntryApi(dataTransfer, logDropError) {
  var files = [];
  var rootPromises = [];
  /**
   * Returns a resolved promise, when :files array is enhanced
   *
   * @param {(FileSystemFileEntry|FileSystemDirectoryEntry)} entry
   * @returns {Promise} - empty promise that resolves when :files is enhanced with a file
   */

  var createPromiseToAddFileOrParseDirectory = function createPromiseToAddFileOrParseDirectory(entry) {
    return new Promise(function (resolve) {
      // This is a base call
      if (entry.isFile) {
        // Creates a new File object which can be used to read the file.
        entry.file(function (file) {
          file.relativePath = getRelativePath(entry);
          files.push(file);
          resolve();
        }, // Make sure we resolve on error anyway, it's fine if only one file couldn't be read!
        function (error) {
          logDropError(error);
          resolve();
        }); // This is a recursive call
      } else if (entry.isDirectory) {
        var directoryReader = entry.createReader();
        getFilesAndDirectoriesFromDirectory(directoryReader, [], logDropError, {
          onSuccess: function onSuccess(entries) {
            var promises = entries.map(function (entry) {
              return createPromiseToAddFileOrParseDirectory(entry);
            });
            Promise.all(promises).then(function () {
              return resolve();
            });
          }
        });
      }
    });
  }; // For each dropped item, - make sure it's a file/directory, and start deepening in!


  toArray(dataTransfer.items).forEach(function (item) {
    var entry = item.webkitGetAsEntry(); // :entry can be null when we drop the url e.g.

    if (entry) {
      rootPromises.push(createPromiseToAddFileOrParseDirectory(entry));
    }
  });
  return Promise.all(rootPromises).then(function () {
    return files;
  });
};

},{"../../../toArray":72,"./getFilesAndDirectoriesFromDirectory":59,"./getRelativePath":60}],62:[function(require,module,exports){
/**
 * Takes a full filename string and returns an object {name, extension}
 *
 * @param {string} fullFileName
 * @returns {object} {name, extension}
 */
module.exports = function getFileNameAndExtension(fullFileName) {
  var re = /(?:\.([^.]+))?$/;
  var fileExt = re.exec(fullFileName)[1];
  var fileName = fullFileName.replace('.' + fileExt, '');
  return {
    name: fileName,
    extension: fileExt
  };
};

},{}],63:[function(require,module,exports){
var getFileNameAndExtension = require('./getFileNameAndExtension');

var mimeTypes = require('./mimeTypes');

module.exports = function getFileType(file) {
  var fileExtension = file.name ? getFileNameAndExtension(file.name).extension : null;
  fileExtension = fileExtension ? fileExtension.toLowerCase() : null;

  if (file.type) {
    // if mime type is set in the file object already, use that
    return file.type;
  } else if (fileExtension && mimeTypes[fileExtension]) {
    // else, see if we can map extension to a mime type
    return mimeTypes[fileExtension];
  } else {
    // if all fails, fall back to a generic byte stream type
    return 'application/octet-stream';
  }
};

},{"./getFileNameAndExtension":62,"./mimeTypes":69}],64:[function(require,module,exports){
module.exports = function getSocketHost(url) {
  // get the host domain
  var regex = /^(?:https?:\/\/|\/\/)?(?:[^@\n]+@)?(?:www\.)?([^\n]+)/i;
  var host = regex.exec(url)[1];
  var socketProtocol = /^http:\/\//i.test(url) ? 'ws' : 'wss';
  return socketProtocol + "://" + host;
};

},{}],65:[function(require,module,exports){
/**
 * Returns a timestamp in the format of `hours:minutes:seconds`
 */
module.exports = function getTimeStamp() {
  var date = new Date();
  var hours = pad(date.getHours().toString());
  var minutes = pad(date.getMinutes().toString());
  var seconds = pad(date.getSeconds().toString());
  return hours + ':' + minutes + ':' + seconds;
};
/**
 * Adds zero to strings shorter than two characters
 */


function pad(str) {
  return str.length !== 2 ? 0 + str : str;
}

},{}],66:[function(require,module,exports){
module.exports = function has(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key);
};

},{}],67:[function(require,module,exports){
/**
 * Check if an object is a DOM element. Duck-typing based on `nodeType`.
 *
 * @param {*} obj
 */
module.exports = function isDOMElement(obj) {
  return obj && typeof obj === 'object' && obj.nodeType === Node.ELEMENT_NODE;
};

},{}],68:[function(require,module,exports){
/**
 * Checks if the browser supports Drag & Drop (not supported on mobile devices, for example).
 *
 * @returns {boolean}
 */
module.exports = function isDragDropSupported() {
  var div = document.createElement('div');

  if (!('draggable' in div) || !('ondragstart' in div && 'ondrop' in div)) {
    return false;
  }

  if (!('FormData' in window)) {
    return false;
  }

  if (!('FileReader' in window)) {
    return false;
  }

  return true;
};

},{}],69:[function(require,module,exports){
// ___Why not add the mime-types package?
//    It's 19.7kB gzipped, and we only need mime types for well-known extensions (for file previews).
// ___Where to take new extensions from?
//    https://github.com/jshttp/mime-db/blob/master/db.json
module.exports = {
  md: 'text/markdown',
  markdown: 'text/markdown',
  mp4: 'video/mp4',
  mp3: 'audio/mp3',
  svg: 'image/svg+xml',
  jpg: 'image/jpeg',
  png: 'image/png',
  gif: 'image/gif',
  heic: 'image/heic',
  heif: 'image/heif',
  yaml: 'text/yaml',
  yml: 'text/yaml',
  csv: 'text/csv',
  avi: 'video/x-msvideo',
  mks: 'video/x-matroska',
  mkv: 'video/x-matroska',
  mov: 'video/quicktime',
  doc: 'application/msword',
  docm: 'application/vnd.ms-word.document.macroenabled.12',
  docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  dot: 'application/msword',
  dotm: 'application/vnd.ms-word.template.macroenabled.12',
  dotx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.template',
  xla: 'application/vnd.ms-excel',
  xlam: 'application/vnd.ms-excel.addin.macroenabled.12',
  xlc: 'application/vnd.ms-excel',
  xlf: 'application/x-xliff+xml',
  xlm: 'application/vnd.ms-excel',
  xls: 'application/vnd.ms-excel',
  xlsb: 'application/vnd.ms-excel.sheet.binary.macroenabled.12',
  xlsm: 'application/vnd.ms-excel.sheet.macroenabled.12',
  xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  xlt: 'application/vnd.ms-excel',
  xltm: 'application/vnd.ms-excel.template.macroenabled.12',
  xltx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.template',
  xlw: 'application/vnd.ms-excel',
  txt: 'text/plain',
  text: 'text/plain',
  conf: 'text/plain',
  log: 'text/plain',
  pdf: 'application/pdf'
};

},{}],70:[function(require,module,exports){
// Adapted from https://github.com/Flet/prettier-bytes/
// Changing 1000 bytes to 1024, so we can keep uppercase KB vs kB
// ISC License (c) Dan Flettre https://github.com/Flet/prettier-bytes/blob/master/LICENSE
module.exports = prettierBytes;

function prettierBytes(num) {
  if (typeof num !== 'number' || isNaN(num)) {
    throw new TypeError('Expected a number, got ' + typeof num);
  }

  var neg = num < 0;
  var units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  if (neg) {
    num = -num;
  }

  if (num < 1) {
    return (neg ? '-' : '') + num + ' B';
  }

  var exponent = Math.min(Math.floor(Math.log(num) / Math.log(1024)), units.length - 1);
  num = Number(num / Math.pow(1024, exponent));
  var unit = units[exponent];

  if (num >= 10 || num % 1 === 0) {
    // Do not show decimals when the number is two-digit, or if the number has no
    // decimal component.
    return (neg ? '-' : '') + num.toFixed(0) + ' ' + unit;
  } else {
    return (neg ? '-' : '') + num.toFixed(1) + ' ' + unit;
  }
}

},{}],71:[function(require,module,exports){
module.exports = function settle(promises) {
  var resolutions = [];
  var rejections = [];

  function resolved(value) {
    resolutions.push(value);
  }

  function rejected(error) {
    rejections.push(error);
  }

  var wait = Promise.all(promises.map(function (promise) {
    return promise.then(resolved, rejected);
  }));
  return wait.then(function () {
    return {
      successful: resolutions,
      failed: rejections
    };
  });
};

},{}],72:[function(require,module,exports){
/**
 * Converts list into array
 */
module.exports = function toArray(list) {
  return Array.prototype.slice.call(list || [], 0);
};

},{}],73:[function(require,module,exports){
require('es6-promise/auto');

require('whatwg-fetch');

var Uppy = require('./../../../../packages/@uppy/core');

var DragDrop = require('./../../../../packages/@uppy/drag-drop');

var ProgressBar = require('./../../../../packages/@uppy/progress-bar');

var Tus = require('./../../../../packages/@uppy/tus'); // Function for displaying uploaded files


var onUploadSuccess = function onUploadSuccess(elForUploadedFiles) {
  return function (file, response) {
    var url = response.uploadURL;
    var fileName = file.name;
    document.querySelector(elForUploadedFiles).innerHTML += "<li><a href=\"" + url + "\" target=\"_blank\">" + fileName + "</a></li>";
  };
};

var uppyOne = new Uppy({
  debug: true,
  autoProceed: true
});
uppyOne.use(DragDrop, {
  target: '.example-one .for-DragDrop'
}).use(Tus, {
  endpoint: 'https://master.tus.io/files/'
}).use(ProgressBar, {
  target: '.example-one .for-ProgressBar',
  hideAfterFinish: false
}).on('upload-success', onUploadSuccess('.example-one .uploaded-files ol'));
var uppyTwo = new Uppy({
  debug: true,
  autoProceed: false
});
uppyTwo.use(DragDrop, {
  target: '.example-two .for-DragDrop'
}).use(Tus, {
  endpoint: 'https://master.tus.io/files/'
}).use(ProgressBar, {
  target: '.example-two .for-ProgressBar',
  hideAfterFinish: false
}).on('upload-success', onUploadSuccess('.example-two .uploaded-files ol'));
var uploadBtn = document.querySelector('.example-two button.upload-button');
uploadBtn.addEventListener('click', function () {
  uppyTwo.upload();
});

},{"./../../../../packages/@uppy/core":28,"./../../../../packages/@uppy/drag-drop":32,"./../../../../packages/@uppy/progress-bar":34,"./../../../../packages/@uppy/tus":50,"es6-promise/auto":5,"whatwg-fetch":17}]},{},[73])
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIi4uL25vZGVfbW9kdWxlcy9icm93c2VyLXBhY2svX3ByZWx1ZGUuanMiLCIuLi9ub2RlX21vZHVsZXMvY3VpZC9pbmRleC5qcyIsIi4uL25vZGVfbW9kdWxlcy9jdWlkL2xpYi9maW5nZXJwcmludC5icm93c2VyLmpzIiwiLi4vbm9kZV9tb2R1bGVzL2N1aWQvbGliL2dldFJhbmRvbVZhbHVlLmJyb3dzZXIuanMiLCIuLi9ub2RlX21vZHVsZXMvY3VpZC9saWIvcGFkLmpzIiwiLi4vbm9kZV9tb2R1bGVzL2VzNi1wcm9taXNlL2F1dG8uanMiLCIuLi9ub2RlX21vZHVsZXMvZXM2LXByb21pc2UvZGlzdC9lczYtcHJvbWlzZS5qcyIsIi4uL25vZGVfbW9kdWxlcy9leHRlbmQvaW5kZXguanMiLCIuLi9ub2RlX21vZHVsZXMvanMtYmFzZTY0L2Jhc2U2NC5qcyIsIi4uL25vZGVfbW9kdWxlcy9sb2Rhc2gudGhyb3R0bGUvaW5kZXguanMiLCIuLi9ub2RlX21vZHVsZXMvbWltZS1tYXRjaC9pbmRleC5qcyIsIi4uL25vZGVfbW9kdWxlcy9uYW1lc3BhY2UtZW1pdHRlci9pbmRleC5qcyIsIi4uL25vZGVfbW9kdWxlcy9wcmVhY3QvZGlzdC9wcmVhY3QuanMiLCIuLi9ub2RlX21vZHVsZXMvcHJvY2Vzcy9icm93c2VyLmpzIiwiLi4vbm9kZV9tb2R1bGVzL3F1ZXJ5c3RyaW5naWZ5L2luZGV4LmpzIiwiLi4vbm9kZV9tb2R1bGVzL3JlcXVpcmVzLXBvcnQvaW5kZXguanMiLCIuLi9ub2RlX21vZHVsZXMvdXJsLXBhcnNlL2luZGV4LmpzIiwiLi4vbm9kZV9tb2R1bGVzL3doYXR3Zy1mZXRjaC9kaXN0L2ZldGNoLnVtZC5qcyIsIi4uL25vZGVfbW9kdWxlcy93aWxkY2FyZC9pbmRleC5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L2NvbXBhbmlvbi1jbGllbnQvcGFja2FnZS5qc29uIiwiLi4vcGFja2FnZXMvQHVwcHkvY29tcGFuaW9uLWNsaWVudC9zcmMvQXV0aEVycm9yLmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvY29tcGFuaW9uLWNsaWVudC9zcmMvUHJvdmlkZXIuanMiLCIuLi9wYWNrYWdlcy9AdXBweS9jb21wYW5pb24tY2xpZW50L3NyYy9SZXF1ZXN0Q2xpZW50LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvY29tcGFuaW9uLWNsaWVudC9zcmMvU29ja2V0LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvY29tcGFuaW9uLWNsaWVudC9zcmMvaW5kZXguanMiLCIuLi9wYWNrYWdlcy9AdXBweS9jb21wYW5pb24tY2xpZW50L3NyYy90b2tlblN0b3JhZ2UuanMiLCIuLi9wYWNrYWdlcy9AdXBweS9jb3JlL3BhY2thZ2UuanNvbiIsIi4uL3BhY2thZ2VzL0B1cHB5L2NvcmUvc3JjL1BsdWdpbi5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L2NvcmUvc3JjL2luZGV4LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvY29yZS9zcmMvbG9nZ2Vycy5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L2NvcmUvc3JjL3N1cHBvcnRzVXBsb2FkUHJvZ3Jlc3MuanMiLCIuLi9wYWNrYWdlcy9AdXBweS9kcmFnLWRyb3AvcGFja2FnZS5qc29uIiwiLi4vcGFja2FnZXMvQHVwcHkvZHJhZy1kcm9wL3NyYy9pbmRleC5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3Byb2dyZXNzLWJhci9wYWNrYWdlLmpzb24iLCIuLi9wYWNrYWdlcy9AdXBweS9wcm9ncmVzcy1iYXIvc3JjL2luZGV4LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvc3RvcmUtZGVmYXVsdC9wYWNrYWdlLmpzb24iLCIuLi9wYWNrYWdlcy9AdXBweS9zdG9yZS1kZWZhdWx0L3NyYy9pbmRleC5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3R1cy9ub2RlX21vZHVsZXMvdHVzLWpzLWNsaWVudC9saWIuZXM1L2Jyb3dzZXIvZmluZ2VycHJpbnQuanMiLCIuLi9wYWNrYWdlcy9AdXBweS90dXMvbm9kZV9tb2R1bGVzL3R1cy1qcy1jbGllbnQvbGliLmVzNS9icm93c2VyL2lzQ29yZG92YS5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3R1cy9ub2RlX21vZHVsZXMvdHVzLWpzLWNsaWVudC9saWIuZXM1L2Jyb3dzZXIvaXNSZWFjdE5hdGl2ZS5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3R1cy9ub2RlX21vZHVsZXMvdHVzLWpzLWNsaWVudC9saWIuZXM1L2Jyb3dzZXIvcmVhZEFzQnl0ZUFycmF5LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdHVzL25vZGVfbW9kdWxlcy90dXMtanMtY2xpZW50L2xpYi5lczUvYnJvd3Nlci9yZXF1ZXN0LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdHVzL25vZGVfbW9kdWxlcy90dXMtanMtY2xpZW50L2xpYi5lczUvYnJvd3Nlci9zb3VyY2UuanMiLCIuLi9wYWNrYWdlcy9AdXBweS90dXMvbm9kZV9tb2R1bGVzL3R1cy1qcy1jbGllbnQvbGliLmVzNS9icm93c2VyL3N0b3JhZ2UuanMiLCIuLi9wYWNrYWdlcy9AdXBweS90dXMvbm9kZV9tb2R1bGVzL3R1cy1qcy1jbGllbnQvbGliLmVzNS9icm93c2VyL3VyaVRvQmxvYi5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3R1cy9ub2RlX21vZHVsZXMvdHVzLWpzLWNsaWVudC9saWIuZXM1L2Vycm9yLmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdHVzL25vZGVfbW9kdWxlcy90dXMtanMtY2xpZW50L2xpYi5lczUvaW5kZXguanMiLCIuLi9wYWNrYWdlcy9AdXBweS90dXMvbm9kZV9tb2R1bGVzL3R1cy1qcy1jbGllbnQvbGliLmVzNS91cGxvYWQuanMiLCIuLi9wYWNrYWdlcy9AdXBweS90dXMvcGFja2FnZS5qc29uIiwiLi4vcGFja2FnZXMvQHVwcHkvdHVzL3NyYy9nZXRGaW5nZXJwcmludC5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3R1cy9zcmMvaW5kZXguanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvRXZlbnRUcmFja2VyLmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdXRpbHMvc3JjL1JhdGVMaW1pdGVkUXVldWUuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvVHJhbnNsYXRvci5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3V0aWxzL3NyYy9lbWl0U29ja2V0UHJvZ3Jlc3MuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZmluZERPTUVsZW1lbnQuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2VuZXJhdGVGaWxlSUQuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2V0RHJvcHBlZEZpbGVzL2luZGV4LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdXRpbHMvc3JjL2dldERyb3BwZWRGaWxlcy91dGlscy9mYWxsYmFja0FwaS5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3V0aWxzL3NyYy9nZXREcm9wcGVkRmlsZXMvdXRpbHMvd2Via2l0R2V0QXNFbnRyeUFwaS9nZXRGaWxlc0FuZERpcmVjdG9yaWVzRnJvbURpcmVjdG9yeS5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3V0aWxzL3NyYy9nZXREcm9wcGVkRmlsZXMvdXRpbHMvd2Via2l0R2V0QXNFbnRyeUFwaS9nZXRSZWxhdGl2ZVBhdGguanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2V0RHJvcHBlZEZpbGVzL3V0aWxzL3dlYmtpdEdldEFzRW50cnlBcGkvaW5kZXguanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2V0RmlsZU5hbWVBbmRFeHRlbnNpb24uanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2V0RmlsZVR5cGUuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvZ2V0U29ja2V0SG9zdC5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3V0aWxzL3NyYy9nZXRUaW1lU3RhbXAuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvaGFzUHJvcGVydHkuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvaXNET01FbGVtZW50LmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdXRpbHMvc3JjL2lzRHJhZ0Ryb3BTdXBwb3J0ZWQuanMiLCIuLi9wYWNrYWdlcy9AdXBweS91dGlscy9zcmMvbWltZVR5cGVzLmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdXRpbHMvc3JjL3ByZXR0eUJ5dGVzLmpzIiwiLi4vcGFja2FnZXMvQHVwcHkvdXRpbHMvc3JjL3NldHRsZS5qcyIsIi4uL3BhY2thZ2VzL0B1cHB5L3V0aWxzL3NyYy90b0FycmF5LmpzIiwic3JjL2V4YW1wbGVzL2RyYWdkcm9wL2FwcC5lczYiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7QUNBQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNwRkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDWkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDZkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNKQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOzs7QUNKQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOzs7O0FDL3BDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7O0FDckhBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7Ozs7O0FDM09BO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7Ozs7QUN2YkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDeEJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDeElBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUN2WkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUN4TEE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUN0SEE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOzs7QUN0Q0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7Ozs7QUNsY0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDbmhCQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7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U0FBUyxZQUFULEdBQXlCO0FBQ3hDLFNBQU8sSUFBSSxZQUFKLEVBQVA7QUFDRCxDQUZEOzs7QUN6Q0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQ3JEQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNUQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNUQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNyQkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNuQkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQzlNQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDOURBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQ3hCQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQ3hDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUMxQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDMXNCQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDaENBLElB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QUcsSUFBSSxJQUFKLENBQVM7QUFBRSxFQUFBLEtBQUssRUFBRSxJQUFUO0FBQWUsRUFBQSxXQUFXLEVBQUU7QUFBNUIsQ0FBVCxDQUFoQjtBQUNBLE9BQU8sQ0FDSixHQURILENBQ08sUUFEUCxFQUNpQjtBQUFFLEVBQUEsTUFBTSxFQUFFO0FBQVYsQ0FEakIsRUFFRyxHQUZILENBRU8sR0FGUCxFQUVZO0FBQUUsRUFBQSxRQUFRLEVBQUU7QUFBWixDQUZaLEVBR0csR0FISCxDQUdPLFdBSFAsRUFHb0I7QUFBRSxFQUFBLE1BQU0sRUFBRSwrQkFBVjtBQUEyQyxFQUFBLGVBQWUsRUFBRTtBQUE1RCxDQUhwQixFQUlHLEVBSkgsQ0FJTSxnQkFKTixFQUl3QixlQUFlLENBQUMsaUNBQUQsQ0FKdkM7QUFNQSxJQUFNLFNBQVMsR0FBRyxRQUFRLENBQUMsYUFBVCxDQUF1QixtQ0FBdkIsQ0FBbEI7QUFDQSxTQUFTLENBQUMsZ0JBQVYsQ0FBMkIsT0FBM0IsRUFBb0MsWUFBWTtBQUM5QyxFQUFBLE9BQU8sQ0FBQyxNQUFSO0FBQ0QsQ0FGRCIsImZpbGUiOiJnZW5lcmF0ZWQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlc0NvbnRlbnQiOlsiKGZ1bmN0aW9uKCl7ZnVuY3Rpb24gcihlLG4sdCl7ZnVuY3Rpb24gbyhpLGYpe2lmKCFuW2ldKXtpZighZVtpXSl7dmFyIGM9XCJmdW5jdGlvblwiPT10eXBlb2YgcmVxdWlyZSYmcmVxdWlyZTtpZighZiYmYylyZXR1cm4gYyhpLCEwKTtpZih1KXJldHVybiB1KGksITApO3ZhciBhPW5ldyBFcnJvcihcIkNhbm5vdCBmaW5kIG1vZHVsZSAnXCIraStcIidcIik7dGhyb3cgYS5jb2RlPVwiTU9EVUxFX05PVF9GT1VORFwiLGF9dmFyIHA9bltpXT17ZXhwb3J0czp7fX07ZVtpXVswXS5jYWxsKHAuZXhwb3J0cyxmdW5jdGlvbihyKXt2YXIgbj1lW2ldWzFdW3JdO3JldHVybiBvKG58fHIpfSxwLHAuZXhwb3J0cyxyLGUsbix0KX1yZXR1cm4gbltpXS5leHBvcnRzfWZvcih2YXIgdT1cImZ1bmN0aW9uXCI9PXR5cGVvZiByZXF1aXJlJiZyZXF1aXJlLGk9MDtpPHQubGVuZ3RoO2krKylvKHRbaV0pO3JldHVybiBvfXJldHVybiByfSkoKSIsIi8qKlxuICogY3VpZC5qc1xuICogQ29sbGlzaW9uLXJlc2lzdGFudCBVSUQgZ2VuZXJhdG9yIGZvciBicm93c2VycyBhbmQgbm9kZS5cbiAqIFNlcXVlbnRpYWwgZm9yIGZhc3QgZGIgbG9va3VwcyBhbmQgcmVjZW5jeSBzb3J0aW5nLlxuICogU2FmZSBmb3IgZWxlbWVudCBJRHMgYW5kIHNlcnZlci1zaWRlIGxvb2t1cHMuXG4gKlxuICogRXh0cmFjdGVkIGZyb20gQ0xDVFJcbiAqXG4gKiBDb3B5cmlnaHQgKGMpIEVyaWMgRWxsaW90dCAyMDEyXG4gKiBNSVQgTGljZW5zZVxuICovXG5cbnZhciBmaW5nZXJwcmludCA9IHJlcXVpcmUoJy4vbGliL2ZpbmdlcnByaW50LmpzJyk7XG52YXIgcGFkID0gcmVxdWlyZSgnLi9saWIvcGFkLmpzJyk7XG52YXIgZ2V0UmFuZG9tVmFsdWUgPSByZXF1aXJlKCcuL2xpYi9nZXRSYW5kb21WYWx1ZS5qcycpO1xuXG52YXIgYyA9IDAsXG4gIGJsb2NrU2l6ZSA9IDQsXG4gIGJhc2UgPSAzNixcbiAgZGlzY3JldGVWYWx1ZXMgPSBNYXRoLnBvdyhiYXNlLCBibG9ja1NpemUpO1xuXG5mdW5jdGlvbiByYW5kb21CbG9jayAoKSB7XG4gIHJldHVybiBwYWQoKGdldFJhbmRvbVZhbHVlKCkgKlxuICAgIGRpc2NyZXRlVmFsdWVzIDw8IDApXG4gICAgLnRvU3RyaW5nKGJhc2UpLCBibG9ja1NpemUpO1xufVxuXG5mdW5jdGlvbiBzYWZlQ291bnRlciAoKSB7XG4gIGMgPSBjIDwgZGlzY3JldGVWYWx1ZXMgPyBjIDogMDtcbiAgYysrOyAvLyB0aGlzIGlzIG5vdCBzdWJsaW1pbmFsXG4gIHJldHVybiBjIC0gMTtcbn1cblxuZnVuY3Rpb24gY3VpZCAoKSB7XG4gIC8vIFN0YXJ0aW5nIHdpdGggYSBsb3dlcmNhc2UgbGV0dGVyIG1ha2VzXG4gIC8vIGl0IEhUTUwgZWxlbWVudCBJRCBmcmllbmRseS5cbiAgdmFyIGxldHRlciA9ICdjJywgLy8gaGFyZC1jb2RlZCBhbGxvd3MgZm9yIHNlcXVlbnRpYWwgYWNjZXNzXG5cbiAgICAvLyB0aW1lc3RhbXBcbiAgICAvLyB3YXJuaW5nOiB0aGlzIGV4cG9zZXMgdGhlIGV4YWN0IGRhdGUgYW5kIHRpbWVcbiAgICAvLyB0aGF0IHRoZSB1aWQgd2FzIGNyZWF0ZWQuXG4gICAgdGltZXN0YW1wID0gKG5ldyBEYXRlKCkuZ2V0VGltZSgpKS50b1N0cmluZyhiYXNlKSxcblxuICAgIC8vIFByZXZlbnQgc2FtZS1tYWNoaW5lIGNvbGxpc2lvbnMuXG4gICAgY291bnRlciA9IHBhZChzYWZlQ291bnRlcigpLnRvU3RyaW5nKGJhc2UpLCBibG9ja1NpemUpLFxuXG4gICAgLy8gQSBmZXcgY2hhcnMgdG8gZ2VuZXJhdGUgZGlzdGluY3QgaWRzIGZvciBkaWZmZXJlbnRcbiAgICAvLyBjbGllbnRzIChzbyBkaWZmZXJlbnQgY29tcHV0ZXJzIGFyZSBmYXIgbGVzc1xuICAgIC8vIGxpa2VseSB0byBnZW5lcmF0ZSB0aGUgc2FtZSBpZClcbiAgICBwcmludCA9IGZpbmdlcnByaW50KCksXG5cbiAgICAvLyBHcmFiIHNvbWUgbW9yZSBjaGFycyBmcm9tIE1hdGgucmFuZG9tKClcbiAgICByYW5kb20gPSByYW5kb21CbG9jaygpICsgcmFuZG9tQmxvY2soKTtcblxuICByZXR1cm4gbGV0dGVyICsgdGltZXN0YW1wICsgY291bnRlciArIHByaW50ICsgcmFuZG9tO1xufVxuXG5jdWlkLnNsdWcgPSBmdW5jdGlvbiBzbHVnICgpIHtcbiAgdmFyIGRhdGUgPSBuZXcgRGF0ZSgpLmdldFRpbWUoKS50b1N0cmluZygzNiksXG4gICAgY291bnRlciA9IHNhZmVDb3VudGVyKCkudG9TdHJpbmcoMzYpLnNsaWNlKC00KSxcbiAgICBwcmludCA9IGZpbmdlcnByaW50KCkuc2xpY2UoMCwgMSkgK1xuICAgICAgZmluZ2VycHJpbnQoKS5zbGljZSgtMSksXG4gICAgcmFuZG9tID0gcmFuZG9tQmxvY2soKS5zbGljZSgtMik7XG5cbiAgcmV0dXJuIGRhdGUuc2xpY2UoLTIpICtcbiAgICBjb3VudGVyICsgcHJpbnQgKyByYW5kb207XG59O1xuXG5jdWlkLmlzQ3VpZCA9IGZ1bmN0aW9uIGlzQ3VpZCAoc3RyaW5nVG9DaGVjaykge1xuICBpZiAodHlwZW9mIHN0cmluZ1RvQ2hlY2sgIT09ICdzdHJpbmcnKSByZXR1cm4gZmFsc2U7XG4gIGlmIChzdHJpbmdUb0NoZWNrLnN0YXJ0c1dpdGgoJ2MnKSkgcmV0dXJuIHRydWU7XG4gIHJldHVybiBmYWxzZTtcbn07XG5cbmN1aWQuaXNTbHVnID0gZnVuY3Rpb24gaXNTbHVnIChzdHJpbmdUb0NoZWNrKSB7XG4gIGlmICh0eXBlb2Ygc3RyaW5nVG9DaGVjayAhPT0gJ3N0cmluZycpIHJldHVybiBmYWxzZTtcbiAgdmFyIHN0cmluZ0xlbmd0aCA9IHN0cmluZ1RvQ2hlY2subGVuZ3RoO1xuICBpZiAoc3RyaW5nTGVuZ3RoID49IDcgJiYgc3RyaW5nTGVuZ3RoIDw9IDEwKSByZXR1cm4gdHJ1ZTtcbiAgcmV0dXJuIGZhbHNlO1xufTtcblxuY3VpZC5maW5nZXJwcmludCA9IGZpbmdlcnByaW50O1xuXG5tb2R1bGUuZXhwb3J0cyA9IGN1aWQ7XG4iLCJ2YXIgcGFkID0gcmVxdWlyZSgnLi9wYWQuanMnKTtcblxudmFyIGVudiA9IHR5cGVvZiB3aW5kb3cgPT09ICdvYmplY3QnID8gd2luZG93IDogc2VsZjtcbnZhciBnbG9iYWxDb3VudCA9IE9iamVjdC5rZXlzKGVudikubGVuZ3RoO1xudmFyIG1pbWVUeXBlc0xlbmd0aCA9IG5hdmlnYXRvci5taW1lVHlwZXMgPyBuYXZpZ2F0b3IubWltZVR5cGVzLmxlbmd0aCA6IDA7XG52YXIgY2xpZW50SWQgPSBwYWQoKG1pbWVUeXBlc0xlbmd0aCArXG4gIG5hdmlnYXRvci51c2VyQWdlbnQubGVuZ3RoKS50b1N0cmluZygzNikgK1xuICBnbG9iYWxDb3VudC50b1N0cmluZygzNiksIDQpO1xuXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGZpbmdlcnByaW50ICgpIHtcbiAgcmV0dXJuIGNsaWVudElkO1xufTtcbiIsIlxudmFyIGdldFJhbmRvbVZhbHVlO1xuXG52YXIgY3J5cHRvID0gd2luZG93LmNyeXB0byB8fCB3aW5kb3cubXNDcnlwdG87XG5cbmlmIChjcnlwdG8pIHtcbiAgICB2YXIgbGltID0gTWF0aC5wb3coMiwgMzIpIC0gMTtcbiAgICBnZXRSYW5kb21WYWx1ZSA9IGZ1bmN0aW9uICgpIHtcbiAgICAgICAgcmV0dXJuIE1hdGguYWJzKGNyeXB0by5nZXRSYW5kb21WYWx1ZXMobmV3IFVpbnQzMkFycmF5KDEpKVswXSAvIGxpbSk7XG4gICAgfTtcbn0gZWxzZSB7XG4gICAgZ2V0UmFuZG9tVmFsdWUgPSBNYXRoLnJhbmRvbTtcbn1cblxubW9kdWxlLmV4cG9ydHMgPSBnZXRSYW5kb21WYWx1ZTtcbiIsIm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gcGFkIChudW0sIHNpemUpIHtcbiAgdmFyIHMgPSAnMDAwMDAwMDAwJyArIG51bTtcbiAgcmV0dXJuIHMuc3Vic3RyKHMubGVuZ3RoIC0gc2l6ZSk7XG59O1xuIiwiLy8gVGhpcyBmaWxlIGNhbiBiZSByZXF1aXJlZCBpbiBCcm93c2VyaWZ5IGFuZCBOb2RlLmpzIGZvciBhdXRvbWF0aWMgcG9seWZpbGxcbi8vIFRvIHVzZSBpdDogIHJlcXVpcmUoJ2VzNi1wcm9taXNlL2F1dG8nKTtcbid1c2Ugc3RyaWN0Jztcbm1vZHVsZS5leHBvcnRzID0gcmVxdWlyZSgnLi8nKS5wb2x5ZmlsbCgpO1xuIiwiLyohXG4gKiBAb3ZlcnZpZXcgZXM2LXByb21pc2UgLSBhIHRpbnkgaW1wbGVtZW50YXRpb24gb2YgUHJvbWlzZXMvQSsuXG4gKiBAY29weXJpZ2h0IENvcHlyaWdodCAoYykgMjAxNCBZZWh1ZGEgS2F0eiwgVG9tIERhbGUsIFN0ZWZhbiBQZW5uZXIgYW5kIGNvbnRyaWJ1dG9ycyAoQ29udmVyc2lvbiB0byBFUzYgQVBJIGJ5IEpha2UgQXJjaGliYWxkKVxuICogQGxpY2Vuc2UgICBMaWNlbnNlZCB1bmRlciBNSVQgbGljZW5zZVxuICogICAgICAgICAgICBTZWUgaHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL3N0ZWZhbnBlbm5lci9lczYtcHJvbWlzZS9tYXN0ZXIvTElDRU5TRVxuICogQHZlcnNpb24gICB2NC4yLjUrN2YyYjUyNmRcbiAqL1xuXG4oZnVuY3Rpb24gKGdsb2JhbCwgZmFjdG9yeSkge1xuXHR0eXBlb2YgZXhwb3J0cyA9PT0gJ29iamVjdCcgJiYgdHlwZW9mIG1vZHVsZSAhPT0gJ3VuZGVmaW5lZCcgPyBtb2R1bGUuZXhwb3J0cyA9IGZhY3RvcnkoKSA6XG5cdHR5cGVvZiBkZWZpbmUgPT09ICdmdW5jdGlvbicgJiYgZGVmaW5lLmFtZCA/IGRlZmluZShmYWN0b3J5KSA6XG5cdChnbG9iYWwuRVM2UHJvbWlzZSA9IGZhY3RvcnkoKSk7XG59KHRoaXMsIChmdW5jdGlvbiAoKSB7ICd1c2Ugc3RyaWN0JztcblxuZnVuY3Rpb24gb2JqZWN0T3JGdW5jdGlvbih4KSB7XG4gIHZhciB0eXBlID0gdHlwZW9mIHg7XG4gIHJldHVybiB4ICE9PSBudWxsICYmICh0eXBlID09PSAnb2JqZWN0JyB8fCB0eXBlID09PSAnZnVuY3Rpb24nKTtcbn1cblxuZnVuY3Rpb24gaXNGdW5jdGlvbih4KSB7XG4gIHJldHVybiB0eXBlb2YgeCA9PT0gJ2Z1bmN0aW9uJztcbn1cblxuXG5cbnZhciBfaXNBcnJheSA9IHZvaWQgMDtcbmlmIChBcnJheS5pc0FycmF5KSB7XG4gIF9pc0FycmF5ID0gQXJyYXkuaXNBcnJheTtcbn0gZWxzZSB7XG4gIF9pc0FycmF5ID0gZnVuY3Rpb24gKHgpIHtcbiAgICByZXR1cm4gT2JqZWN0LnByb3RvdHlwZS50b1N0cmluZy5jYWxsKHgpID09PSAnW29iamVjdCBBcnJheV0nO1xuICB9O1xufVxuXG52YXIgaXNBcnJheSA9IF9pc0FycmF5O1xuXG52YXIgbGVuID0gMDtcbnZhciB2ZXJ0eE5leHQgPSB2b2lkIDA7XG52YXIgY3VzdG9tU2NoZWR1bGVyRm4gPSB2b2lkIDA7XG5cbnZhciBhc2FwID0gZnVuY3Rpb24gYXNhcChjYWxsYmFjaywgYXJnKSB7XG4gIHF1ZXVlW2xlbl0gPSBjYWxsYmFjaztcbiAgcXVldWVbbGVuICsgMV0gPSBhcmc7XG4gIGxlbiArPSAyO1xuICBpZiAobGVuID09PSAyKSB7XG4gICAgLy8gSWYgbGVuIGlzIDIsIHRoYXQgbWVhbnMgdGhhdCB3ZSBuZWVkIHRvIHNjaGVkdWxlIGFuIGFzeW5jIGZsdXNoLlxuICAgIC8vIElmIGFkZGl0aW9uYWwgY2FsbGJhY2tzIGFyZSBxdWV1ZWQgYmVmb3JlIHRoZSBxdWV1ZSBpcyBmbHVzaGVkLCB0aGV5XG4gICAgLy8gd2lsbCBiZSBwcm9jZXNzZWQgYnkgdGhpcyBmbHVzaCB0aGF0IHdlIGFyZSBzY2hlZHVsaW5nLlxuICAgIGlmIChjdXN0b21TY2hlZHVsZXJGbikge1xuICAgICAgY3VzdG9tU2NoZWR1bGVyRm4oZmx1c2gpO1xuICAgIH0gZWxzZSB7XG4gICAgICBzY2hlZHVsZUZsdXNoKCk7XG4gICAgfVxuICB9XG59O1xuXG5mdW5jdGlvbiBzZXRTY2hlZHVsZXIoc2NoZWR1bGVGbikge1xuICBjdXN0b21TY2hlZHVsZXJGbiA9IHNjaGVkdWxlRm47XG59XG5cbmZ1bmN0aW9uIHNldEFzYXAoYXNhcEZuKSB7XG4gIGFzYXAgPSBhc2FwRm47XG59XG5cbnZhciBicm93c2VyV2luZG93ID0gdHlwZW9mIHdpbmRvdyAhPT0gJ3VuZGVmaW5lZCcgPyB3aW5kb3cgOiB1bmRlZmluZWQ7XG52YXIgYnJvd3Nlckdsb2JhbCA9IGJyb3dzZXJXaW5kb3cgfHwge307XG52YXIgQnJvd3Nlck11dGF0aW9uT2JzZXJ2ZXIgPSBicm93c2VyR2xvYmFsLk11dGF0aW9uT2JzZXJ2ZXIgfHwgYnJvd3Nlckdsb2JhbC5XZWJLaXRNdXRhdGlvbk9ic2VydmVyO1xudmFyIGlzTm9kZSA9IHR5cGVvZiBzZWxmID09PSAndW5kZWZpbmVkJyAmJiB0eXBlb2YgcHJvY2VzcyAhPT0gJ3VuZGVmaW5lZCcgJiYge30udG9TdHJpbmcuY2FsbChwcm9jZXNzKSA9PT0gJ1tvYmplY3QgcHJvY2Vzc10nO1xuXG4vLyB0ZXN0IGZvciB3ZWIgd29ya2VyIGJ1dCBub3QgaW4gSUUxMFxudmFyIGlzV29ya2VyID0gdHlwZW9mIFVpbnQ4Q2xhbXBlZEFycmF5ICE9PSAndW5kZWZpbmVkJyAmJiB0eXBlb2YgaW1wb3J0U2NyaXB0cyAhPT0gJ3VuZGVmaW5lZCcgJiYgdHlwZW9mIE1lc3NhZ2VDaGFubmVsICE9PSAndW5kZWZpbmVkJztcblxuLy8gbm9kZVxuZnVuY3Rpb24gdXNlTmV4dFRpY2soKSB7XG4gIC8vIG5vZGUgdmVyc2lvbiAwLjEwLnggZGlzcGxheXMgYSBkZXByZWNhdGlvbiB3YXJuaW5nIHdoZW4gbmV4dFRpY2sgaXMgdXNlZCByZWN1cnNpdmVseVxuICAvLyBzZWUgaHR0cHM6Ly9naXRodWIuY29tL2N1am9qcy93aGVuL2lzc3Vlcy80MTAgZm9yIGRldGFpbHNcbiAgcmV0dXJuIGZ1bmN0aW9uICgpIHtcbiAgICByZXR1cm4gcHJvY2Vzcy5uZXh0VGljayhmbHVzaCk7XG4gIH07XG59XG5cbi8vIHZlcnR4XG5mdW5jdGlvbiB1c2VWZXJ0eFRpbWVyKCkge1xuICBpZiAodHlwZW9mIHZlcnR4TmV4dCAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICByZXR1cm4gZnVuY3Rpb24gKCkge1xuICAgICAgdmVydHhOZXh0KGZsdXNoKTtcbiAgICB9O1xuICB9XG5cbiAgcmV0dXJuIHVzZVNldFRpbWVvdXQoKTtcbn1cblxuZnVuY3Rpb24gdXNlTXV0YXRpb25PYnNlcnZlcigpIHtcbiAgdmFyIGl0ZXJhdGlvbnMgPSAwO1xuICB2YXIgb2JzZXJ2ZXIgPSBuZXcgQnJvd3Nlck11dGF0aW9uT2JzZXJ2ZXIoZmx1c2gpO1xuICB2YXIgbm9kZSA9IGRvY3VtZW50LmNyZWF0ZVRleHROb2RlKCcnKTtcbiAgb2JzZXJ2ZXIub2JzZXJ2ZShub2RlLCB7IGNoYXJhY3RlckRhdGE6IHRydWUgfSk7XG5cbiAgcmV0dXJuIGZ1bmN0aW9uICgpIHtcbiAgICBub2RlLmRhdGEgPSBpdGVyYXRpb25zID0gKytpdGVyYXRpb25zICUgMjtcbiAgfTtcbn1cblxuLy8gd2ViIHdvcmtlclxuZnVuY3Rpb24gdXNlTWVzc2FnZUNoYW5uZWwoKSB7XG4gIHZhciBjaGFubmVsID0gbmV3IE1lc3NhZ2VDaGFubmVsKCk7XG4gIGNoYW5uZWwucG9ydDEub25tZXNzYWdlID0gZmx1c2g7XG4gIHJldHVybiBmdW5jdGlvbiAoKSB7XG4gICAgcmV0dXJuIGNoYW5uZWwucG9ydDIucG9zdE1lc3NhZ2UoMCk7XG4gIH07XG59XG5cbmZ1bmN0aW9uIHVzZVNldFRpbWVvdXQoKSB7XG4gIC8vIFN0b3JlIHNldFRpbWVvdXQgcmVmZXJlbmNlIHNvIGVzNi1wcm9taXNlIHdpbGwgYmUgdW5hZmZlY3RlZCBieVxuICAvLyBvdGhlciBjb2RlIG1vZGlmeWluZyBzZXRUaW1lb3V0IChsaWtlIHNpbm9uLnVzZUZha2VUaW1lcnMoKSlcbiAgdmFyIGdsb2JhbFNldFRpbWVvdXQgPSBzZXRUaW1lb3V0O1xuICByZXR1cm4gZnVuY3Rpb24gKCkge1xuICAgIHJldHVybiBnbG9iYWxTZXRUaW1lb3V0KGZsdXNoLCAxKTtcbiAgfTtcbn1cblxudmFyIHF1ZXVlID0gbmV3IEFycmF5KDEwMDApO1xuZnVuY3Rpb24gZmx1c2goKSB7XG4gIGZvciAodmFyIGkgPSAwOyBpIDwgbGVuOyBpICs9IDIpIHtcbiAgICB2YXIgY2FsbGJhY2sgPSBxdWV1ZVtpXTtcbiAgICB2YXIgYXJnID0gcXVldWVbaSArIDFdO1xuXG4gICAgY2FsbGJhY2soYXJnKTtcblxuICAgIHF1ZXVlW2ldID0gdW5kZWZpbmVkO1xuICAgIHF1ZXVlW2kgKyAxXSA9IHVuZGVmaW5lZDtcbiAgfVxuXG4gIGxlbiA9IDA7XG59XG5cbmZ1bmN0aW9uIGF0dGVtcHRWZXJ0eCgpIHtcbiAgdHJ5IHtcbiAgICB2YXIgdmVydHggPSBGdW5jdGlvbigncmV0dXJuIHRoaXMnKSgpLnJlcXVpcmUoJ3ZlcnR4Jyk7XG4gICAgdmVydHhOZXh0ID0gdmVydHgucnVuT25Mb29wIHx8IHZlcnR4LnJ1bk9uQ29udGV4dDtcbiAgICByZXR1cm4gdXNlVmVydHhUaW1lcigpO1xuICB9IGNhdGNoIChlKSB7XG4gICAgcmV0dXJuIHVzZVNldFRpbWVvdXQoKTtcbiAgfVxufVxuXG52YXIgc2NoZWR1bGVGbHVzaCA9IHZvaWQgMDtcbi8vIERlY2lkZSB3aGF0IGFzeW5jIG1ldGhvZCB0byB1c2UgdG8gdHJpZ2dlcmluZyBwcm9jZXNzaW5nIG9mIHF1ZXVlZCBjYWxsYmFja3M6XG5pZiAoaXNOb2RlKSB7XG4gIHNjaGVkdWxlRmx1c2ggPSB1c2VOZXh0VGljaygpO1xufSBlbHNlIGlmIChCcm93c2VyTXV0YXRpb25PYnNlcnZlcikge1xuICBzY2hlZHVsZUZsdXNoID0gdXNlTXV0YXRpb25PYnNlcnZlcigpO1xufSBlbHNlIGlmIChpc1dvcmtlcikge1xuICBzY2hlZHVsZUZsdXNoID0gdXNlTWVzc2FnZUNoYW5uZWwoKTtcbn0gZWxzZSBpZiAoYnJvd3NlcldpbmRvdyA9PT0gdW5kZWZpbmVkICYmIHR5cGVvZiByZXF1aXJlID09PSAnZnVuY3Rpb24nKSB7XG4gIHNjaGVkdWxlRmx1c2ggPSBhdHRlbXB0VmVydHgoKTtcbn0gZWxzZSB7XG4gIHNjaGVkdWxlRmx1c2ggPSB1c2VTZXRUaW1lb3V0KCk7XG59XG5cbmZ1bmN0aW9uIHRoZW4ob25GdWxmaWxsbWVudCwgb25SZWplY3Rpb24pIHtcbiAgdmFyIHBhcmVudCA9IHRoaXM7XG5cbiAgdmFyIGNoaWxkID0gbmV3IHRoaXMuY29uc3RydWN0b3Iobm9vcCk7XG5cbiAgaWYgKGNoaWxkW1BST01JU0VfSURdID09PSB1bmRlZmluZWQpIHtcbiAgICBtYWtlUHJvbWlzZShjaGlsZCk7XG4gIH1cblxuICB2YXIgX3N0YXRlID0gcGFyZW50Ll9zdGF0ZTtcblxuXG4gIGlmIChfc3RhdGUpIHtcbiAgICB2YXIgY2FsbGJhY2sgPSBhcmd1bWVudHNbX3N0YXRlIC0gMV07XG4gICAgYXNhcChmdW5jdGlvbiAoKSB7XG4gICAgICByZXR1cm4gaW52b2tlQ2FsbGJhY2soX3N0YXRlLCBjaGlsZCwgY2FsbGJhY2ssIHBhcmVudC5fcmVzdWx0KTtcbiAgICB9KTtcbiAgfSBlbHNlIHtcbiAgICBzdWJzY3JpYmUocGFyZW50LCBjaGlsZCwgb25GdWxmaWxsbWVudCwgb25SZWplY3Rpb24pO1xuICB9XG5cbiAgcmV0dXJuIGNoaWxkO1xufVxuXG4vKipcbiAgYFByb21pc2UucmVzb2x2ZWAgcmV0dXJucyBhIHByb21pc2UgdGhhdCB3aWxsIGJlY29tZSByZXNvbHZlZCB3aXRoIHRoZVxuICBwYXNzZWQgYHZhbHVlYC4gSXQgaXMgc2hvcnRoYW5kIGZvciB0aGUgZm9sbG93aW5nOlxuXG4gIGBgYGphdmFzY3JpcHRcbiAgbGV0IHByb21pc2UgPSBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXNvbHZlLCByZWplY3Qpe1xuICAgIHJlc29sdmUoMSk7XG4gIH0pO1xuXG4gIHByb21pc2UudGhlbihmdW5jdGlvbih2YWx1ZSl7XG4gICAgLy8gdmFsdWUgPT09IDFcbiAgfSk7XG4gIGBgYFxuXG4gIEluc3RlYWQgb2Ygd3JpdGluZyB0aGUgYWJvdmUsIHlvdXIgY29kZSBub3cgc2ltcGx5IGJlY29tZXMgdGhlIGZvbGxvd2luZzpcblxuICBgYGBqYXZhc2NyaXB0XG4gIGxldCBwcm9taXNlID0gUHJvbWlzZS5yZXNvbHZlKDEpO1xuXG4gIHByb21pc2UudGhlbihmdW5jdGlvbih2YWx1ZSl7XG4gICAgLy8gdmFsdWUgPT09IDFcbiAgfSk7XG4gIGBgYFxuXG4gIEBtZXRob2QgcmVzb2x2ZVxuICBAc3RhdGljXG4gIEBwYXJhbSB7QW55fSB2YWx1ZSB2YWx1ZSB0aGF0IHRoZSByZXR1cm5lZCBwcm9taXNlIHdpbGwgYmUgcmVzb2x2ZWQgd2l0aFxuICBVc2VmdWwgZm9yIHRvb2xpbmcuXG4gIEByZXR1cm4ge1Byb21pc2V9IGEgcHJvbWlzZSB0aGF0IHdpbGwgYmVjb21lIGZ1bGZpbGxlZCB3aXRoIHRoZSBnaXZlblxuICBgdmFsdWVgXG4qL1xuZnVuY3Rpb24gcmVzb2x2ZSQxKG9iamVjdCkge1xuICAvKmpzaGludCB2YWxpZHRoaXM6dHJ1ZSAqL1xuICB2YXIgQ29uc3RydWN0b3IgPSB0aGlzO1xuXG4gIGlmIChvYmplY3QgJiYgdHlwZW9mIG9iamVjdCA9PT0gJ29iamVjdCcgJiYgb2JqZWN0LmNvbnN0cnVjdG9yID09PSBDb25zdHJ1Y3Rvcikge1xuICAgIHJldHVybiBvYmplY3Q7XG4gIH1cblxuICB2YXIgcHJvbWlzZSA9IG5ldyBDb25zdHJ1Y3Rvcihub29wKTtcbiAgcmVzb2x2ZShwcm9taXNlLCBvYmplY3QpO1xuICByZXR1cm4gcHJvbWlzZTtcbn1cblxudmFyIFBST01JU0VfSUQgPSBNYXRoLnJhbmRvbSgpLnRvU3RyaW5nKDM2KS5zdWJzdHJpbmcoMik7XG5cbmZ1bmN0aW9uIG5vb3AoKSB7fVxuXG52YXIgUEVORElORyA9IHZvaWQgMDtcbnZhciBGVUxGSUxMRUQgPSAxO1xudmFyIFJFSkVDVEVEID0gMjtcblxudmFyIFRSWV9DQVRDSF9FUlJPUiA9IHsgZXJyb3I6IG51bGwgfTtcblxuZnVuY3Rpb24gc2VsZkZ1bGZpbGxtZW50KCkge1xuICByZXR1cm4gbmV3IFR5cGVFcnJvcihcIllvdSBjYW5ub3QgcmVzb2x2ZSBhIHByb21pc2Ugd2l0aCBpdHNlbGZcIik7XG59XG5cbmZ1bmN0aW9uIGNhbm5vdFJldHVybk93bigpIHtcbiAgcmV0dXJuIG5ldyBUeXBlRXJyb3IoJ0EgcHJvbWlzZXMgY2FsbGJhY2sgY2Fubm90IHJldHVybiB0aGF0IHNhbWUgcHJvbWlzZS4nKTtcbn1cblxuZnVuY3Rpb24gZ2V0VGhlbihwcm9taXNlKSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIHByb21pc2UudGhlbjtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICBUUllfQ0FUQ0hfRVJST1IuZXJyb3IgPSBlcnJvcjtcbiAgICByZXR1cm4gVFJZX0NBVENIX0VSUk9SO1xuICB9XG59XG5cbmZ1bmN0aW9uIHRyeVRoZW4odGhlbiQkMSwgdmFsdWUsIGZ1bGZpbGxtZW50SGFuZGxlciwgcmVqZWN0aW9uSGFuZGxlcikge1xuICB0cnkge1xuICAgIHRoZW4kJDEuY2FsbCh2YWx1ZSwgZnVsZmlsbG1lbnRIYW5kbGVyLCByZWplY3Rpb25IYW5kbGVyKTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIHJldHVybiBlO1xuICB9XG59XG5cbmZ1bmN0aW9uIGhhbmRsZUZvcmVpZ25UaGVuYWJsZShwcm9taXNlLCB0aGVuYWJsZSwgdGhlbiQkMSkge1xuICBhc2FwKGZ1bmN0aW9uIChwcm9taXNlKSB7XG4gICAgdmFyIHNlYWxlZCA9IGZhbHNlO1xuICAgIHZhciBlcnJvciA9IHRyeVRoZW4odGhlbiQkMSwgdGhlbmFibGUsIGZ1bmN0aW9uICh2YWx1ZSkge1xuICAgICAgaWYgKHNlYWxlZCkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgICBzZWFsZWQgPSB0cnVlO1xuICAgICAgaWYgKHRoZW5hYmxlICE9PSB2YWx1ZSkge1xuICAgICAgICByZXNvbHZlKHByb21pc2UsIHZhbHVlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGZ1bGZpbGwocHJvbWlzZSwgdmFsdWUpO1xuICAgICAgfVxuICAgIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICAgIGlmIChzZWFsZWQpIHtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuICAgICAgc2VhbGVkID0gdHJ1ZTtcblxuICAgICAgcmVqZWN0KHByb21pc2UsIHJlYXNvbik7XG4gICAgfSwgJ1NldHRsZTogJyArIChwcm9taXNlLl9sYWJlbCB8fCAnIHVua25vd24gcHJvbWlzZScpKTtcblxuICAgIGlmICghc2VhbGVkICYmIGVycm9yKSB7XG4gICAgICBzZWFsZWQgPSB0cnVlO1xuICAgICAgcmVqZWN0KHByb21pc2UsIGVycm9yKTtcbiAgICB9XG4gIH0sIHByb21pc2UpO1xufVxuXG5mdW5jdGlvbiBoYW5kbGVPd25UaGVuYWJsZShwcm9taXNlLCB0aGVuYWJsZSkge1xuICBpZiAodGhlbmFibGUuX3N0YXRlID09PSBGVUxGSUxMRUQpIHtcbiAgICBmdWxmaWxsKHByb21pc2UsIHRoZW5hYmxlLl9yZXN1bHQpO1xuICB9IGVsc2UgaWYgKHRoZW5hYmxlLl9zdGF0ZSA9PT0gUkVKRUNURUQpIHtcbiAgICByZWplY3QocHJvbWlzZSwgdGhlbmFibGUuX3Jlc3VsdCk7XG4gIH0gZWxzZSB7XG4gICAgc3Vic2NyaWJlKHRoZW5hYmxlLCB1bmRlZmluZWQsIGZ1bmN0aW9uICh2YWx1ZSkge1xuICAgICAgcmV0dXJuIHJlc29sdmUocHJvbWlzZSwgdmFsdWUpO1xuICAgIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICAgIHJldHVybiByZWplY3QocHJvbWlzZSwgcmVhc29uKTtcbiAgICB9KTtcbiAgfVxufVxuXG5mdW5jdGlvbiBoYW5kbGVNYXliZVRoZW5hYmxlKHByb21pc2UsIG1heWJlVGhlbmFibGUsIHRoZW4kJDEpIHtcbiAgaWYgKG1heWJlVGhlbmFibGUuY29uc3RydWN0b3IgPT09IHByb21pc2UuY29uc3RydWN0b3IgJiYgdGhlbiQkMSA9PT0gdGhlbiAmJiBtYXliZVRoZW5hYmxlLmNvbnN0cnVjdG9yLnJlc29sdmUgPT09IHJlc29sdmUkMSkge1xuICAgIGhhbmRsZU93blRoZW5hYmxlKHByb21pc2UsIG1heWJlVGhlbmFibGUpO1xuICB9IGVsc2Uge1xuICAgIGlmICh0aGVuJCQxID09PSBUUllfQ0FUQ0hfRVJST1IpIHtcbiAgICAgIHJlamVjdChwcm9taXNlLCBUUllfQ0FUQ0hfRVJST1IuZXJyb3IpO1xuICAgICAgVFJZX0NBVENIX0VSUk9SLmVycm9yID0gbnVsbDtcbiAgICB9IGVsc2UgaWYgKHRoZW4kJDEgPT09IHVuZGVmaW5lZCkge1xuICAgICAgZnVsZmlsbChwcm9taXNlLCBtYXliZVRoZW5hYmxlKTtcbiAgICB9IGVsc2UgaWYgKGlzRnVuY3Rpb24odGhlbiQkMSkpIHtcbiAgICAgIGhhbmRsZUZvcmVpZ25UaGVuYWJsZShwcm9taXNlLCBtYXliZVRoZW5hYmxlLCB0aGVuJCQxKTtcbiAgICB9IGVsc2Uge1xuICAgICAgZnVsZmlsbChwcm9taXNlLCBtYXliZVRoZW5hYmxlKTtcbiAgICB9XG4gIH1cbn1cblxuZnVuY3Rpb24gcmVzb2x2ZShwcm9taXNlLCB2YWx1ZSkge1xuICBpZiAocHJvbWlzZSA9PT0gdmFsdWUpIHtcbiAgICByZWplY3QocHJvbWlzZSwgc2VsZkZ1bGZpbGxtZW50KCkpO1xuICB9IGVsc2UgaWYgKG9iamVjdE9yRnVuY3Rpb24odmFsdWUpKSB7XG4gICAgaGFuZGxlTWF5YmVUaGVuYWJsZShwcm9taXNlLCB2YWx1ZSwgZ2V0VGhlbih2YWx1ZSkpO1xuICB9IGVsc2Uge1xuICAgIGZ1bGZpbGwocHJvbWlzZSwgdmFsdWUpO1xuICB9XG59XG5cbmZ1bmN0aW9uIHB1Ymxpc2hSZWplY3Rpb24ocHJvbWlzZSkge1xuICBpZiAocHJvbWlzZS5fb25lcnJvcikge1xuICAgIHByb21pc2UuX29uZXJyb3IocHJvbWlzZS5fcmVzdWx0KTtcbiAgfVxuXG4gIHB1Ymxpc2gocHJvbWlzZSk7XG59XG5cbmZ1bmN0aW9uIGZ1bGZpbGwocHJvbWlzZSwgdmFsdWUpIHtcbiAgaWYgKHByb21pc2UuX3N0YXRlICE9PSBQRU5ESU5HKSB7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgcHJvbWlzZS5fcmVzdWx0ID0gdmFsdWU7XG4gIHByb21pc2UuX3N0YXRlID0gRlVMRklMTEVEO1xuXG4gIGlmIChwcm9taXNlLl9zdWJzY3JpYmVycy5sZW5ndGggIT09IDApIHtcbiAgICBhc2FwKHB1Ymxpc2gsIHByb21pc2UpO1xuICB9XG59XG5cbmZ1bmN0aW9uIHJlamVjdChwcm9taXNlLCByZWFzb24pIHtcbiAgaWYgKHByb21pc2UuX3N0YXRlICE9PSBQRU5ESU5HKSB7XG4gICAgcmV0dXJuO1xuICB9XG4gIHByb21pc2UuX3N0YXRlID0gUkVKRUNURUQ7XG4gIHByb21pc2UuX3Jlc3VsdCA9IHJlYXNvbjtcblxuICBhc2FwKHB1Ymxpc2hSZWplY3Rpb24sIHByb21pc2UpO1xufVxuXG5mdW5jdGlvbiBzdWJzY3JpYmUocGFyZW50LCBjaGlsZCwgb25GdWxmaWxsbWVudCwgb25SZWplY3Rpb24pIHtcbiAgdmFyIF9zdWJzY3JpYmVycyA9IHBhcmVudC5fc3Vic2NyaWJlcnM7XG4gIHZhciBsZW5ndGggPSBfc3Vic2NyaWJlcnMubGVuZ3RoO1xuXG5cbiAgcGFyZW50Ll9vbmVycm9yID0gbnVsbDtcblxuICBfc3Vic2NyaWJlcnNbbGVuZ3RoXSA9IGNoaWxkO1xuICBfc3Vic2NyaWJlcnNbbGVuZ3RoICsgRlVMRklMTEVEXSA9IG9uRnVsZmlsbG1lbnQ7XG4gIF9zdWJzY3JpYmVyc1tsZW5ndGggKyBSRUpFQ1RFRF0gPSBvblJlamVjdGlvbjtcblxuICBpZiAobGVuZ3RoID09PSAwICYmIHBhcmVudC5fc3RhdGUpIHtcbiAgICBhc2FwKHB1Ymxpc2gsIHBhcmVudCk7XG4gIH1cbn1cblxuZnVuY3Rpb24gcHVibGlzaChwcm9taXNlKSB7XG4gIHZhciBzdWJzY3JpYmVycyA9IHByb21pc2UuX3N1YnNjcmliZXJzO1xuICB2YXIgc2V0dGxlZCA9IHByb21pc2UuX3N0YXRlO1xuXG4gIGlmIChzdWJzY3JpYmVycy5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm47XG4gIH1cblxuICB2YXIgY2hpbGQgPSB2b2lkIDAsXG4gICAgICBjYWxsYmFjayA9IHZvaWQgMCxcbiAgICAgIGRldGFpbCA9IHByb21pc2UuX3Jlc3VsdDtcblxuICBmb3IgKHZhciBpID0gMDsgaSA8IHN1YnNjcmliZXJzLmxlbmd0aDsgaSArPSAzKSB7XG4gICAgY2hpbGQgPSBzdWJzY3JpYmVyc1tpXTtcbiAgICBjYWxsYmFjayA9IHN1YnNjcmliZXJzW2kgKyBzZXR0bGVkXTtcblxuICAgIGlmIChjaGlsZCkge1xuICAgICAgaW52b2tlQ2FsbGJhY2soc2V0dGxlZCwgY2hpbGQsIGNhbGxiYWNrLCBkZXRhaWwpO1xuICAgIH0gZWxzZSB7XG4gICAgICBjYWxsYmFjayhkZXRhaWwpO1xuICAgIH1cbiAgfVxuXG4gIHByb21pc2UuX3N1YnNjcmliZXJzLmxlbmd0aCA9IDA7XG59XG5cbmZ1bmN0aW9uIHRyeUNhdGNoKGNhbGxiYWNrLCBkZXRhaWwpIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gY2FsbGJhY2soZGV0YWlsKTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIFRSWV9DQVRDSF9FUlJPUi5lcnJvciA9IGU7XG4gICAgcmV0dXJuIFRSWV9DQVRDSF9FUlJPUjtcbiAgfVxufVxuXG5mdW5jdGlvbiBpbnZva2VDYWxsYmFjayhzZXR0bGVkLCBwcm9taXNlLCBjYWxsYmFjaywgZGV0YWlsKSB7XG4gIHZhciBoYXNDYWxsYmFjayA9IGlzRnVuY3Rpb24oY2FsbGJhY2spLFxuICAgICAgdmFsdWUgPSB2b2lkIDAsXG4gICAgICBlcnJvciA9IHZvaWQgMCxcbiAgICAgIHN1Y2NlZWRlZCA9IHZvaWQgMCxcbiAgICAgIGZhaWxlZCA9IHZvaWQgMDtcblxuICBpZiAoaGFzQ2FsbGJhY2spIHtcbiAgICB2YWx1ZSA9IHRyeUNhdGNoKGNhbGxiYWNrLCBkZXRhaWwpO1xuXG4gICAgaWYgKHZhbHVlID09PSBUUllfQ0FUQ0hfRVJST1IpIHtcbiAgICAgIGZhaWxlZCA9IHRydWU7XG4gICAgICBlcnJvciA9IHZhbHVlLmVycm9yO1xuICAgICAgdmFsdWUuZXJyb3IgPSBudWxsO1xuICAgIH0gZWxzZSB7XG4gICAgICBzdWNjZWVkZWQgPSB0cnVlO1xuICAgIH1cblxuICAgIGlmIChwcm9taXNlID09PSB2YWx1ZSkge1xuICAgICAgcmVqZWN0KHByb21pc2UsIGNhbm5vdFJldHVybk93bigpKTtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gIH0gZWxzZSB7XG4gICAgdmFsdWUgPSBkZXRhaWw7XG4gICAgc3VjY2VlZGVkID0gdHJ1ZTtcbiAgfVxuXG4gIGlmIChwcm9taXNlLl9zdGF0ZSAhPT0gUEVORElORykge1xuICAgIC8vIG5vb3BcbiAgfSBlbHNlIGlmIChoYXNDYWxsYmFjayAmJiBzdWNjZWVkZWQpIHtcbiAgICByZXNvbHZlKHByb21pc2UsIHZhbHVlKTtcbiAgfSBlbHNlIGlmIChmYWlsZWQpIHtcbiAgICByZWplY3QocHJvbWlzZSwgZXJyb3IpO1xuICB9IGVsc2UgaWYgKHNldHRsZWQgPT09IEZVTEZJTExFRCkge1xuICAgIGZ1bGZpbGwocHJvbWlzZSwgdmFsdWUpO1xuICB9IGVsc2UgaWYgKHNldHRsZWQgPT09IFJFSkVDVEVEKSB7XG4gICAgcmVqZWN0KHByb21pc2UsIHZhbHVlKTtcbiAgfVxufVxuXG5mdW5jdGlvbiBpbml0aWFsaXplUHJvbWlzZShwcm9taXNlLCByZXNvbHZlcikge1xuICB0cnkge1xuICAgIHJlc29sdmVyKGZ1bmN0aW9uIHJlc29sdmVQcm9taXNlKHZhbHVlKSB7XG4gICAgICByZXNvbHZlKHByb21pc2UsIHZhbHVlKTtcbiAgICB9LCBmdW5jdGlvbiByZWplY3RQcm9taXNlKHJlYXNvbikge1xuICAgICAgcmVqZWN0KHByb21pc2UsIHJlYXNvbik7XG4gICAgfSk7XG4gIH0gY2F0Y2ggKGUpIHtcbiAgICByZWplY3QocHJvbWlzZSwgZSk7XG4gIH1cbn1cblxudmFyIGlkID0gMDtcbmZ1bmN0aW9uIG5leHRJZCgpIHtcbiAgcmV0dXJuIGlkKys7XG59XG5cbmZ1bmN0aW9uIG1ha2VQcm9taXNlKHByb21pc2UpIHtcbiAgcHJvbWlzZVtQUk9NSVNFX0lEXSA9IGlkKys7XG4gIHByb21pc2UuX3N0YXRlID0gdW5kZWZpbmVkO1xuICBwcm9taXNlLl9yZXN1bHQgPSB1bmRlZmluZWQ7XG4gIHByb21pc2UuX3N1YnNjcmliZXJzID0gW107XG59XG5cbmZ1bmN0aW9uIHZhbGlkYXRpb25FcnJvcigpIHtcbiAgcmV0dXJuIG5ldyBFcnJvcignQXJyYXkgTWV0aG9kcyBtdXN0IGJlIHByb3ZpZGVkIGFuIEFycmF5Jyk7XG59XG5cbnZhciBFbnVtZXJhdG9yID0gZnVuY3Rpb24gKCkge1xuICBmdW5jdGlvbiBFbnVtZXJhdG9yKENvbnN0cnVjdG9yLCBpbnB1dCkge1xuICAgIHRoaXMuX2luc3RhbmNlQ29uc3RydWN0b3IgPSBDb25zdHJ1Y3RvcjtcbiAgICB0aGlzLnByb21pc2UgPSBuZXcgQ29uc3RydWN0b3Iobm9vcCk7XG5cbiAgICBpZiAoIXRoaXMucHJvbWlzZVtQUk9NSVNFX0lEXSkge1xuICAgICAgbWFrZVByb21pc2UodGhpcy5wcm9taXNlKTtcbiAgICB9XG5cbiAgICBpZiAoaXNBcnJheShpbnB1dCkpIHtcbiAgICAgIHRoaXMubGVuZ3RoID0gaW5wdXQubGVuZ3RoO1xuICAgICAgdGhpcy5fcmVtYWluaW5nID0gaW5wdXQubGVuZ3RoO1xuXG4gICAgICB0aGlzLl9yZXN1bHQgPSBuZXcgQXJyYXkodGhpcy5sZW5ndGgpO1xuXG4gICAgICBpZiAodGhpcy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgZnVsZmlsbCh0aGlzLnByb21pc2UsIHRoaXMuX3Jlc3VsdCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLmxlbmd0aCA9IHRoaXMubGVuZ3RoIHx8IDA7XG4gICAgICAgIHRoaXMuX2VudW1lcmF0ZShpbnB1dCk7XG4gICAgICAgIGlmICh0aGlzLl9yZW1haW5pbmcgPT09IDApIHtcbiAgICAgICAgICBmdWxmaWxsKHRoaXMucHJvbWlzZSwgdGhpcy5fcmVzdWx0KTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICByZWplY3QodGhpcy5wcm9taXNlLCB2YWxpZGF0aW9uRXJyb3IoKSk7XG4gICAgfVxuICB9XG5cbiAgRW51bWVyYXRvci5wcm90b3R5cGUuX2VudW1lcmF0ZSA9IGZ1bmN0aW9uIF9lbnVtZXJhdGUoaW5wdXQpIHtcbiAgICBmb3IgKHZhciBpID0gMDsgdGhpcy5fc3RhdGUgPT09IFBFTkRJTkcgJiYgaSA8IGlucHV0Lmxlbmd0aDsgaSsrKSB7XG4gICAgICB0aGlzLl9lYWNoRW50cnkoaW5wdXRbaV0sIGkpO1xuICAgIH1cbiAgfTtcblxuICBFbnVtZXJhdG9yLnByb3RvdHlwZS5fZWFjaEVudHJ5ID0gZnVuY3Rpb24gX2VhY2hFbnRyeShlbnRyeSwgaSkge1xuICAgIHZhciBjID0gdGhpcy5faW5zdGFuY2VDb25zdHJ1Y3RvcjtcbiAgICB2YXIgcmVzb2x2ZSQkMSA9IGMucmVzb2x2ZTtcblxuXG4gICAgaWYgKHJlc29sdmUkJDEgPT09IHJlc29sdmUkMSkge1xuICAgICAgdmFyIF90aGVuID0gZ2V0VGhlbihlbnRyeSk7XG5cbiAgICAgIGlmIChfdGhlbiA9PT0gdGhlbiAmJiBlbnRyeS5fc3RhdGUgIT09IFBFTkRJTkcpIHtcbiAgICAgICAgdGhpcy5fc2V0dGxlZEF0KGVudHJ5Ll9zdGF0ZSwgaSwgZW50cnkuX3Jlc3VsdCk7XG4gICAgICB9IGVsc2UgaWYgKHR5cGVvZiBfdGhlbiAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB0aGlzLl9yZW1haW5pbmctLTtcbiAgICAgICAgdGhpcy5fcmVzdWx0W2ldID0gZW50cnk7XG4gICAgICB9IGVsc2UgaWYgKGMgPT09IFByb21pc2UkMSkge1xuICAgICAgICB2YXIgcHJvbWlzZSA9IG5ldyBjKG5vb3ApO1xuICAgICAgICBoYW5kbGVNYXliZVRoZW5hYmxlKHByb21pc2UsIGVudHJ5LCBfdGhlbik7XG4gICAgICAgIHRoaXMuX3dpbGxTZXR0bGVBdChwcm9taXNlLCBpKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX3dpbGxTZXR0bGVBdChuZXcgYyhmdW5jdGlvbiAocmVzb2x2ZSQkMSkge1xuICAgICAgICAgIHJldHVybiByZXNvbHZlJCQxKGVudHJ5KTtcbiAgICAgICAgfSksIGkpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLl93aWxsU2V0dGxlQXQocmVzb2x2ZSQkMShlbnRyeSksIGkpO1xuICAgIH1cbiAgfTtcblxuICBFbnVtZXJhdG9yLnByb3RvdHlwZS5fc2V0dGxlZEF0ID0gZnVuY3Rpb24gX3NldHRsZWRBdChzdGF0ZSwgaSwgdmFsdWUpIHtcbiAgICB2YXIgcHJvbWlzZSA9IHRoaXMucHJvbWlzZTtcblxuXG4gICAgaWYgKHByb21pc2UuX3N0YXRlID09PSBQRU5ESU5HKSB7XG4gICAgICB0aGlzLl9yZW1haW5pbmctLTtcblxuICAgICAgaWYgKHN0YXRlID09PSBSRUpFQ1RFRCkge1xuICAgICAgICByZWplY3QocHJvbWlzZSwgdmFsdWUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5fcmVzdWx0W2ldID0gdmFsdWU7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHRoaXMuX3JlbWFpbmluZyA9PT0gMCkge1xuICAgICAgZnVsZmlsbChwcm9taXNlLCB0aGlzLl9yZXN1bHQpO1xuICAgIH1cbiAgfTtcblxuICBFbnVtZXJhdG9yLnByb3RvdHlwZS5fd2lsbFNldHRsZUF0ID0gZnVuY3Rpb24gX3dpbGxTZXR0bGVBdChwcm9taXNlLCBpKSB7XG4gICAgdmFyIGVudW1lcmF0b3IgPSB0aGlzO1xuXG4gICAgc3Vic2NyaWJlKHByb21pc2UsIHVuZGVmaW5lZCwgZnVuY3Rpb24gKHZhbHVlKSB7XG4gICAgICByZXR1cm4gZW51bWVyYXRvci5fc2V0dGxlZEF0KEZVTEZJTExFRCwgaSwgdmFsdWUpO1xuICAgIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICAgIHJldHVybiBlbnVtZXJhdG9yLl9zZXR0bGVkQXQoUkVKRUNURUQsIGksIHJlYXNvbik7XG4gICAgfSk7XG4gIH07XG5cbiAgcmV0dXJuIEVudW1lcmF0b3I7XG59KCk7XG5cbi8qKlxuICBgUHJvbWlzZS5hbGxgIGFjY2VwdHMgYW4gYXJyYXkgb2YgcHJvbWlzZXMsIGFuZCByZXR1cm5zIGEgbmV3IHByb21pc2Ugd2hpY2hcbiAgaXMgZnVsZmlsbGVkIHdpdGggYW4gYXJyYXkgb2YgZnVsZmlsbG1lbnQgdmFsdWVzIGZvciB0aGUgcGFzc2VkIHByb21pc2VzLCBvclxuICByZWplY3RlZCB3aXRoIHRoZSByZWFzb24gb2YgdGhlIGZpcnN0IHBhc3NlZCBwcm9taXNlIHRvIGJlIHJlamVjdGVkLiBJdCBjYXN0cyBhbGxcbiAgZWxlbWVudHMgb2YgdGhlIHBhc3NlZCBpdGVyYWJsZSB0byBwcm9taXNlcyBhcyBpdCBydW5zIHRoaXMgYWxnb3JpdGhtLlxuXG4gIEV4YW1wbGU6XG5cbiAgYGBgamF2YXNjcmlwdFxuICBsZXQgcHJvbWlzZTEgPSByZXNvbHZlKDEpO1xuICBsZXQgcHJvbWlzZTIgPSByZXNvbHZlKDIpO1xuICBsZXQgcHJvbWlzZTMgPSByZXNvbHZlKDMpO1xuICBsZXQgcHJvbWlzZXMgPSBbIHByb21pc2UxLCBwcm9taXNlMiwgcHJvbWlzZTMgXTtcblxuICBQcm9taXNlLmFsbChwcm9taXNlcykudGhlbihmdW5jdGlvbihhcnJheSl7XG4gICAgLy8gVGhlIGFycmF5IGhlcmUgd291bGQgYmUgWyAxLCAyLCAzIF07XG4gIH0pO1xuICBgYGBcblxuICBJZiBhbnkgb2YgdGhlIGBwcm9taXNlc2AgZ2l2ZW4gdG8gYGFsbGAgYXJlIHJlamVjdGVkLCB0aGUgZmlyc3QgcHJvbWlzZVxuICB0aGF0IGlzIHJlamVjdGVkIHdpbGwgYmUgZ2l2ZW4gYXMgYW4gYXJndW1lbnQgdG8gdGhlIHJldHVybmVkIHByb21pc2VzJ3NcbiAgcmVqZWN0aW9uIGhhbmRsZXIuIEZvciBleGFtcGxlOlxuXG4gIEV4YW1wbGU6XG5cbiAgYGBgamF2YXNjcmlwdFxuICBsZXQgcHJvbWlzZTEgPSByZXNvbHZlKDEpO1xuICBsZXQgcHJvbWlzZTIgPSByZWplY3QobmV3IEVycm9yKFwiMlwiKSk7XG4gIGxldCBwcm9taXNlMyA9IHJlamVjdChuZXcgRXJyb3IoXCIzXCIpKTtcbiAgbGV0IHByb21pc2VzID0gWyBwcm9taXNlMSwgcHJvbWlzZTIsIHByb21pc2UzIF07XG5cbiAgUHJvbWlzZS5hbGwocHJvbWlzZXMpLnRoZW4oZnVuY3Rpb24oYXJyYXkpe1xuICAgIC8vIENvZGUgaGVyZSBuZXZlciBydW5zIGJlY2F1c2UgdGhlcmUgYXJlIHJlamVjdGVkIHByb21pc2VzIVxuICB9LCBmdW5jdGlvbihlcnJvcikge1xuICAgIC8vIGVycm9yLm1lc3NhZ2UgPT09IFwiMlwiXG4gIH0pO1xuICBgYGBcblxuICBAbWV0aG9kIGFsbFxuICBAc3RhdGljXG4gIEBwYXJhbSB7QXJyYXl9IGVudHJpZXMgYXJyYXkgb2YgcHJvbWlzZXNcbiAgQHBhcmFtIHtTdHJpbmd9IGxhYmVsIG9wdGlvbmFsIHN0cmluZyBmb3IgbGFiZWxpbmcgdGhlIHByb21pc2UuXG4gIFVzZWZ1bCBmb3IgdG9vbGluZy5cbiAgQHJldHVybiB7UHJvbWlzZX0gcHJvbWlzZSB0aGF0IGlzIGZ1bGZpbGxlZCB3aGVuIGFsbCBgcHJvbWlzZXNgIGhhdmUgYmVlblxuICBmdWxmaWxsZWQsIG9yIHJlamVjdGVkIGlmIGFueSBvZiB0aGVtIGJlY29tZSByZWplY3RlZC5cbiAgQHN0YXRpY1xuKi9cbmZ1bmN0aW9uIGFsbChlbnRyaWVzKSB7XG4gIHJldHVybiBuZXcgRW51bWVyYXRvcih0aGlzLCBlbnRyaWVzKS5wcm9taXNlO1xufVxuXG4vKipcbiAgYFByb21pc2UucmFjZWAgcmV0dXJucyBhIG5ldyBwcm9taXNlIHdoaWNoIGlzIHNldHRsZWQgaW4gdGhlIHNhbWUgd2F5IGFzIHRoZVxuICBmaXJzdCBwYXNzZWQgcHJvbWlzZSB0byBzZXR0bGUuXG5cbiAgRXhhbXBsZTpcblxuICBgYGBqYXZhc2NyaXB0XG4gIGxldCBwcm9taXNlMSA9IG5ldyBQcm9taXNlKGZ1bmN0aW9uKHJlc29sdmUsIHJlamVjdCl7XG4gICAgc2V0VGltZW91dChmdW5jdGlvbigpe1xuICAgICAgcmVzb2x2ZSgncHJvbWlzZSAxJyk7XG4gICAgfSwgMjAwKTtcbiAgfSk7XG5cbiAgbGV0IHByb21pc2UyID0gbmV3IFByb21pc2UoZnVuY3Rpb24ocmVzb2x2ZSwgcmVqZWN0KXtcbiAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uKCl7XG4gICAgICByZXNvbHZlKCdwcm9taXNlIDInKTtcbiAgICB9LCAxMDApO1xuICB9KTtcblxuICBQcm9taXNlLnJhY2UoW3Byb21pc2UxLCBwcm9taXNlMl0pLnRoZW4oZnVuY3Rpb24ocmVzdWx0KXtcbiAgICAvLyByZXN1bHQgPT09ICdwcm9taXNlIDInIGJlY2F1c2UgaXQgd2FzIHJlc29sdmVkIGJlZm9yZSBwcm9taXNlMVxuICAgIC8vIHdhcyByZXNvbHZlZC5cbiAgfSk7XG4gIGBgYFxuXG4gIGBQcm9taXNlLnJhY2VgIGlzIGRldGVybWluaXN0aWMgaW4gdGhhdCBvbmx5IHRoZSBzdGF0ZSBvZiB0aGUgZmlyc3RcbiAgc2V0dGxlZCBwcm9taXNlIG1hdHRlcnMuIEZvciBleGFtcGxlLCBldmVuIGlmIG90aGVyIHByb21pc2VzIGdpdmVuIHRvIHRoZVxuICBgcHJvbWlzZXNgIGFycmF5IGFyZ3VtZW50IGFyZSByZXNvbHZlZCwgYnV0IHRoZSBmaXJzdCBzZXR0bGVkIHByb21pc2UgaGFzXG4gIGJlY29tZSByZWplY3RlZCBiZWZvcmUgdGhlIG90aGVyIHByb21pc2VzIGJlY2FtZSBmdWxmaWxsZWQsIHRoZSByZXR1cm5lZFxuICBwcm9taXNlIHdpbGwgYmVjb21lIHJlamVjdGVkOlxuXG4gIGBgYGphdmFzY3JpcHRcbiAgbGV0IHByb21pc2UxID0gbmV3IFByb21pc2UoZnVuY3Rpb24ocmVzb2x2ZSwgcmVqZWN0KXtcbiAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uKCl7XG4gICAgICByZXNvbHZlKCdwcm9taXNlIDEnKTtcbiAgICB9LCAyMDApO1xuICB9KTtcblxuICBsZXQgcHJvbWlzZTIgPSBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXNvbHZlLCByZWplY3Qpe1xuICAgIHNldFRpbWVvdXQoZnVuY3Rpb24oKXtcbiAgICAgIHJlamVjdChuZXcgRXJyb3IoJ3Byb21pc2UgMicpKTtcbiAgICB9LCAxMDApO1xuICB9KTtcblxuICBQcm9taXNlLnJhY2UoW3Byb21pc2UxLCBwcm9taXNlMl0pLnRoZW4oZnVuY3Rpb24ocmVzdWx0KXtcbiAgICAvLyBDb2RlIGhlcmUgbmV2ZXIgcnVuc1xuICB9LCBmdW5jdGlvbihyZWFzb24pe1xuICAgIC8vIHJlYXNvbi5tZXNzYWdlID09PSAncHJvbWlzZSAyJyBiZWNhdXNlIHByb21pc2UgMiBiZWNhbWUgcmVqZWN0ZWQgYmVmb3JlXG4gICAgLy8gcHJvbWlzZSAxIGJlY2FtZSBmdWxmaWxsZWRcbiAgfSk7XG4gIGBgYFxuXG4gIEFuIGV4YW1wbGUgcmVhbC13b3JsZCB1c2UgY2FzZSBpcyBpbXBsZW1lbnRpbmcgdGltZW91dHM6XG5cbiAgYGBgamF2YXNjcmlwdFxuICBQcm9taXNlLnJhY2UoW2FqYXgoJ2Zvby5qc29uJyksIHRpbWVvdXQoNTAwMCldKVxuICBgYGBcblxuICBAbWV0aG9kIHJhY2VcbiAgQHN0YXRpY1xuICBAcGFyYW0ge0FycmF5fSBwcm9taXNlcyBhcnJheSBvZiBwcm9taXNlcyB0byBvYnNlcnZlXG4gIFVzZWZ1bCBmb3IgdG9vbGluZy5cbiAgQHJldHVybiB7UHJvbWlzZX0gYSBwcm9taXNlIHdoaWNoIHNldHRsZXMgaW4gdGhlIHNhbWUgd2F5IGFzIHRoZSBmaXJzdCBwYXNzZWRcbiAgcHJvbWlzZSB0byBzZXR0bGUuXG4qL1xuZnVuY3Rpb24gcmFjZShlbnRyaWVzKSB7XG4gIC8qanNoaW50IHZhbGlkdGhpczp0cnVlICovXG4gIHZhciBDb25zdHJ1Y3RvciA9IHRoaXM7XG5cbiAgaWYgKCFpc0FycmF5KGVudHJpZXMpKSB7XG4gICAgcmV0dXJuIG5ldyBDb25zdHJ1Y3RvcihmdW5jdGlvbiAoXywgcmVqZWN0KSB7XG4gICAgICByZXR1cm4gcmVqZWN0KG5ldyBUeXBlRXJyb3IoJ1lvdSBtdXN0IHBhc3MgYW4gYXJyYXkgdG8gcmFjZS4nKSk7XG4gICAgfSk7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIG5ldyBDb25zdHJ1Y3RvcihmdW5jdGlvbiAocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICB2YXIgbGVuZ3RoID0gZW50cmllcy5sZW5ndGg7XG4gICAgICBmb3IgKHZhciBpID0gMDsgaSA8IGxlbmd0aDsgaSsrKSB7XG4gICAgICAgIENvbnN0cnVjdG9yLnJlc29sdmUoZW50cmllc1tpXSkudGhlbihyZXNvbHZlLCByZWplY3QpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG59XG5cbi8qKlxuICBgUHJvbWlzZS5yZWplY3RgIHJldHVybnMgYSBwcm9taXNlIHJlamVjdGVkIHdpdGggdGhlIHBhc3NlZCBgcmVhc29uYC5cbiAgSXQgaXMgc2hvcnRoYW5kIGZvciB0aGUgZm9sbG93aW5nOlxuXG4gIGBgYGphdmFzY3JpcHRcbiAgbGV0IHByb21pc2UgPSBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXNvbHZlLCByZWplY3Qpe1xuICAgIHJlamVjdChuZXcgRXJyb3IoJ1dIT09QUycpKTtcbiAgfSk7XG5cbiAgcHJvbWlzZS50aGVuKGZ1bmN0aW9uKHZhbHVlKXtcbiAgICAvLyBDb2RlIGhlcmUgZG9lc24ndCBydW4gYmVjYXVzZSB0aGUgcHJvbWlzZSBpcyByZWplY3RlZCFcbiAgfSwgZnVuY3Rpb24ocmVhc29uKXtcbiAgICAvLyByZWFzb24ubWVzc2FnZSA9PT0gJ1dIT09QUydcbiAgfSk7XG4gIGBgYFxuXG4gIEluc3RlYWQgb2Ygd3JpdGluZyB0aGUgYWJvdmUsIHlvdXIgY29kZSBub3cgc2ltcGx5IGJlY29tZXMgdGhlIGZvbGxvd2luZzpcblxuICBgYGBqYXZhc2NyaXB0XG4gIGxldCBwcm9taXNlID0gUHJvbWlzZS5yZWplY3QobmV3IEVycm9yKCdXSE9PUFMnKSk7XG5cbiAgcHJvbWlzZS50aGVuKGZ1bmN0aW9uKHZhbHVlKXtcbiAgICAvLyBDb2RlIGhlcmUgZG9lc24ndCBydW4gYmVjYXVzZSB0aGUgcHJvbWlzZSBpcyByZWplY3RlZCFcbiAgfSwgZnVuY3Rpb24ocmVhc29uKXtcbiAgICAvLyByZWFzb24ubWVzc2FnZSA9PT0gJ1dIT09QUydcbiAgfSk7XG4gIGBgYFxuXG4gIEBtZXRob2QgcmVqZWN0XG4gIEBzdGF0aWNcbiAgQHBhcmFtIHtBbnl9IHJlYXNvbiB2YWx1ZSB0aGF0IHRoZSByZXR1cm5lZCBwcm9taXNlIHdpbGwgYmUgcmVqZWN0ZWQgd2l0aC5cbiAgVXNlZnVsIGZvciB0b29saW5nLlxuICBAcmV0dXJuIHtQcm9taXNlfSBhIHByb21pc2UgcmVqZWN0ZWQgd2l0aCB0aGUgZ2l2ZW4gYHJlYXNvbmAuXG4qL1xuZnVuY3Rpb24gcmVqZWN0JDEocmVhc29uKSB7XG4gIC8qanNoaW50IHZhbGlkdGhpczp0cnVlICovXG4gIHZhciBDb25zdHJ1Y3RvciA9IHRoaXM7XG4gIHZhciBwcm9taXNlID0gbmV3IENvbnN0cnVjdG9yKG5vb3ApO1xuICByZWplY3QocHJvbWlzZSwgcmVhc29uKTtcbiAgcmV0dXJuIHByb21pc2U7XG59XG5cbmZ1bmN0aW9uIG5lZWRzUmVzb2x2ZXIoKSB7XG4gIHRocm93IG5ldyBUeXBlRXJyb3IoJ1lvdSBtdXN0IHBhc3MgYSByZXNvbHZlciBmdW5jdGlvbiBhcyB0aGUgZmlyc3QgYXJndW1lbnQgdG8gdGhlIHByb21pc2UgY29uc3RydWN0b3InKTtcbn1cblxuZnVuY3Rpb24gbmVlZHNOZXcoKSB7XG4gIHRocm93IG5ldyBUeXBlRXJyb3IoXCJGYWlsZWQgdG8gY29uc3RydWN0ICdQcm9taXNlJzogUGxlYXNlIHVzZSB0aGUgJ25ldycgb3BlcmF0b3IsIHRoaXMgb2JqZWN0IGNvbnN0cnVjdG9yIGNhbm5vdCBiZSBjYWxsZWQgYXMgYSBmdW5jdGlvbi5cIik7XG59XG5cbi8qKlxuICBQcm9taXNlIG9iamVjdHMgcmVwcmVzZW50IHRoZSBldmVudHVhbCByZXN1bHQgb2YgYW4gYXN5bmNocm9ub3VzIG9wZXJhdGlvbi4gVGhlXG4gIHByaW1hcnkgd2F5IG9mIGludGVyYWN0aW5nIHdpdGggYSBwcm9taXNlIGlzIHRocm91Z2ggaXRzIGB0aGVuYCBtZXRob2QsIHdoaWNoXG4gIHJlZ2lzdGVycyBjYWxsYmFja3MgdG8gcmVjZWl2ZSBlaXRoZXIgYSBwcm9taXNlJ3MgZXZlbnR1YWwgdmFsdWUgb3IgdGhlIHJlYXNvblxuICB3aHkgdGhlIHByb21pc2UgY2Fubm90IGJlIGZ1bGZpbGxlZC5cblxuICBUZXJtaW5vbG9neVxuICAtLS0tLS0tLS0tLVxuXG4gIC0gYHByb21pc2VgIGlzIGFuIG9iamVjdCBvciBmdW5jdGlvbiB3aXRoIGEgYHRoZW5gIG1ldGhvZCB3aG9zZSBiZWhhdmlvciBjb25mb3JtcyB0byB0aGlzIHNwZWNpZmljYXRpb24uXG4gIC0gYHRoZW5hYmxlYCBpcyBhbiBvYmplY3Qgb3IgZnVuY3Rpb24gdGhhdCBkZWZpbmVzIGEgYHRoZW5gIG1ldGhvZC5cbiAgLSBgdmFsdWVgIGlzIGFueSBsZWdhbCBKYXZhU2NyaXB0IHZhbHVlIChpbmNsdWRpbmcgdW5kZWZpbmVkLCBhIHRoZW5hYmxlLCBvciBhIHByb21pc2UpLlxuICAtIGBleGNlcHRpb25gIGlzIGEgdmFsdWUgdGhhdCBpcyB0aHJvd24gdXNpbmcgdGhlIHRocm93IHN0YXRlbWVudC5cbiAgLSBgcmVhc29uYCBpcyBhIHZhbHVlIHRoYXQgaW5kaWNhdGVzIHdoeSBhIHByb21pc2Ugd2FzIHJlamVjdGVkLlxuICAtIGBzZXR0bGVkYCB0aGUgZmluYWwgcmVzdGluZyBzdGF0ZSBvZiBhIHByb21pc2UsIGZ1bGZpbGxlZCBvciByZWplY3RlZC5cblxuICBBIHByb21pc2UgY2FuIGJlIGluIG9uZSBvZiB0aHJlZSBzdGF0ZXM6IHBlbmRpbmcsIGZ1bGZpbGxlZCwgb3IgcmVqZWN0ZWQuXG5cbiAgUHJvbWlzZXMgdGhhdCBhcmUgZnVsZmlsbGVkIGhhdmUgYSBmdWxmaWxsbWVudCB2YWx1ZSBhbmQgYXJlIGluIHRoZSBmdWxmaWxsZWRcbiAgc3RhdGUuICBQcm9taXNlcyB0aGF0IGFyZSByZWplY3RlZCBoYXZlIGEgcmVqZWN0aW9uIHJlYXNvbiBhbmQgYXJlIGluIHRoZVxuICByZWplY3RlZCBzdGF0ZS4gIEEgZnVsZmlsbG1lbnQgdmFsdWUgaXMgbmV2ZXIgYSB0aGVuYWJsZS5cblxuICBQcm9taXNlcyBjYW4gYWxzbyBiZSBzYWlkIHRvICpyZXNvbHZlKiBhIHZhbHVlLiAgSWYgdGhpcyB2YWx1ZSBpcyBhbHNvIGFcbiAgcHJvbWlzZSwgdGhlbiB0aGUgb3JpZ2luYWwgcHJvbWlzZSdzIHNldHRsZWQgc3RhdGUgd2lsbCBtYXRjaCB0aGUgdmFsdWUnc1xuICBzZXR0bGVkIHN0YXRlLiAgU28gYSBwcm9taXNlIHRoYXQgKnJlc29sdmVzKiBhIHByb21pc2UgdGhhdCByZWplY3RzIHdpbGxcbiAgaXRzZWxmIHJlamVjdCwgYW5kIGEgcHJvbWlzZSB0aGF0ICpyZXNvbHZlcyogYSBwcm9taXNlIHRoYXQgZnVsZmlsbHMgd2lsbFxuICBpdHNlbGYgZnVsZmlsbC5cblxuXG4gIEJhc2ljIFVzYWdlOlxuICAtLS0tLS0tLS0tLS1cblxuICBgYGBqc1xuICBsZXQgcHJvbWlzZSA9IG5ldyBQcm9taXNlKGZ1bmN0aW9uKHJlc29sdmUsIHJlamVjdCkge1xuICAgIC8vIG9uIHN1Y2Nlc3NcbiAgICByZXNvbHZlKHZhbHVlKTtcblxuICAgIC8vIG9uIGZhaWx1cmVcbiAgICByZWplY3QocmVhc29uKTtcbiAgfSk7XG5cbiAgcHJvbWlzZS50aGVuKGZ1bmN0aW9uKHZhbHVlKSB7XG4gICAgLy8gb24gZnVsZmlsbG1lbnRcbiAgfSwgZnVuY3Rpb24ocmVhc29uKSB7XG4gICAgLy8gb24gcmVqZWN0aW9uXG4gIH0pO1xuICBgYGBcblxuICBBZHZhbmNlZCBVc2FnZTpcbiAgLS0tLS0tLS0tLS0tLS0tXG5cbiAgUHJvbWlzZXMgc2hpbmUgd2hlbiBhYnN0cmFjdGluZyBhd2F5IGFzeW5jaHJvbm91cyBpbnRlcmFjdGlvbnMgc3VjaCBhc1xuICBgWE1MSHR0cFJlcXVlc3Rgcy5cblxuICBgYGBqc1xuICBmdW5jdGlvbiBnZXRKU09OKHVybCkge1xuICAgIHJldHVybiBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXNvbHZlLCByZWplY3Qpe1xuICAgICAgbGV0IHhociA9IG5ldyBYTUxIdHRwUmVxdWVzdCgpO1xuXG4gICAgICB4aHIub3BlbignR0VUJywgdXJsKTtcbiAgICAgIHhoci5vbnJlYWR5c3RhdGVjaGFuZ2UgPSBoYW5kbGVyO1xuICAgICAgeGhyLnJlc3BvbnNlVHlwZSA9ICdqc29uJztcbiAgICAgIHhoci5zZXRSZXF1ZXN0SGVhZGVyKCdBY2NlcHQnLCAnYXBwbGljYXRpb24vanNvbicpO1xuICAgICAgeGhyLnNlbmQoKTtcblxuICAgICAgZnVuY3Rpb24gaGFuZGxlcigpIHtcbiAgICAgICAgaWYgKHRoaXMucmVhZHlTdGF0ZSA9PT0gdGhpcy5ET05FKSB7XG4gICAgICAgICAgaWYgKHRoaXMuc3RhdHVzID09PSAyMDApIHtcbiAgICAgICAgICAgIHJlc29sdmUodGhpcy5yZXNwb25zZSk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJlamVjdChuZXcgRXJyb3IoJ2dldEpTT046IGAnICsgdXJsICsgJ2AgZmFpbGVkIHdpdGggc3RhdHVzOiBbJyArIHRoaXMuc3RhdHVzICsgJ10nKSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgIH0pO1xuICB9XG5cbiAgZ2V0SlNPTignL3Bvc3RzLmpzb24nKS50aGVuKGZ1bmN0aW9uKGpzb24pIHtcbiAgICAvLyBvbiBmdWxmaWxsbWVudFxuICB9LCBmdW5jdGlvbihyZWFzb24pIHtcbiAgICAvLyBvbiByZWplY3Rpb25cbiAgfSk7XG4gIGBgYFxuXG4gIFVubGlrZSBjYWxsYmFja3MsIHByb21pc2VzIGFyZSBncmVhdCBjb21wb3NhYmxlIHByaW1pdGl2ZXMuXG5cbiAgYGBganNcbiAgUHJvbWlzZS5hbGwoW1xuICAgIGdldEpTT04oJy9wb3N0cycpLFxuICAgIGdldEpTT04oJy9jb21tZW50cycpXG4gIF0pLnRoZW4oZnVuY3Rpb24odmFsdWVzKXtcbiAgICB2YWx1ZXNbMF0gLy8gPT4gcG9zdHNKU09OXG4gICAgdmFsdWVzWzFdIC8vID0+IGNvbW1lbnRzSlNPTlxuXG4gICAgcmV0dXJuIHZhbHVlcztcbiAgfSk7XG4gIGBgYFxuXG4gIEBjbGFzcyBQcm9taXNlXG4gIEBwYXJhbSB7RnVuY3Rpb259IHJlc29sdmVyXG4gIFVzZWZ1bCBmb3IgdG9vbGluZy5cbiAgQGNvbnN0cnVjdG9yXG4qL1xuXG52YXIgUHJvbWlzZSQxID0gZnVuY3Rpb24gKCkge1xuICBmdW5jdGlvbiBQcm9taXNlKHJlc29sdmVyKSB7XG4gICAgdGhpc1tQUk9NSVNFX0lEXSA9IG5leHRJZCgpO1xuICAgIHRoaXMuX3Jlc3VsdCA9IHRoaXMuX3N0YXRlID0gdW5kZWZpbmVkO1xuICAgIHRoaXMuX3N1YnNjcmliZXJzID0gW107XG5cbiAgICBpZiAobm9vcCAhPT0gcmVzb2x2ZXIpIHtcbiAgICAgIHR5cGVvZiByZXNvbHZlciAhPT0gJ2Z1bmN0aW9uJyAmJiBuZWVkc1Jlc29sdmVyKCk7XG4gICAgICB0aGlzIGluc3RhbmNlb2YgUHJvbWlzZSA/IGluaXRpYWxpemVQcm9taXNlKHRoaXMsIHJlc29sdmVyKSA6IG5lZWRzTmV3KCk7XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gIFRoZSBwcmltYXJ5IHdheSBvZiBpbnRlcmFjdGluZyB3aXRoIGEgcHJvbWlzZSBpcyB0aHJvdWdoIGl0cyBgdGhlbmAgbWV0aG9kLFxuICB3aGljaCByZWdpc3RlcnMgY2FsbGJhY2tzIHRvIHJlY2VpdmUgZWl0aGVyIGEgcHJvbWlzZSdzIGV2ZW50dWFsIHZhbHVlIG9yIHRoZVxuICByZWFzb24gd2h5IHRoZSBwcm9taXNlIGNhbm5vdCBiZSBmdWxmaWxsZWQuXG4gICBgYGBqc1xuICBmaW5kVXNlcigpLnRoZW4oZnVuY3Rpb24odXNlcil7XG4gICAgLy8gdXNlciBpcyBhdmFpbGFibGVcbiAgfSwgZnVuY3Rpb24ocmVhc29uKXtcbiAgICAvLyB1c2VyIGlzIHVuYXZhaWxhYmxlLCBhbmQgeW91IGFyZSBnaXZlbiB0aGUgcmVhc29uIHdoeVxuICB9KTtcbiAgYGBgXG4gICBDaGFpbmluZ1xuICAtLS0tLS0tLVxuICAgVGhlIHJldHVybiB2YWx1ZSBvZiBgdGhlbmAgaXMgaXRzZWxmIGEgcHJvbWlzZS4gIFRoaXMgc2Vjb25kLCAnZG93bnN0cmVhbSdcbiAgcHJvbWlzZSBpcyByZXNvbHZlZCB3aXRoIHRoZSByZXR1cm4gdmFsdWUgb2YgdGhlIGZpcnN0IHByb21pc2UncyBmdWxmaWxsbWVudFxuICBvciByZWplY3Rpb24gaGFuZGxlciwgb3IgcmVqZWN0ZWQgaWYgdGhlIGhhbmRsZXIgdGhyb3dzIGFuIGV4Y2VwdGlvbi5cbiAgIGBgYGpzXG4gIGZpbmRVc2VyKCkudGhlbihmdW5jdGlvbiAodXNlcikge1xuICAgIHJldHVybiB1c2VyLm5hbWU7XG4gIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICByZXR1cm4gJ2RlZmF1bHQgbmFtZSc7XG4gIH0pLnRoZW4oZnVuY3Rpb24gKHVzZXJOYW1lKSB7XG4gICAgLy8gSWYgYGZpbmRVc2VyYCBmdWxmaWxsZWQsIGB1c2VyTmFtZWAgd2lsbCBiZSB0aGUgdXNlcidzIG5hbWUsIG90aGVyd2lzZSBpdFxuICAgIC8vIHdpbGwgYmUgYCdkZWZhdWx0IG5hbWUnYFxuICB9KTtcbiAgIGZpbmRVc2VyKCkudGhlbihmdW5jdGlvbiAodXNlcikge1xuICAgIHRocm93IG5ldyBFcnJvcignRm91bmQgdXNlciwgYnV0IHN0aWxsIHVuaGFwcHknKTtcbiAgfSwgZnVuY3Rpb24gKHJlYXNvbikge1xuICAgIHRocm93IG5ldyBFcnJvcignYGZpbmRVc2VyYCByZWplY3RlZCBhbmQgd2UncmUgdW5oYXBweScpO1xuICB9KS50aGVuKGZ1bmN0aW9uICh2YWx1ZSkge1xuICAgIC8vIG5ldmVyIHJlYWNoZWRcbiAgfSwgZnVuY3Rpb24gKHJlYXNvbikge1xuICAgIC8vIGlmIGBmaW5kVXNlcmAgZnVsZmlsbGVkLCBgcmVhc29uYCB3aWxsIGJlICdGb3VuZCB1c2VyLCBidXQgc3RpbGwgdW5oYXBweScuXG4gICAgLy8gSWYgYGZpbmRVc2VyYCByZWplY3RlZCwgYHJlYXNvbmAgd2lsbCBiZSAnYGZpbmRVc2VyYCByZWplY3RlZCBhbmQgd2UncmUgdW5oYXBweScuXG4gIH0pO1xuICBgYGBcbiAgSWYgdGhlIGRvd25zdHJlYW0gcHJvbWlzZSBkb2VzIG5vdCBzcGVjaWZ5IGEgcmVqZWN0aW9uIGhhbmRsZXIsIHJlamVjdGlvbiByZWFzb25zIHdpbGwgYmUgcHJvcGFnYXRlZCBmdXJ0aGVyIGRvd25zdHJlYW0uXG4gICBgYGBqc1xuICBmaW5kVXNlcigpLnRoZW4oZnVuY3Rpb24gKHVzZXIpIHtcbiAgICB0aHJvdyBuZXcgUGVkYWdvZ2ljYWxFeGNlcHRpb24oJ1Vwc3RyZWFtIGVycm9yJyk7XG4gIH0pLnRoZW4oZnVuY3Rpb24gKHZhbHVlKSB7XG4gICAgLy8gbmV2ZXIgcmVhY2hlZFxuICB9KS50aGVuKGZ1bmN0aW9uICh2YWx1ZSkge1xuICAgIC8vIG5ldmVyIHJlYWNoZWRcbiAgfSwgZnVuY3Rpb24gKHJlYXNvbikge1xuICAgIC8vIFRoZSBgUGVkZ2Fnb2NpYWxFeGNlcHRpb25gIGlzIHByb3BhZ2F0ZWQgYWxsIHRoZSB3YXkgZG93biB0byBoZXJlXG4gIH0pO1xuICBgYGBcbiAgIEFzc2ltaWxhdGlvblxuICAtLS0tLS0tLS0tLS1cbiAgIFNvbWV0aW1lcyB0aGUgdmFsdWUgeW91IHdhbnQgdG8gcHJvcGFnYXRlIHRvIGEgZG93bnN0cmVhbSBwcm9taXNlIGNhbiBvbmx5IGJlXG4gIHJldHJpZXZlZCBhc3luY2hyb25vdXNseS4gVGhpcyBjYW4gYmUgYWNoaWV2ZWQgYnkgcmV0dXJuaW5nIGEgcHJvbWlzZSBpbiB0aGVcbiAgZnVsZmlsbG1lbnQgb3IgcmVqZWN0aW9uIGhhbmRsZXIuIFRoZSBkb3duc3RyZWFtIHByb21pc2Ugd2lsbCB0aGVuIGJlIHBlbmRpbmdcbiAgdW50aWwgdGhlIHJldHVybmVkIHByb21pc2UgaXMgc2V0dGxlZC4gVGhpcyBpcyBjYWxsZWQgKmFzc2ltaWxhdGlvbiouXG4gICBgYGBqc1xuICBmaW5kVXNlcigpLnRoZW4oZnVuY3Rpb24gKHVzZXIpIHtcbiAgICByZXR1cm4gZmluZENvbW1lbnRzQnlBdXRob3IodXNlcik7XG4gIH0pLnRoZW4oZnVuY3Rpb24gKGNvbW1lbnRzKSB7XG4gICAgLy8gVGhlIHVzZXIncyBjb21tZW50cyBhcmUgbm93IGF2YWlsYWJsZVxuICB9KTtcbiAgYGBgXG4gICBJZiB0aGUgYXNzaW1saWF0ZWQgcHJvbWlzZSByZWplY3RzLCB0aGVuIHRoZSBkb3duc3RyZWFtIHByb21pc2Ugd2lsbCBhbHNvIHJlamVjdC5cbiAgIGBgYGpzXG4gIGZpbmRVc2VyKCkudGhlbihmdW5jdGlvbiAodXNlcikge1xuICAgIHJldHVybiBmaW5kQ29tbWVudHNCeUF1dGhvcih1c2VyKTtcbiAgfSkudGhlbihmdW5jdGlvbiAoY29tbWVudHMpIHtcbiAgICAvLyBJZiBgZmluZENvbW1lbnRzQnlBdXRob3JgIGZ1bGZpbGxzLCB3ZSdsbCBoYXZlIHRoZSB2YWx1ZSBoZXJlXG4gIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICAvLyBJZiBgZmluZENvbW1lbnRzQnlBdXRob3JgIHJlamVjdHMsIHdlJ2xsIGhhdmUgdGhlIHJlYXNvbiBoZXJlXG4gIH0pO1xuICBgYGBcbiAgIFNpbXBsZSBFeGFtcGxlXG4gIC0tLS0tLS0tLS0tLS0tXG4gICBTeW5jaHJvbm91cyBFeGFtcGxlXG4gICBgYGBqYXZhc2NyaXB0XG4gIGxldCByZXN1bHQ7XG4gICB0cnkge1xuICAgIHJlc3VsdCA9IGZpbmRSZXN1bHQoKTtcbiAgICAvLyBzdWNjZXNzXG4gIH0gY2F0Y2gocmVhc29uKSB7XG4gICAgLy8gZmFpbHVyZVxuICB9XG4gIGBgYFxuICAgRXJyYmFjayBFeGFtcGxlXG4gICBgYGBqc1xuICBmaW5kUmVzdWx0KGZ1bmN0aW9uKHJlc3VsdCwgZXJyKXtcbiAgICBpZiAoZXJyKSB7XG4gICAgICAvLyBmYWlsdXJlXG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIHN1Y2Nlc3NcbiAgICB9XG4gIH0pO1xuICBgYGBcbiAgIFByb21pc2UgRXhhbXBsZTtcbiAgIGBgYGphdmFzY3JpcHRcbiAgZmluZFJlc3VsdCgpLnRoZW4oZnVuY3Rpb24ocmVzdWx0KXtcbiAgICAvLyBzdWNjZXNzXG4gIH0sIGZ1bmN0aW9uKHJlYXNvbil7XG4gICAgLy8gZmFpbHVyZVxuICB9KTtcbiAgYGBgXG4gICBBZHZhbmNlZCBFeGFtcGxlXG4gIC0tLS0tLS0tLS0tLS0tXG4gICBTeW5jaHJvbm91cyBFeGFtcGxlXG4gICBgYGBqYXZhc2NyaXB0XG4gIGxldCBhdXRob3IsIGJvb2tzO1xuICAgdHJ5IHtcbiAgICBhdXRob3IgPSBmaW5kQXV0aG9yKCk7XG4gICAgYm9va3MgID0gZmluZEJvb2tzQnlBdXRob3IoYXV0aG9yKTtcbiAgICAvLyBzdWNjZXNzXG4gIH0gY2F0Y2gocmVhc29uKSB7XG4gICAgLy8gZmFpbHVyZVxuICB9XG4gIGBgYFxuICAgRXJyYmFjayBFeGFtcGxlXG4gICBgYGBqc1xuICAgZnVuY3Rpb24gZm91bmRCb29rcyhib29rcykge1xuICAgfVxuICAgZnVuY3Rpb24gZmFpbHVyZShyZWFzb24pIHtcbiAgIH1cbiAgIGZpbmRBdXRob3IoZnVuY3Rpb24oYXV0aG9yLCBlcnIpe1xuICAgIGlmIChlcnIpIHtcbiAgICAgIGZhaWx1cmUoZXJyKTtcbiAgICAgIC8vIGZhaWx1cmVcbiAgICB9IGVsc2Uge1xuICAgICAgdHJ5IHtcbiAgICAgICAgZmluZEJvb29rc0J5QXV0aG9yKGF1dGhvciwgZnVuY3Rpb24oYm9va3MsIGVycikge1xuICAgICAgICAgIGlmIChlcnIpIHtcbiAgICAgICAgICAgIGZhaWx1cmUoZXJyKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgICAgZm91bmRCb29rcyhib29rcyk7XG4gICAgICAgICAgICB9IGNhdGNoKHJlYXNvbikge1xuICAgICAgICAgICAgICBmYWlsdXJlKHJlYXNvbik7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH0gY2F0Y2goZXJyb3IpIHtcbiAgICAgICAgZmFpbHVyZShlcnIpO1xuICAgICAgfVxuICAgICAgLy8gc3VjY2Vzc1xuICAgIH1cbiAgfSk7XG4gIGBgYFxuICAgUHJvbWlzZSBFeGFtcGxlO1xuICAgYGBgamF2YXNjcmlwdFxuICBmaW5kQXV0aG9yKCkuXG4gICAgdGhlbihmaW5kQm9va3NCeUF1dGhvcikuXG4gICAgdGhlbihmdW5jdGlvbihib29rcyl7XG4gICAgICAvLyBmb3VuZCBib29rc1xuICB9KS5jYXRjaChmdW5jdGlvbihyZWFzb24pe1xuICAgIC8vIHNvbWV0aGluZyB3ZW50IHdyb25nXG4gIH0pO1xuICBgYGBcbiAgIEBtZXRob2QgdGhlblxuICBAcGFyYW0ge0Z1bmN0aW9ufSBvbkZ1bGZpbGxlZFxuICBAcGFyYW0ge0Z1bmN0aW9ufSBvblJlamVjdGVkXG4gIFVzZWZ1bCBmb3IgdG9vbGluZy5cbiAgQHJldHVybiB7UHJvbWlzZX1cbiAgKi9cblxuICAvKipcbiAgYGNhdGNoYCBpcyBzaW1wbHkgc3VnYXIgZm9yIGB0aGVuKHVuZGVmaW5lZCwgb25SZWplY3Rpb24pYCB3aGljaCBtYWtlcyBpdCB0aGUgc2FtZVxuICBhcyB0aGUgY2F0Y2ggYmxvY2sgb2YgYSB0cnkvY2F0Y2ggc3RhdGVtZW50LlxuICBgYGBqc1xuICBmdW5jdGlvbiBmaW5kQXV0aG9yKCl7XG4gIHRocm93IG5ldyBFcnJvcignY291bGRuJ3QgZmluZCB0aGF0IGF1dGhvcicpO1xuICB9XG4gIC8vIHN5bmNocm9ub3VzXG4gIHRyeSB7XG4gIGZpbmRBdXRob3IoKTtcbiAgfSBjYXRjaChyZWFzb24pIHtcbiAgLy8gc29tZXRoaW5nIHdlbnQgd3JvbmdcbiAgfVxuICAvLyBhc3luYyB3aXRoIHByb21pc2VzXG4gIGZpbmRBdXRob3IoKS5jYXRjaChmdW5jdGlvbihyZWFzb24pe1xuICAvLyBzb21ldGhpbmcgd2VudCB3cm9uZ1xuICB9KTtcbiAgYGBgXG4gIEBtZXRob2QgY2F0Y2hcbiAgQHBhcmFtIHtGdW5jdGlvbn0gb25SZWplY3Rpb25cbiAgVXNlZnVsIGZvciB0b29saW5nLlxuICBAcmV0dXJuIHtQcm9taXNlfVxuICAqL1xuXG5cbiAgUHJvbWlzZS5wcm90b3R5cGUuY2F0Y2ggPSBmdW5jdGlvbiBfY2F0Y2gob25SZWplY3Rpb24pIHtcbiAgICByZXR1cm4gdGhpcy50aGVuKG51bGwsIG9uUmVqZWN0aW9uKTtcbiAgfTtcblxuICAvKipcbiAgICBgZmluYWxseWAgd2lsbCBiZSBpbnZva2VkIHJlZ2FyZGxlc3Mgb2YgdGhlIHByb21pc2UncyBmYXRlIGp1c3QgYXMgbmF0aXZlXG4gICAgdHJ5L2NhdGNoL2ZpbmFsbHkgYmVoYXZlc1xuICBcbiAgICBTeW5jaHJvbm91cyBleGFtcGxlOlxuICBcbiAgICBgYGBqc1xuICAgIGZpbmRBdXRob3IoKSB7XG4gICAgICBpZiAoTWF0aC5yYW5kb20oKSA+IDAuNSkge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoKTtcbiAgICAgIH1cbiAgICAgIHJldHVybiBuZXcgQXV0aG9yKCk7XG4gICAgfVxuICBcbiAgICB0cnkge1xuICAgICAgcmV0dXJuIGZpbmRBdXRob3IoKTsgLy8gc3VjY2VlZCBvciBmYWlsXG4gICAgfSBjYXRjaChlcnJvcikge1xuICAgICAgcmV0dXJuIGZpbmRPdGhlckF1dGhlcigpO1xuICAgIH0gZmluYWxseSB7XG4gICAgICAvLyBhbHdheXMgcnVuc1xuICAgICAgLy8gZG9lc24ndCBhZmZlY3QgdGhlIHJldHVybiB2YWx1ZVxuICAgIH1cbiAgICBgYGBcbiAgXG4gICAgQXN5bmNocm9ub3VzIGV4YW1wbGU6XG4gIFxuICAgIGBgYGpzXG4gICAgZmluZEF1dGhvcigpLmNhdGNoKGZ1bmN0aW9uKHJlYXNvbil7XG4gICAgICByZXR1cm4gZmluZE90aGVyQXV0aGVyKCk7XG4gICAgfSkuZmluYWxseShmdW5jdGlvbigpe1xuICAgICAgLy8gYXV0aG9yIHdhcyBlaXRoZXIgZm91bmQsIG9yIG5vdFxuICAgIH0pO1xuICAgIGBgYFxuICBcbiAgICBAbWV0aG9kIGZpbmFsbHlcbiAgICBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICAgIEByZXR1cm4ge1Byb21pc2V9XG4gICovXG5cblxuICBQcm9taXNlLnByb3RvdHlwZS5maW5hbGx5ID0gZnVuY3Rpb24gX2ZpbmFsbHkoY2FsbGJhY2spIHtcbiAgICB2YXIgcHJvbWlzZSA9IHRoaXM7XG4gICAgdmFyIGNvbnN0cnVjdG9yID0gcHJvbWlzZS5jb25zdHJ1Y3RvcjtcblxuICAgIGlmIChpc0Z1bmN0aW9uKGNhbGxiYWNrKSkge1xuICAgICAgcmV0dXJuIHByb21pc2UudGhlbihmdW5jdGlvbiAodmFsdWUpIHtcbiAgICAgICAgcmV0dXJuIGNvbnN0cnVjdG9yLnJlc29sdmUoY2FsbGJhY2soKSkudGhlbihmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgcmV0dXJuIHZhbHVlO1xuICAgICAgICB9KTtcbiAgICAgIH0sIGZ1bmN0aW9uIChyZWFzb24pIHtcbiAgICAgICAgcmV0dXJuIGNvbnN0cnVjdG9yLnJlc29sdmUoY2FsbGJhY2soKSkudGhlbihmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgdGhyb3cgcmVhc29uO1xuICAgICAgICB9KTtcbiAgICAgIH0pO1xuICAgIH1cblxuICAgIHJldHVybiBwcm9taXNlLnRoZW4oY2FsbGJhY2ssIGNhbGxiYWNrKTtcbiAgfTtcblxuICByZXR1cm4gUHJvbWlzZTtcbn0oKTtcblxuUHJvbWlzZSQxLnByb3RvdHlwZS50aGVuID0gdGhlbjtcblByb21pc2UkMS5hbGwgPSBhbGw7XG5Qcm9taXNlJDEucmFjZSA9IHJhY2U7XG5Qcm9taXNlJDEucmVzb2x2ZSA9IHJlc29sdmUkMTtcblByb21pc2UkMS5yZWplY3QgPSByZWplY3QkMTtcblByb21pc2UkMS5fc2V0U2NoZWR1bGVyID0gc2V0U2NoZWR1bGVyO1xuUHJvbWlzZSQxLl9zZXRBc2FwID0gc2V0QXNhcDtcblByb21pc2UkMS5fYXNhcCA9IGFzYXA7XG5cbi8qZ2xvYmFsIHNlbGYqL1xuZnVuY3Rpb24gcG9seWZpbGwoKSB7XG4gIHZhciBsb2NhbCA9IHZvaWQgMDtcblxuICBpZiAodHlwZW9mIGdsb2JhbCAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICBsb2NhbCA9IGdsb2JhbDtcbiAgfSBlbHNlIGlmICh0eXBlb2Ygc2VsZiAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICBsb2NhbCA9IHNlbGY7XG4gIH0gZWxzZSB7XG4gICAgdHJ5IHtcbiAgICAgIGxvY2FsID0gRnVuY3Rpb24oJ3JldHVybiB0aGlzJykoKTtcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ3BvbHlmaWxsIGZhaWxlZCBiZWNhdXNlIGdsb2JhbCBvYmplY3QgaXMgdW5hdmFpbGFibGUgaW4gdGhpcyBlbnZpcm9ubWVudCcpO1xuICAgIH1cbiAgfVxuXG4gIHZhciBQID0gbG9jYWwuUHJvbWlzZTtcblxuICBpZiAoUCkge1xuICAgIHZhciBwcm9taXNlVG9TdHJpbmcgPSBudWxsO1xuICAgIHRyeSB7XG4gICAgICBwcm9taXNlVG9TdHJpbmcgPSBPYmplY3QucHJvdG90eXBlLnRvU3RyaW5nLmNhbGwoUC5yZXNvbHZlKCkpO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIC8vIHNpbGVudGx5IGlnbm9yZWRcbiAgICB9XG5cbiAgICBpZiAocHJvbWlzZVRvU3RyaW5nID09PSAnW29iamVjdCBQcm9taXNlXScgJiYgIVAuY2FzdCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgfVxuXG4gIGxvY2FsLlByb21pc2UgPSBQcm9taXNlJDE7XG59XG5cbi8vIFN0cmFuZ2UgY29tcGF0Li5cblByb21pc2UkMS5wb2x5ZmlsbCA9IHBvbHlmaWxsO1xuUHJvbWlzZSQxLlByb21pc2UgPSBQcm9taXNlJDE7XG5cbnJldHVybiBQcm9taXNlJDE7XG5cbn0pKSk7XG5cblxuXG4vLyMgc291cmNlTWFwcGluZ1VSTD1lczYtcHJvbWlzZS5tYXBcbiIsIid1c2Ugc3RyaWN0JztcblxudmFyIGhhc093biA9IE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHk7XG52YXIgdG9TdHIgPSBPYmplY3QucHJvdG90eXBlLnRvU3RyaW5nO1xudmFyIGRlZmluZVByb3BlcnR5ID0gT2JqZWN0LmRlZmluZVByb3BlcnR5O1xudmFyIGdPUEQgPSBPYmplY3QuZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yO1xuXG52YXIgaXNBcnJheSA9IGZ1bmN0aW9uIGlzQXJyYXkoYXJyKSB7XG5cdGlmICh0eXBlb2YgQXJyYXkuaXNBcnJheSA9PT0gJ2Z1bmN0aW9uJykge1xuXHRcdHJldHVybiBBcnJheS5pc0FycmF5KGFycik7XG5cdH1cblxuXHRyZXR1cm4gdG9TdHIuY2FsbChhcnIpID09PSAnW29iamVjdCBBcnJheV0nO1xufTtcblxudmFyIGlzUGxhaW5PYmplY3QgPSBmdW5jdGlvbiBpc1BsYWluT2JqZWN0KG9iaikge1xuXHRpZiAoIW9iaiB8fCB0b1N0ci5jYWxsKG9iaikgIT09ICdbb2JqZWN0IE9iamVjdF0nKSB7XG5cdFx0cmV0dXJuIGZhbHNlO1xuXHR9XG5cblx0dmFyIGhhc093bkNvbnN0cnVjdG9yID0gaGFzT3duLmNhbGwob2JqLCAnY29uc3RydWN0b3InKTtcblx0dmFyIGhhc0lzUHJvdG90eXBlT2YgPSBvYmouY29uc3RydWN0b3IgJiYgb2JqLmNvbnN0cnVjdG9yLnByb3RvdHlwZSAmJiBoYXNPd24uY2FsbChvYmouY29uc3RydWN0b3IucHJvdG90eXBlLCAnaXNQcm90b3R5cGVPZicpO1xuXHQvLyBOb3Qgb3duIGNvbnN0cnVjdG9yIHByb3BlcnR5IG11c3QgYmUgT2JqZWN0XG5cdGlmIChvYmouY29uc3RydWN0b3IgJiYgIWhhc093bkNvbnN0cnVjdG9yICYmICFoYXNJc1Byb3RvdHlwZU9mKSB7XG5cdFx0cmV0dXJuIGZhbHNlO1xuXHR9XG5cblx0Ly8gT3duIHByb3BlcnRpZXMgYXJlIGVudW1lcmF0ZWQgZmlyc3RseSwgc28gdG8gc3BlZWQgdXAsXG5cdC8vIGlmIGxhc3Qgb25lIGlzIG93biwgdGhlbiBhbGwgcHJvcGVydGllcyBhcmUgb3duLlxuXHR2YXIga2V5O1xuXHRmb3IgKGtleSBpbiBvYmopIHsgLyoqLyB9XG5cblx0cmV0dXJuIHR5cGVvZiBrZXkgPT09ICd1bmRlZmluZWQnIHx8IGhhc093bi5jYWxsKG9iaiwga2V5KTtcbn07XG5cbi8vIElmIG5hbWUgaXMgJ19fcHJvdG9fXycsIGFuZCBPYmplY3QuZGVmaW5lUHJvcGVydHkgaXMgYXZhaWxhYmxlLCBkZWZpbmUgX19wcm90b19fIGFzIGFuIG93biBwcm9wZXJ0eSBvbiB0YXJnZXRcbnZhciBzZXRQcm9wZXJ0eSA9IGZ1bmN0aW9uIHNldFByb3BlcnR5KHRhcmdldCwgb3B0aW9ucykge1xuXHRpZiAoZGVmaW5lUHJvcGVydHkgJiYgb3B0aW9ucy5uYW1lID09PSAnX19wcm90b19fJykge1xuXHRcdGRlZmluZVByb3BlcnR5KHRhcmdldCwgb3B0aW9ucy5uYW1lLCB7XG5cdFx0XHRlbnVtZXJhYmxlOiB0cnVlLFxuXHRcdFx0Y29uZmlndXJhYmxlOiB0cnVlLFxuXHRcdFx0dmFsdWU6IG9wdGlvbnMubmV3VmFsdWUsXG5cdFx0XHR3cml0YWJsZTogdHJ1ZVxuXHRcdH0pO1xuXHR9IGVsc2Uge1xuXHRcdHRhcmdldFtvcHRpb25zLm5hbWVdID0gb3B0aW9ucy5uZXdWYWx1ZTtcblx0fVxufTtcblxuLy8gUmV0dXJuIHVuZGVmaW5lZCBpbnN0ZWFkIG9mIF9fcHJvdG9fXyBpZiAnX19wcm90b19fJyBpcyBub3QgYW4gb3duIHByb3BlcnR5XG52YXIgZ2V0UHJvcGVydHkgPSBmdW5jdGlvbiBnZXRQcm9wZXJ0eShvYmosIG5hbWUpIHtcblx0aWYgKG5hbWUgPT09ICdfX3Byb3RvX18nKSB7XG5cdFx0aWYgKCFoYXNPd24uY2FsbChvYmosIG5hbWUpKSB7XG5cdFx0XHRyZXR1cm4gdm9pZCAwO1xuXHRcdH0gZWxzZSBpZiAoZ09QRCkge1xuXHRcdFx0Ly8gSW4gZWFybHkgdmVyc2lvbnMgb2Ygbm9kZSwgb2JqWydfX3Byb3RvX18nXSBpcyBidWdneSB3aGVuIG9iaiBoYXNcblx0XHRcdC8vIF9fcHJvdG9fXyBhcyBhbiBvd24gcHJvcGVydHkuIE9iamVjdC5nZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IoKSB3b3Jrcy5cblx0XHRcdHJldHVybiBnT1BEKG9iaiwgbmFtZSkudmFsdWU7XG5cdFx0fVxuXHR9XG5cblx0cmV0dXJuIG9ialtuYW1lXTtcbn07XG5cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gZXh0ZW5kKCkge1xuXHR2YXIgb3B0aW9ucywgbmFtZSwgc3JjLCBjb3B5LCBjb3B5SXNBcnJheSwgY2xvbmU7XG5cdHZhciB0YXJnZXQgPSBhcmd1bWVudHNbMF07XG5cdHZhciBpID0gMTtcblx0dmFyIGxlbmd0aCA9IGFyZ3VtZW50cy5sZW5ndGg7XG5cdHZhciBkZWVwID0gZmFsc2U7XG5cblx0Ly8gSGFuZGxlIGEgZGVlcCBjb3B5IHNpdHVhdGlvblxuXHRpZiAodHlwZW9mIHRhcmdldCA9PT0gJ2Jvb2xlYW4nKSB7XG5cdFx0ZGVlcCA9IHRhcmdldDtcblx0XHR0YXJnZXQgPSBhcmd1bWVudHNbMV0gfHwge307XG5cdFx0Ly8gc2tpcCB0aGUgYm9vbGVhbiBhbmQgdGhlIHRhcmdldFxuXHRcdGkgPSAyO1xuXHR9XG5cdGlmICh0YXJnZXQgPT0gbnVsbCB8fCAodHlwZW9mIHRhcmdldCAhPT0gJ29iamVjdCcgJiYgdHlwZW9mIHRhcmdldCAhPT0gJ2Z1bmN0aW9uJykpIHtcblx0XHR0YXJnZXQgPSB7fTtcblx0fVxuXG5cdGZvciAoOyBpIDwgbGVuZ3RoOyArK2kpIHtcblx0XHRvcHRpb25zID0gYXJndW1lbnRzW2ldO1xuXHRcdC8vIE9ubHkgZGVhbCB3aXRoIG5vbi1udWxsL3VuZGVmaW5lZCB2YWx1ZXNcblx0XHRpZiAob3B0aW9ucyAhPSBudWxsKSB7XG5cdFx0XHQvLyBFeHRlbmQgdGhlIGJhc2Ugb2JqZWN0XG5cdFx0XHRmb3IgKG5hbWUgaW4gb3B0aW9ucykge1xuXHRcdFx0XHRzcmMgPSBnZXRQcm9wZXJ0eSh0YXJnZXQsIG5hbWUpO1xuXHRcdFx0XHRjb3B5ID0gZ2V0UHJvcGVydHkob3B0aW9ucywgbmFtZSk7XG5cblx0XHRcdFx0Ly8gUHJldmVudCBuZXZlci1lbmRpbmcgbG9vcFxuXHRcdFx0XHRpZiAodGFyZ2V0ICE9PSBjb3B5KSB7XG5cdFx0XHRcdFx0Ly8gUmVjdXJzZSBpZiB3ZSdyZSBtZXJnaW5nIHBsYWluIG9iamVjdHMgb3IgYXJyYXlzXG5cdFx0XHRcdFx0aWYgKGRlZXAgJiYgY29weSAmJiAoaXNQbGFpbk9iamVjdChjb3B5KSB8fCAoY29weUlzQXJyYXkgPSBpc0FycmF5KGNvcHkpKSkpIHtcblx0XHRcdFx0XHRcdGlmIChjb3B5SXNBcnJheSkge1xuXHRcdFx0XHRcdFx0XHRjb3B5SXNBcnJheSA9IGZhbHNlO1xuXHRcdFx0XHRcdFx0XHRjbG9uZSA9IHNyYyAmJiBpc0FycmF5KHNyYykgPyBzcmMgOiBbXTtcblx0XHRcdFx0XHRcdH0gZWxzZSB7XG5cdFx0XHRcdFx0XHRcdGNsb25lID0gc3JjICYmIGlzUGxhaW5PYmplY3Qoc3JjKSA/IHNyYyA6IHt9O1xuXHRcdFx0XHRcdFx0fVxuXG5cdFx0XHRcdFx0XHQvLyBOZXZlciBtb3ZlIG9yaWdpbmFsIG9iamVjdHMsIGNsb25lIHRoZW1cblx0XHRcdFx0XHRcdHNldFByb3BlcnR5KHRhcmdldCwgeyBuYW1lOiBuYW1lLCBuZXdWYWx1ZTogZXh0ZW5kKGRlZXAsIGNsb25lLCBjb3B5KSB9KTtcblxuXHRcdFx0XHRcdC8vIERvbid0IGJyaW5nIGluIHVuZGVmaW5lZCB2YWx1ZXNcblx0XHRcdFx0XHR9IGVsc2UgaWYgKHR5cGVvZiBjb3B5ICE9PSAndW5kZWZpbmVkJykge1xuXHRcdFx0XHRcdFx0c2V0UHJvcGVydHkodGFyZ2V0LCB7IG5hbWU6IG5hbWUsIG5ld1ZhbHVlOiBjb3B5IH0pO1xuXHRcdFx0XHRcdH1cblx0XHRcdFx0fVxuXHRcdFx0fVxuXHRcdH1cblx0fVxuXG5cdC8vIFJldHVybiB0aGUgbW9kaWZpZWQgb2JqZWN0XG5cdHJldHVybiB0YXJnZXQ7XG59O1xuIiwiLypcbiAqICBiYXNlNjQuanNcbiAqXG4gKiAgTGljZW5zZWQgdW5kZXIgdGhlIEJTRCAzLUNsYXVzZSBMaWNlbnNlLlxuICogICAgaHR0cDovL29wZW5zb3VyY2Uub3JnL2xpY2Vuc2VzL0JTRC0zLUNsYXVzZVxuICpcbiAqICBSZWZlcmVuY2VzOlxuICogICAgaHR0cDovL2VuLndpa2lwZWRpYS5vcmcvd2lraS9CYXNlNjRcbiAqL1xuOyhmdW5jdGlvbiAoZ2xvYmFsLCBmYWN0b3J5KSB7XG4gICAgdHlwZW9mIGV4cG9ydHMgPT09ICdvYmplY3QnICYmIHR5cGVvZiBtb2R1bGUgIT09ICd1bmRlZmluZWQnXG4gICAgICAgID8gbW9kdWxlLmV4cG9ydHMgPSBmYWN0b3J5KGdsb2JhbClcbiAgICAgICAgOiB0eXBlb2YgZGVmaW5lID09PSAnZnVuY3Rpb24nICYmIGRlZmluZS5hbWRcbiAgICAgICAgPyBkZWZpbmUoZmFjdG9yeSkgOiBmYWN0b3J5KGdsb2JhbClcbn0oKFxuICAgIHR5cGVvZiBzZWxmICE9PSAndW5kZWZpbmVkJyA/IHNlbGZcbiAgICAgICAgOiB0eXBlb2Ygd2luZG93ICE9PSAndW5kZWZpbmVkJyA/IHdpbmRvd1xuICAgICAgICA6IHR5cGVvZiBnbG9iYWwgIT09ICd1bmRlZmluZWQnID8gZ2xvYmFsXG46IHRoaXNcbiksIGZ1bmN0aW9uKGdsb2JhbCkge1xuICAgICd1c2Ugc3RyaWN0JztcbiAgICAvLyBleGlzdGluZyB2ZXJzaW9uIGZvciBub0NvbmZsaWN0KClcbiAgICBnbG9iYWwgPSBnbG9iYWwgfHwge307XG4gICAgdmFyIF9CYXNlNjQgPSBnbG9iYWwuQmFzZTY0O1xuICAgIHZhciB2ZXJzaW9uID0gXCIyLjUuMVwiO1xuICAgIC8vIGlmIG5vZGUuanMgYW5kIE5PVCBSZWFjdCBOYXRpdmUsIHdlIHVzZSBCdWZmZXJcbiAgICB2YXIgYnVmZmVyO1xuICAgIGlmICh0eXBlb2YgbW9kdWxlICE9PSAndW5kZWZpbmVkJyAmJiBtb2R1bGUuZXhwb3J0cykge1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgYnVmZmVyID0gZXZhbChcInJlcXVpcmUoJ2J1ZmZlcicpLkJ1ZmZlclwiKTtcbiAgICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgICAgICBidWZmZXIgPSB1bmRlZmluZWQ7XG4gICAgICAgIH1cbiAgICB9XG4gICAgLy8gY29uc3RhbnRzXG4gICAgdmFyIGI2NGNoYXJzXG4gICAgICAgID0gJ0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXowMTIzNDU2Nzg5Ky8nO1xuICAgIHZhciBiNjR0YWIgPSBmdW5jdGlvbihiaW4pIHtcbiAgICAgICAgdmFyIHQgPSB7fTtcbiAgICAgICAgZm9yICh2YXIgaSA9IDAsIGwgPSBiaW4ubGVuZ3RoOyBpIDwgbDsgaSsrKSB0W2Jpbi5jaGFyQXQoaSldID0gaTtcbiAgICAgICAgcmV0dXJuIHQ7XG4gICAgfShiNjRjaGFycyk7XG4gICAgdmFyIGZyb21DaGFyQ29kZSA9IFN0cmluZy5mcm9tQ2hhckNvZGU7XG4gICAgLy8gZW5jb2RlciBzdHVmZlxuICAgIHZhciBjYl91dG9iID0gZnVuY3Rpb24oYykge1xuICAgICAgICBpZiAoYy5sZW5ndGggPCAyKSB7XG4gICAgICAgICAgICB2YXIgY2MgPSBjLmNoYXJDb2RlQXQoMCk7XG4gICAgICAgICAgICByZXR1cm4gY2MgPCAweDgwID8gY1xuICAgICAgICAgICAgICAgIDogY2MgPCAweDgwMCA/IChmcm9tQ2hhckNvZGUoMHhjMCB8IChjYyA+Pj4gNikpXG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICsgZnJvbUNoYXJDb2RlKDB4ODAgfCAoY2MgJiAweDNmKSkpXG4gICAgICAgICAgICAgICAgOiAoZnJvbUNoYXJDb2RlKDB4ZTAgfCAoKGNjID4+PiAxMikgJiAweDBmKSlcbiAgICAgICAgICAgICAgICAgICArIGZyb21DaGFyQ29kZSgweDgwIHwgKChjYyA+Pj4gIDYpICYgMHgzZikpXG4gICAgICAgICAgICAgICAgICAgKyBmcm9tQ2hhckNvZGUoMHg4MCB8ICggY2MgICAgICAgICAmIDB4M2YpKSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICB2YXIgY2MgPSAweDEwMDAwXG4gICAgICAgICAgICAgICAgKyAoYy5jaGFyQ29kZUF0KDApIC0gMHhEODAwKSAqIDB4NDAwXG4gICAgICAgICAgICAgICAgKyAoYy5jaGFyQ29kZUF0KDEpIC0gMHhEQzAwKTtcbiAgICAgICAgICAgIHJldHVybiAoZnJvbUNoYXJDb2RlKDB4ZjAgfCAoKGNjID4+PiAxOCkgJiAweDA3KSlcbiAgICAgICAgICAgICAgICAgICAgKyBmcm9tQ2hhckNvZGUoMHg4MCB8ICgoY2MgPj4+IDEyKSAmIDB4M2YpKVxuICAgICAgICAgICAgICAgICAgICArIGZyb21DaGFyQ29kZSgweDgwIHwgKChjYyA+Pj4gIDYpICYgMHgzZikpXG4gICAgICAgICAgICAgICAgICAgICsgZnJvbUNoYXJDb2RlKDB4ODAgfCAoIGNjICAgICAgICAgJiAweDNmKSkpO1xuICAgICAgICB9XG4gICAgfTtcbiAgICB2YXIgcmVfdXRvYiA9IC9bXFx1RDgwMC1cXHVEQkZGXVtcXHVEQzAwLVxcdURGRkZGXXxbXlxceDAwLVxceDdGXS9nO1xuICAgIHZhciB1dG9iID0gZnVuY3Rpb24odSkge1xuICAgICAgICByZXR1cm4gdS5yZXBsYWNlKHJlX3V0b2IsIGNiX3V0b2IpO1xuICAgIH07XG4gICAgdmFyIGNiX2VuY29kZSA9IGZ1bmN0aW9uKGNjYykge1xuICAgICAgICB2YXIgcGFkbGVuID0gWzAsIDIsIDFdW2NjYy5sZW5ndGggJSAzXSxcbiAgICAgICAgb3JkID0gY2NjLmNoYXJDb2RlQXQoMCkgPDwgMTZcbiAgICAgICAgICAgIHwgKChjY2MubGVuZ3RoID4gMSA/IGNjYy5jaGFyQ29kZUF0KDEpIDogMCkgPDwgOClcbiAgICAgICAgICAgIHwgKChjY2MubGVuZ3RoID4gMiA/IGNjYy5jaGFyQ29kZUF0KDIpIDogMCkpLFxuICAgICAgICBjaGFycyA9IFtcbiAgICAgICAgICAgIGI2NGNoYXJzLmNoYXJBdCggb3JkID4+PiAxOCksXG4gICAgICAgICAgICBiNjRjaGFycy5jaGFyQXQoKG9yZCA+Pj4gMTIpICYgNjMpLFxuICAgICAgICAgICAgcGFkbGVuID49IDIgPyAnPScgOiBiNjRjaGFycy5jaGFyQXQoKG9yZCA+Pj4gNikgJiA2MyksXG4gICAgICAgICAgICBwYWRsZW4gPj0gMSA/ICc9JyA6IGI2NGNoYXJzLmNoYXJBdChvcmQgJiA2MylcbiAgICAgICAgXTtcbiAgICAgICAgcmV0dXJuIGNoYXJzLmpvaW4oJycpO1xuICAgIH07XG4gICAgdmFyIGJ0b2EgPSBnbG9iYWwuYnRvYSA/IGZ1bmN0aW9uKGIpIHtcbiAgICAgICAgcmV0dXJuIGdsb2JhbC5idG9hKGIpO1xuICAgIH0gOiBmdW5jdGlvbihiKSB7XG4gICAgICAgIHJldHVybiBiLnJlcGxhY2UoL1tcXHNcXFNdezEsM30vZywgY2JfZW5jb2RlKTtcbiAgICB9O1xuICAgIHZhciBfZW5jb2RlID0gYnVmZmVyID9cbiAgICAgICAgYnVmZmVyLmZyb20gJiYgVWludDhBcnJheSAmJiBidWZmZXIuZnJvbSAhPT0gVWludDhBcnJheS5mcm9tXG4gICAgICAgID8gZnVuY3Rpb24gKHUpIHtcbiAgICAgICAgICAgIHJldHVybiAodS5jb25zdHJ1Y3RvciA9PT0gYnVmZmVyLmNvbnN0cnVjdG9yID8gdSA6IGJ1ZmZlci5mcm9tKHUpKVxuICAgICAgICAgICAgICAgIC50b1N0cmluZygnYmFzZTY0JylcbiAgICAgICAgfVxuICAgICAgICA6ICBmdW5jdGlvbiAodSkge1xuICAgICAgICAgICAgcmV0dXJuICh1LmNvbnN0cnVjdG9yID09PSBidWZmZXIuY29uc3RydWN0b3IgPyB1IDogbmV3ICBidWZmZXIodSkpXG4gICAgICAgICAgICAgICAgLnRvU3RyaW5nKCdiYXNlNjQnKVxuICAgICAgICB9XG4gICAgICAgIDogZnVuY3Rpb24gKHUpIHsgcmV0dXJuIGJ0b2EodXRvYih1KSkgfVxuICAgIDtcbiAgICB2YXIgZW5jb2RlID0gZnVuY3Rpb24odSwgdXJpc2FmZSkge1xuICAgICAgICByZXR1cm4gIXVyaXNhZmVcbiAgICAgICAgICAgID8gX2VuY29kZShTdHJpbmcodSkpXG4gICAgICAgICAgICA6IF9lbmNvZGUoU3RyaW5nKHUpKS5yZXBsYWNlKC9bK1xcL10vZywgZnVuY3Rpb24obTApIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gbTAgPT0gJysnID8gJy0nIDogJ18nO1xuICAgICAgICAgICAgfSkucmVwbGFjZSgvPS9nLCAnJyk7XG4gICAgfTtcbiAgICB2YXIgZW5jb2RlVVJJID0gZnVuY3Rpb24odSkgeyByZXR1cm4gZW5jb2RlKHUsIHRydWUpIH07XG4gICAgLy8gZGVjb2RlciBzdHVmZlxuICAgIHZhciByZV9idG91ID0gbmV3IFJlZ0V4cChbXG4gICAgICAgICdbXFx4QzAtXFx4REZdW1xceDgwLVxceEJGXScsXG4gICAgICAgICdbXFx4RTAtXFx4RUZdW1xceDgwLVxceEJGXXsyfScsXG4gICAgICAgICdbXFx4RjAtXFx4RjddW1xceDgwLVxceEJGXXszfSdcbiAgICBdLmpvaW4oJ3wnKSwgJ2cnKTtcbiAgICB2YXIgY2JfYnRvdSA9IGZ1bmN0aW9uKGNjY2MpIHtcbiAgICAgICAgc3dpdGNoKGNjY2MubGVuZ3RoKSB7XG4gICAgICAgIGNhc2UgNDpcbiAgICAgICAgICAgIHZhciBjcCA9ICgoMHgwNyAmIGNjY2MuY2hhckNvZGVBdCgwKSkgPDwgMTgpXG4gICAgICAgICAgICAgICAgfCAgICAoKDB4M2YgJiBjY2NjLmNoYXJDb2RlQXQoMSkpIDw8IDEyKVxuICAgICAgICAgICAgICAgIHwgICAgKCgweDNmICYgY2NjYy5jaGFyQ29kZUF0KDIpKSA8PCAgNilcbiAgICAgICAgICAgICAgICB8ICAgICAoMHgzZiAmIGNjY2MuY2hhckNvZGVBdCgzKSksXG4gICAgICAgICAgICBvZmZzZXQgPSBjcCAtIDB4MTAwMDA7XG4gICAgICAgICAgICByZXR1cm4gKGZyb21DaGFyQ29kZSgob2Zmc2V0ICA+Pj4gMTApICsgMHhEODAwKVxuICAgICAgICAgICAgICAgICAgICArIGZyb21DaGFyQ29kZSgob2Zmc2V0ICYgMHgzRkYpICsgMHhEQzAwKSk7XG4gICAgICAgIGNhc2UgMzpcbiAgICAgICAgICAgIHJldHVybiBmcm9tQ2hhckNvZGUoXG4gICAgICAgICAgICAgICAgKCgweDBmICYgY2NjYy5jaGFyQ29kZUF0KDApKSA8PCAxMilcbiAgICAgICAgICAgICAgICAgICAgfCAoKDB4M2YgJiBjY2NjLmNoYXJDb2RlQXQoMSkpIDw8IDYpXG4gICAgICAgICAgICAgICAgICAgIHwgICgweDNmICYgY2NjYy5jaGFyQ29kZUF0KDIpKVxuICAgICAgICAgICAgKTtcbiAgICAgICAgZGVmYXVsdDpcbiAgICAgICAgICAgIHJldHVybiAgZnJvbUNoYXJDb2RlKFxuICAgICAgICAgICAgICAgICgoMHgxZiAmIGNjY2MuY2hhckNvZGVBdCgwKSkgPDwgNilcbiAgICAgICAgICAgICAgICAgICAgfCAgKDB4M2YgJiBjY2NjLmNoYXJDb2RlQXQoMSkpXG4gICAgICAgICAgICApO1xuICAgICAgICB9XG4gICAgfTtcbiAgICB2YXIgYnRvdSA9IGZ1bmN0aW9uKGIpIHtcbiAgICAgICAgcmV0dXJuIGIucmVwbGFjZShyZV9idG91LCBjYl9idG91KTtcbiAgICB9O1xuICAgIHZhciBjYl9kZWNvZGUgPSBmdW5jdGlvbihjY2NjKSB7XG4gICAgICAgIHZhciBsZW4gPSBjY2NjLmxlbmd0aCxcbiAgICAgICAgcGFkbGVuID0gbGVuICUgNCxcbiAgICAgICAgbiA9IChsZW4gPiAwID8gYjY0dGFiW2NjY2MuY2hhckF0KDApXSA8PCAxOCA6IDApXG4gICAgICAgICAgICB8IChsZW4gPiAxID8gYjY0dGFiW2NjY2MuY2hhckF0KDEpXSA8PCAxMiA6IDApXG4gICAgICAgICAgICB8IChsZW4gPiAyID8gYjY0dGFiW2NjY2MuY2hhckF0KDIpXSA8PCAgNiA6IDApXG4gICAgICAgICAgICB8IChsZW4gPiAzID8gYjY0dGFiW2NjY2MuY2hhckF0KDMpXSAgICAgICA6IDApLFxuICAgICAgICBjaGFycyA9IFtcbiAgICAgICAgICAgIGZyb21DaGFyQ29kZSggbiA+Pj4gMTYpLFxuICAgICAgICAgICAgZnJvbUNoYXJDb2RlKChuID4+PiAgOCkgJiAweGZmKSxcbiAgICAgICAgICAgIGZyb21DaGFyQ29kZSggbiAgICAgICAgICYgMHhmZilcbiAgICAgICAgXTtcbiAgICAgICAgY2hhcnMubGVuZ3RoIC09IFswLCAwLCAyLCAxXVtwYWRsZW5dO1xuICAgICAgICByZXR1cm4gY2hhcnMuam9pbignJyk7XG4gICAgfTtcbiAgICB2YXIgX2F0b2IgPSBnbG9iYWwuYXRvYiA/IGZ1bmN0aW9uKGEpIHtcbiAgICAgICAgcmV0dXJuIGdsb2JhbC5hdG9iKGEpO1xuICAgIH0gOiBmdW5jdGlvbihhKXtcbiAgICAgICAgcmV0dXJuIGEucmVwbGFjZSgvXFxTezEsNH0vZywgY2JfZGVjb2RlKTtcbiAgICB9O1xuICAgIHZhciBhdG9iID0gZnVuY3Rpb24oYSkge1xuICAgICAgICByZXR1cm4gX2F0b2IoU3RyaW5nKGEpLnJlcGxhY2UoL1teQS1aYS16MC05XFwrXFwvXS9nLCAnJykpO1xuICAgIH07XG4gICAgdmFyIF9kZWNvZGUgPSBidWZmZXIgP1xuICAgICAgICBidWZmZXIuZnJvbSAmJiBVaW50OEFycmF5ICYmIGJ1ZmZlci5mcm9tICE9PSBVaW50OEFycmF5LmZyb21cbiAgICAgICAgPyBmdW5jdGlvbihhKSB7XG4gICAgICAgICAgICByZXR1cm4gKGEuY29uc3RydWN0b3IgPT09IGJ1ZmZlci5jb25zdHJ1Y3RvclxuICAgICAgICAgICAgICAgICAgICA/IGEgOiBidWZmZXIuZnJvbShhLCAnYmFzZTY0JykpLnRvU3RyaW5nKCk7XG4gICAgICAgIH1cbiAgICAgICAgOiBmdW5jdGlvbihhKSB7XG4gICAgICAgICAgICByZXR1cm4gKGEuY29uc3RydWN0b3IgPT09IGJ1ZmZlci5jb25zdHJ1Y3RvclxuICAgICAgICAgICAgICAgICAgICA/IGEgOiBuZXcgYnVmZmVyKGEsICdiYXNlNjQnKSkudG9TdHJpbmcoKTtcbiAgICAgICAgfVxuICAgICAgICA6IGZ1bmN0aW9uKGEpIHsgcmV0dXJuIGJ0b3UoX2F0b2IoYSkpIH07XG4gICAgdmFyIGRlY29kZSA9IGZ1bmN0aW9uKGEpe1xuICAgICAgICByZXR1cm4gX2RlY29kZShcbiAgICAgICAgICAgIFN0cmluZyhhKS5yZXBsYWNlKC9bLV9dL2csIGZ1bmN0aW9uKG0wKSB7IHJldHVybiBtMCA9PSAnLScgPyAnKycgOiAnLycgfSlcbiAgICAgICAgICAgICAgICAucmVwbGFjZSgvW15BLVphLXowLTlcXCtcXC9dL2csICcnKVxuICAgICAgICApO1xuICAgIH07XG4gICAgdmFyIG5vQ29uZmxpY3QgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIEJhc2U2NCA9IGdsb2JhbC5CYXNlNjQ7XG4gICAgICAgIGdsb2JhbC5CYXNlNjQgPSBfQmFzZTY0O1xuICAgICAgICByZXR1cm4gQmFzZTY0O1xuICAgIH07XG4gICAgLy8gZXhwb3J0IEJhc2U2NFxuICAgIGdsb2JhbC5CYXNlNjQgPSB7XG4gICAgICAgIFZFUlNJT046IHZlcnNpb24sXG4gICAgICAgIGF0b2I6IGF0b2IsXG4gICAgICAgIGJ0b2E6IGJ0b2EsXG4gICAgICAgIGZyb21CYXNlNjQ6IGRlY29kZSxcbiAgICAgICAgdG9CYXNlNjQ6IGVuY29kZSxcbiAgICAgICAgdXRvYjogdXRvYixcbiAgICAgICAgZW5jb2RlOiBlbmNvZGUsXG4gICAgICAgIGVuY29kZVVSSTogZW5jb2RlVVJJLFxuICAgICAgICBidG91OiBidG91LFxuICAgICAgICBkZWNvZGU6IGRlY29kZSxcbiAgICAgICAgbm9Db25mbGljdDogbm9Db25mbGljdCxcbiAgICAgICAgX19idWZmZXJfXzogYnVmZmVyXG4gICAgfTtcbiAgICAvLyBpZiBFUzUgaXMgYXZhaWxhYmxlLCBtYWtlIEJhc2U2NC5leHRlbmRTdHJpbmcoKSBhdmFpbGFibGVcbiAgICBpZiAodHlwZW9mIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICB2YXIgbm9FbnVtID0gZnVuY3Rpb24odil7XG4gICAgICAgICAgICByZXR1cm4ge3ZhbHVlOnYsZW51bWVyYWJsZTpmYWxzZSx3cml0YWJsZTp0cnVlLGNvbmZpZ3VyYWJsZTp0cnVlfTtcbiAgICAgICAgfTtcbiAgICAgICAgZ2xvYmFsLkJhc2U2NC5leHRlbmRTdHJpbmcgPSBmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkoXG4gICAgICAgICAgICAgICAgU3RyaW5nLnByb3RvdHlwZSwgJ2Zyb21CYXNlNjQnLCBub0VudW0oZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gZGVjb2RlKHRoaXMpXG4gICAgICAgICAgICAgICAgfSkpO1xuICAgICAgICAgICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KFxuICAgICAgICAgICAgICAgIFN0cmluZy5wcm90b3R5cGUsICd0b0Jhc2U2NCcsIG5vRW51bShmdW5jdGlvbiAodXJpc2FmZSkge1xuICAgICAgICAgICAgICAgICAgICByZXR1cm4gZW5jb2RlKHRoaXMsIHVyaXNhZmUpXG4gICAgICAgICAgICAgICAgfSkpO1xuICAgICAgICAgICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KFxuICAgICAgICAgICAgICAgIFN0cmluZy5wcm90b3R5cGUsICd0b0Jhc2U2NFVSSScsIG5vRW51bShmdW5jdGlvbiAoKSB7XG4gICAgICAgICAgICAgICAgICAgIHJldHVybiBlbmNvZGUodGhpcywgdHJ1ZSlcbiAgICAgICAgICAgICAgICB9KSk7XG4gICAgICAgIH07XG4gICAgfVxuICAgIC8vXG4gICAgLy8gZXhwb3J0IEJhc2U2NCB0byB0aGUgbmFtZXNwYWNlXG4gICAgLy9cbiAgICBpZiAoZ2xvYmFsWydNZXRlb3InXSkgeyAvLyBNZXRlb3IuanNcbiAgICAgICAgQmFzZTY0ID0gZ2xvYmFsLkJhc2U2NDtcbiAgICB9XG4gICAgLy8gbW9kdWxlLmV4cG9ydHMgYW5kIEFNRCBhcmUgbXV0dWFsbHkgZXhjbHVzaXZlLlxuICAgIC8vIG1vZHVsZS5leHBvcnRzIGhhcyBwcmVjZWRlbmNlLlxuICAgIGlmICh0eXBlb2YgbW9kdWxlICE9PSAndW5kZWZpbmVkJyAmJiBtb2R1bGUuZXhwb3J0cykge1xuICAgICAgICBtb2R1bGUuZXhwb3J0cy5CYXNlNjQgPSBnbG9iYWwuQmFzZTY0O1xuICAgIH1cbiAgICBlbHNlIGlmICh0eXBlb2YgZGVmaW5lID09PSAnZnVuY3Rpb24nICYmIGRlZmluZS5hbWQpIHtcbiAgICAgICAgLy8gQU1ELiBSZWdpc3RlciBhcyBhbiBhbm9ueW1vdXMgbW9kdWxlLlxuICAgICAgICBkZWZpbmUoW10sIGZ1bmN0aW9uKCl7IHJldHVybiBnbG9iYWwuQmFzZTY0IH0pO1xuICAgIH1cbiAgICAvLyB0aGF0J3MgaXQhXG4gICAgcmV0dXJuIHtCYXNlNjQ6IGdsb2JhbC5CYXNlNjR9XG59KSk7XG4iLCIvKipcbiAqIGxvZGFzaCAoQ3VzdG9tIEJ1aWxkKSA8aHR0cHM6Ly9sb2Rhc2guY29tLz5cbiAqIEJ1aWxkOiBgbG9kYXNoIG1vZHVsYXJpemUgZXhwb3J0cz1cIm5wbVwiIC1vIC4vYFxuICogQ29weXJpZ2h0IGpRdWVyeSBGb3VuZGF0aW9uIGFuZCBvdGhlciBjb250cmlidXRvcnMgPGh0dHBzOi8vanF1ZXJ5Lm9yZy8+XG4gKiBSZWxlYXNlZCB1bmRlciBNSVQgbGljZW5zZSA8aHR0cHM6Ly9sb2Rhc2guY29tL2xpY2Vuc2U+XG4gKiBCYXNlZCBvbiBVbmRlcnNjb3JlLmpzIDEuOC4zIDxodHRwOi8vdW5kZXJzY29yZWpzLm9yZy9MSUNFTlNFPlxuICogQ29weXJpZ2h0IEplcmVteSBBc2hrZW5hcywgRG9jdW1lbnRDbG91ZCBhbmQgSW52ZXN0aWdhdGl2ZSBSZXBvcnRlcnMgJiBFZGl0b3JzXG4gKi9cblxuLyoqIFVzZWQgYXMgdGhlIGBUeXBlRXJyb3JgIG1lc3NhZ2UgZm9yIFwiRnVuY3Rpb25zXCIgbWV0aG9kcy4gKi9cbnZhciBGVU5DX0VSUk9SX1RFWFQgPSAnRXhwZWN0ZWQgYSBmdW5jdGlvbic7XG5cbi8qKiBVc2VkIGFzIHJlZmVyZW5jZXMgZm9yIHZhcmlvdXMgYE51bWJlcmAgY29uc3RhbnRzLiAqL1xudmFyIE5BTiA9IDAgLyAwO1xuXG4vKiogYE9iamVjdCN0b1N0cmluZ2AgcmVzdWx0IHJlZmVyZW5jZXMuICovXG52YXIgc3ltYm9sVGFnID0gJ1tvYmplY3QgU3ltYm9sXSc7XG5cbi8qKiBVc2VkIHRvIG1hdGNoIGxlYWRpbmcgYW5kIHRyYWlsaW5nIHdoaXRlc3BhY2UuICovXG52YXIgcmVUcmltID0gL15cXHMrfFxccyskL2c7XG5cbi8qKiBVc2VkIHRvIGRldGVjdCBiYWQgc2lnbmVkIGhleGFkZWNpbWFsIHN0cmluZyB2YWx1ZXMuICovXG52YXIgcmVJc0JhZEhleCA9IC9eWy0rXTB4WzAtOWEtZl0rJC9pO1xuXG4vKiogVXNlZCB0byBkZXRlY3QgYmluYXJ5IHN0cmluZyB2YWx1ZXMuICovXG52YXIgcmVJc0JpbmFyeSA9IC9eMGJbMDFdKyQvaTtcblxuLyoqIFVzZWQgdG8gZGV0ZWN0IG9jdGFsIHN0cmluZyB2YWx1ZXMuICovXG52YXIgcmVJc09jdGFsID0gL14wb1swLTddKyQvaTtcblxuLyoqIEJ1aWx0LWluIG1ldGhvZCByZWZlcmVuY2VzIHdpdGhvdXQgYSBkZXBlbmRlbmN5IG9uIGByb290YC4gKi9cbnZhciBmcmVlUGFyc2VJbnQgPSBwYXJzZUludDtcblxuLyoqIERldGVjdCBmcmVlIHZhcmlhYmxlIGBnbG9iYWxgIGZyb20gTm9kZS5qcy4gKi9cbnZhciBmcmVlR2xvYmFsID0gdHlwZW9mIGdsb2JhbCA9PSAnb2JqZWN0JyAmJiBnbG9iYWwgJiYgZ2xvYmFsLk9iamVjdCA9PT0gT2JqZWN0ICYmIGdsb2JhbDtcblxuLyoqIERldGVjdCBmcmVlIHZhcmlhYmxlIGBzZWxmYC4gKi9cbnZhciBmcmVlU2VsZiA9IHR5cGVvZiBzZWxmID09ICdvYmplY3QnICYmIHNlbGYgJiYgc2VsZi5PYmplY3QgPT09IE9iamVjdCAmJiBzZWxmO1xuXG4vKiogVXNlZCBhcyBhIHJlZmVyZW5jZSB0byB0aGUgZ2xvYmFsIG9iamVjdC4gKi9cbnZhciByb290ID0gZnJlZUdsb2JhbCB8fCBmcmVlU2VsZiB8fCBGdW5jdGlvbigncmV0dXJuIHRoaXMnKSgpO1xuXG4vKiogVXNlZCBmb3IgYnVpbHQtaW4gbWV0aG9kIHJlZmVyZW5jZXMuICovXG52YXIgb2JqZWN0UHJvdG8gPSBPYmplY3QucHJvdG90eXBlO1xuXG4vKipcbiAqIFVzZWQgdG8gcmVzb2x2ZSB0aGVcbiAqIFtgdG9TdHJpbmdUYWdgXShodHRwOi8vZWNtYS1pbnRlcm5hdGlvbmFsLm9yZy9lY21hLTI2Mi83LjAvI3NlYy1vYmplY3QucHJvdG90eXBlLnRvc3RyaW5nKVxuICogb2YgdmFsdWVzLlxuICovXG52YXIgb2JqZWN0VG9TdHJpbmcgPSBvYmplY3RQcm90by50b1N0cmluZztcblxuLyogQnVpbHQtaW4gbWV0aG9kIHJlZmVyZW5jZXMgZm9yIHRob3NlIHdpdGggdGhlIHNhbWUgbmFtZSBhcyBvdGhlciBgbG9kYXNoYCBtZXRob2RzLiAqL1xudmFyIG5hdGl2ZU1heCA9IE1hdGgubWF4LFxuICAgIG5hdGl2ZU1pbiA9IE1hdGgubWluO1xuXG4vKipcbiAqIEdldHMgdGhlIHRpbWVzdGFtcCBvZiB0aGUgbnVtYmVyIG9mIG1pbGxpc2Vjb25kcyB0aGF0IGhhdmUgZWxhcHNlZCBzaW5jZVxuICogdGhlIFVuaXggZXBvY2ggKDEgSmFudWFyeSAxOTcwIDAwOjAwOjAwIFVUQykuXG4gKlxuICogQHN0YXRpY1xuICogQG1lbWJlck9mIF9cbiAqIEBzaW5jZSAyLjQuMFxuICogQGNhdGVnb3J5IERhdGVcbiAqIEByZXR1cm5zIHtudW1iZXJ9IFJldHVybnMgdGhlIHRpbWVzdGFtcC5cbiAqIEBleGFtcGxlXG4gKlxuICogXy5kZWZlcihmdW5jdGlvbihzdGFtcCkge1xuICogICBjb25zb2xlLmxvZyhfLm5vdygpIC0gc3RhbXApO1xuICogfSwgXy5ub3coKSk7XG4gKiAvLyA9PiBMb2dzIHRoZSBudW1iZXIgb2YgbWlsbGlzZWNvbmRzIGl0IHRvb2sgZm9yIHRoZSBkZWZlcnJlZCBpbnZvY2F0aW9uLlxuICovXG52YXIgbm93ID0gZnVuY3Rpb24oKSB7XG4gIHJldHVybiByb290LkRhdGUubm93KCk7XG59O1xuXG4vKipcbiAqIENyZWF0ZXMgYSBkZWJvdW5jZWQgZnVuY3Rpb24gdGhhdCBkZWxheXMgaW52b2tpbmcgYGZ1bmNgIHVudGlsIGFmdGVyIGB3YWl0YFxuICogbWlsbGlzZWNvbmRzIGhhdmUgZWxhcHNlZCBzaW5jZSB0aGUgbGFzdCB0aW1lIHRoZSBkZWJvdW5jZWQgZnVuY3Rpb24gd2FzXG4gKiBpbnZva2VkLiBUaGUgZGVib3VuY2VkIGZ1bmN0aW9uIGNvbWVzIHdpdGggYSBgY2FuY2VsYCBtZXRob2QgdG8gY2FuY2VsXG4gKiBkZWxheWVkIGBmdW5jYCBpbnZvY2F0aW9ucyBhbmQgYSBgZmx1c2hgIG1ldGhvZCB0byBpbW1lZGlhdGVseSBpbnZva2UgdGhlbS5cbiAqIFByb3ZpZGUgYG9wdGlvbnNgIHRvIGluZGljYXRlIHdoZXRoZXIgYGZ1bmNgIHNob3VsZCBiZSBpbnZva2VkIG9uIHRoZVxuICogbGVhZGluZyBhbmQvb3IgdHJhaWxpbmcgZWRnZSBvZiB0aGUgYHdhaXRgIHRpbWVvdXQuIFRoZSBgZnVuY2AgaXMgaW52b2tlZFxuICogd2l0aCB0aGUgbGFzdCBhcmd1bWVudHMgcHJvdmlkZWQgdG8gdGhlIGRlYm91bmNlZCBmdW5jdGlvbi4gU3Vic2VxdWVudFxuICogY2FsbHMgdG8gdGhlIGRlYm91bmNlZCBmdW5jdGlvbiByZXR1cm4gdGhlIHJlc3VsdCBvZiB0aGUgbGFzdCBgZnVuY2BcbiAqIGludm9jYXRpb24uXG4gKlxuICogKipOb3RlOioqIElmIGBsZWFkaW5nYCBhbmQgYHRyYWlsaW5nYCBvcHRpb25zIGFyZSBgdHJ1ZWAsIGBmdW5jYCBpc1xuICogaW52b2tlZCBvbiB0aGUgdHJhaWxpbmcgZWRnZSBvZiB0aGUgdGltZW91dCBvbmx5IGlmIHRoZSBkZWJvdW5jZWQgZnVuY3Rpb25cbiAqIGlzIGludm9rZWQgbW9yZSB0aGFuIG9uY2UgZHVyaW5nIHRoZSBgd2FpdGAgdGltZW91dC5cbiAqXG4gKiBJZiBgd2FpdGAgaXMgYDBgIGFuZCBgbGVhZGluZ2AgaXMgYGZhbHNlYCwgYGZ1bmNgIGludm9jYXRpb24gaXMgZGVmZXJyZWRcbiAqIHVudGlsIHRvIHRoZSBuZXh0IHRpY2ssIHNpbWlsYXIgdG8gYHNldFRpbWVvdXRgIHdpdGggYSB0aW1lb3V0IG9mIGAwYC5cbiAqXG4gKiBTZWUgW0RhdmlkIENvcmJhY2hvJ3MgYXJ0aWNsZV0oaHR0cHM6Ly9jc3MtdHJpY2tzLmNvbS9kZWJvdW5jaW5nLXRocm90dGxpbmctZXhwbGFpbmVkLWV4YW1wbGVzLylcbiAqIGZvciBkZXRhaWxzIG92ZXIgdGhlIGRpZmZlcmVuY2VzIGJldHdlZW4gYF8uZGVib3VuY2VgIGFuZCBgXy50aHJvdHRsZWAuXG4gKlxuICogQHN0YXRpY1xuICogQG1lbWJlck9mIF9cbiAqIEBzaW5jZSAwLjEuMFxuICogQGNhdGVnb3J5IEZ1bmN0aW9uXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBmdW5jIFRoZSBmdW5jdGlvbiB0byBkZWJvdW5jZS5cbiAqIEBwYXJhbSB7bnVtYmVyfSBbd2FpdD0wXSBUaGUgbnVtYmVyIG9mIG1pbGxpc2Vjb25kcyB0byBkZWxheS5cbiAqIEBwYXJhbSB7T2JqZWN0fSBbb3B0aW9ucz17fV0gVGhlIG9wdGlvbnMgb2JqZWN0LlxuICogQHBhcmFtIHtib29sZWFufSBbb3B0aW9ucy5sZWFkaW5nPWZhbHNlXVxuICogIFNwZWNpZnkgaW52b2tpbmcgb24gdGhlIGxlYWRpbmcgZWRnZSBvZiB0aGUgdGltZW91dC5cbiAqIEBwYXJhbSB7bnVtYmVyfSBbb3B0aW9ucy5tYXhXYWl0XVxuICogIFRoZSBtYXhpbXVtIHRpbWUgYGZ1bmNgIGlzIGFsbG93ZWQgdG8gYmUgZGVsYXllZCBiZWZvcmUgaXQncyBpbnZva2VkLlxuICogQHBhcmFtIHtib29sZWFufSBbb3B0aW9ucy50cmFpbGluZz10cnVlXVxuICogIFNwZWNpZnkgaW52b2tpbmcgb24gdGhlIHRyYWlsaW5nIGVkZ2Ugb2YgdGhlIHRpbWVvdXQuXG4gKiBAcmV0dXJucyB7RnVuY3Rpb259IFJldHVybnMgdGhlIG5ldyBkZWJvdW5jZWQgZnVuY3Rpb24uXG4gKiBAZXhhbXBsZVxuICpcbiAqIC8vIEF2b2lkIGNvc3RseSBjYWxjdWxhdGlvbnMgd2hpbGUgdGhlIHdpbmRvdyBzaXplIGlzIGluIGZsdXguXG4gKiBqUXVlcnkod2luZG93KS5vbigncmVzaXplJywgXy5kZWJvdW5jZShjYWxjdWxhdGVMYXlvdXQsIDE1MCkpO1xuICpcbiAqIC8vIEludm9rZSBgc2VuZE1haWxgIHdoZW4gY2xpY2tlZCwgZGVib3VuY2luZyBzdWJzZXF1ZW50IGNhbGxzLlxuICogalF1ZXJ5KGVsZW1lbnQpLm9uKCdjbGljaycsIF8uZGVib3VuY2Uoc2VuZE1haWwsIDMwMCwge1xuICogICAnbGVhZGluZyc6IHRydWUsXG4gKiAgICd0cmFpbGluZyc6IGZhbHNlXG4gKiB9KSk7XG4gKlxuICogLy8gRW5zdXJlIGBiYXRjaExvZ2AgaXMgaW52b2tlZCBvbmNlIGFmdGVyIDEgc2Vjb25kIG9mIGRlYm91bmNlZCBjYWxscy5cbiAqIHZhciBkZWJvdW5jZWQgPSBfLmRlYm91bmNlKGJhdGNoTG9nLCAyNTAsIHsgJ21heFdhaXQnOiAxMDAwIH0pO1xuICogdmFyIHNvdXJjZSA9IG5ldyBFdmVudFNvdXJjZSgnL3N0cmVhbScpO1xuICogalF1ZXJ5KHNvdXJjZSkub24oJ21lc3NhZ2UnLCBkZWJvdW5jZWQpO1xuICpcbiAqIC8vIENhbmNlbCB0aGUgdHJhaWxpbmcgZGVib3VuY2VkIGludm9jYXRpb24uXG4gKiBqUXVlcnkod2luZG93KS5vbigncG9wc3RhdGUnLCBkZWJvdW5jZWQuY2FuY2VsKTtcbiAqL1xuZnVuY3Rpb24gZGVib3VuY2UoZnVuYywgd2FpdCwgb3B0aW9ucykge1xuICB2YXIgbGFzdEFyZ3MsXG4gICAgICBsYXN0VGhpcyxcbiAgICAgIG1heFdhaXQsXG4gICAgICByZXN1bHQsXG4gICAgICB0aW1lcklkLFxuICAgICAgbGFzdENhbGxUaW1lLFxuICAgICAgbGFzdEludm9rZVRpbWUgPSAwLFxuICAgICAgbGVhZGluZyA9IGZhbHNlLFxuICAgICAgbWF4aW5nID0gZmFsc2UsXG4gICAgICB0cmFpbGluZyA9IHRydWU7XG5cbiAgaWYgKHR5cGVvZiBmdW5jICE9ICdmdW5jdGlvbicpIHtcbiAgICB0aHJvdyBuZXcgVHlwZUVycm9yKEZVTkNfRVJST1JfVEVYVCk7XG4gIH1cbiAgd2FpdCA9IHRvTnVtYmVyKHdhaXQpIHx8IDA7XG4gIGlmIChpc09iamVjdChvcHRpb25zKSkge1xuICAgIGxlYWRpbmcgPSAhIW9wdGlvbnMubGVhZGluZztcbiAgICBtYXhpbmcgPSAnbWF4V2FpdCcgaW4gb3B0aW9ucztcbiAgICBtYXhXYWl0ID0gbWF4aW5nID8gbmF0aXZlTWF4KHRvTnVtYmVyKG9wdGlvbnMubWF4V2FpdCkgfHwgMCwgd2FpdCkgOiBtYXhXYWl0O1xuICAgIHRyYWlsaW5nID0gJ3RyYWlsaW5nJyBpbiBvcHRpb25zID8gISFvcHRpb25zLnRyYWlsaW5nIDogdHJhaWxpbmc7XG4gIH1cblxuICBmdW5jdGlvbiBpbnZva2VGdW5jKHRpbWUpIHtcbiAgICB2YXIgYXJncyA9IGxhc3RBcmdzLFxuICAgICAgICB0aGlzQXJnID0gbGFzdFRoaXM7XG5cbiAgICBsYXN0QXJncyA9IGxhc3RUaGlzID0gdW5kZWZpbmVkO1xuICAgIGxhc3RJbnZva2VUaW1lID0gdGltZTtcbiAgICByZXN1bHQgPSBmdW5jLmFwcGx5KHRoaXNBcmcsIGFyZ3MpO1xuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBmdW5jdGlvbiBsZWFkaW5nRWRnZSh0aW1lKSB7XG4gICAgLy8gUmVzZXQgYW55IGBtYXhXYWl0YCB0aW1lci5cbiAgICBsYXN0SW52b2tlVGltZSA9IHRpbWU7XG4gICAgLy8gU3RhcnQgdGhlIHRpbWVyIGZvciB0aGUgdHJhaWxpbmcgZWRnZS5cbiAgICB0aW1lcklkID0gc2V0VGltZW91dCh0aW1lckV4cGlyZWQsIHdhaXQpO1xuICAgIC8vIEludm9rZSB0aGUgbGVhZGluZyBlZGdlLlxuICAgIHJldHVybiBsZWFkaW5nID8gaW52b2tlRnVuYyh0aW1lKSA6IHJlc3VsdDtcbiAgfVxuXG4gIGZ1bmN0aW9uIHJlbWFpbmluZ1dhaXQodGltZSkge1xuICAgIHZhciB0aW1lU2luY2VMYXN0Q2FsbCA9IHRpbWUgLSBsYXN0Q2FsbFRpbWUsXG4gICAgICAgIHRpbWVTaW5jZUxhc3RJbnZva2UgPSB0aW1lIC0gbGFzdEludm9rZVRpbWUsXG4gICAgICAgIHJlc3VsdCA9IHdhaXQgLSB0aW1lU2luY2VMYXN0Q2FsbDtcblxuICAgIHJldHVybiBtYXhpbmcgPyBuYXRpdmVNaW4ocmVzdWx0LCBtYXhXYWl0IC0gdGltZVNpbmNlTGFzdEludm9rZSkgOiByZXN1bHQ7XG4gIH1cblxuICBmdW5jdGlvbiBzaG91bGRJbnZva2UodGltZSkge1xuICAgIHZhciB0aW1lU2luY2VMYXN0Q2FsbCA9IHRpbWUgLSBsYXN0Q2FsbFRpbWUsXG4gICAgICAgIHRpbWVTaW5jZUxhc3RJbnZva2UgPSB0aW1lIC0gbGFzdEludm9rZVRpbWU7XG5cbiAgICAvLyBFaXRoZXIgdGhpcyBpcyB0aGUgZmlyc3QgY2FsbCwgYWN0aXZpdHkgaGFzIHN0b3BwZWQgYW5kIHdlJ3JlIGF0IHRoZVxuICAgIC8vIHRyYWlsaW5nIGVkZ2UsIHRoZSBzeXN0ZW0gdGltZSBoYXMgZ29uZSBiYWNrd2FyZHMgYW5kIHdlJ3JlIHRyZWF0aW5nXG4gICAgLy8gaXQgYXMgdGhlIHRyYWlsaW5nIGVkZ2UsIG9yIHdlJ3ZlIGhpdCB0aGUgYG1heFdhaXRgIGxpbWl0LlxuICAgIHJldHVybiAobGFzdENhbGxUaW1lID09PSB1bmRlZmluZWQgfHwgKHRpbWVTaW5jZUxhc3RDYWxsID49IHdhaXQpIHx8XG4gICAgICAodGltZVNpbmNlTGFzdENhbGwgPCAwKSB8fCAobWF4aW5nICYmIHRpbWVTaW5jZUxhc3RJbnZva2UgPj0gbWF4V2FpdCkpO1xuICB9XG5cbiAgZnVuY3Rpb24gdGltZXJFeHBpcmVkKCkge1xuICAgIHZhciB0aW1lID0gbm93KCk7XG4gICAgaWYgKHNob3VsZEludm9rZSh0aW1lKSkge1xuICAgICAgcmV0dXJuIHRyYWlsaW5nRWRnZSh0aW1lKTtcbiAgICB9XG4gICAgLy8gUmVzdGFydCB0aGUgdGltZXIuXG4gICAgdGltZXJJZCA9IHNldFRpbWVvdXQodGltZXJFeHBpcmVkLCByZW1haW5pbmdXYWl0KHRpbWUpKTtcbiAgfVxuXG4gIGZ1bmN0aW9uIHRyYWlsaW5nRWRnZSh0aW1lKSB7XG4gICAgdGltZXJJZCA9IHVuZGVmaW5lZDtcblxuICAgIC8vIE9ubHkgaW52b2tlIGlmIHdlIGhhdmUgYGxhc3RBcmdzYCB3aGljaCBtZWFucyBgZnVuY2AgaGFzIGJlZW5cbiAgICAvLyBkZWJvdW5jZWQgYXQgbGVhc3Qgb25jZS5cbiAgICBpZiAodHJhaWxpbmcgJiYgbGFzdEFyZ3MpIHtcbiAgICAgIHJldHVybiBpbnZva2VGdW5jKHRpbWUpO1xuICAgIH1cbiAgICBsYXN0QXJncyA9IGxhc3RUaGlzID0gdW5kZWZpbmVkO1xuICAgIHJldHVybiByZXN1bHQ7XG4gIH1cblxuICBmdW5jdGlvbiBjYW5jZWwoKSB7XG4gICAgaWYgKHRpbWVySWQgIT09IHVuZGVmaW5lZCkge1xuICAgICAgY2xlYXJUaW1lb3V0KHRpbWVySWQpO1xuICAgIH1cbiAgICBsYXN0SW52b2tlVGltZSA9IDA7XG4gICAgbGFzdEFyZ3MgPSBsYXN0Q2FsbFRpbWUgPSBsYXN0VGhpcyA9IHRpbWVySWQgPSB1bmRlZmluZWQ7XG4gIH1cblxuICBmdW5jdGlvbiBmbHVzaCgpIHtcbiAgICByZXR1cm4gdGltZXJJZCA9PT0gdW5kZWZpbmVkID8gcmVzdWx0IDogdHJhaWxpbmdFZGdlKG5vdygpKTtcbiAgfVxuXG4gIGZ1bmN0aW9uIGRlYm91bmNlZCgpIHtcbiAgICB2YXIgdGltZSA9IG5vdygpLFxuICAgICAgICBpc0ludm9raW5nID0gc2hvdWxkSW52b2tlKHRpbWUpO1xuXG4gICAgbGFzdEFyZ3MgPSBhcmd1bWVudHM7XG4gICAgbGFzdFRoaXMgPSB0aGlzO1xuICAgIGxhc3RDYWxsVGltZSA9IHRpbWU7XG5cbiAgICBpZiAoaXNJbnZva2luZykge1xuICAgICAgaWYgKHRpbWVySWQgPT09IHVuZGVmaW5lZCkge1xuICAgICAgICByZXR1cm4gbGVhZGluZ0VkZ2UobGFzdENhbGxUaW1lKTtcbiAgICAgIH1cbiAgICAgIGlmIChtYXhpbmcpIHtcbiAgICAgICAgLy8gSGFuZGxlIGludm9jYXRpb25zIGluIGEgdGlnaHQgbG9vcC5cbiAgICAgICAgdGltZXJJZCA9IHNldFRpbWVvdXQodGltZXJFeHBpcmVkLCB3YWl0KTtcbiAgICAgICAgcmV0dXJuIGludm9rZUZ1bmMobGFzdENhbGxUaW1lKTtcbiAgICAgIH1cbiAgICB9XG4gICAgaWYgKHRpbWVySWQgPT09IHVuZGVmaW5lZCkge1xuICAgICAgdGltZXJJZCA9IHNldFRpbWVvdXQodGltZXJFeHBpcmVkLCB3YWl0KTtcbiAgICB9XG4gICAgcmV0dXJuIHJlc3VsdDtcbiAgfVxuICBkZWJvdW5jZWQuY2FuY2VsID0gY2FuY2VsO1xuICBkZWJvdW5jZWQuZmx1c2ggPSBmbHVzaDtcbiAgcmV0dXJuIGRlYm91bmNlZDtcbn1cblxuLyoqXG4gKiBDcmVhdGVzIGEgdGhyb3R0bGVkIGZ1bmN0aW9uIHRoYXQgb25seSBpbnZva2VzIGBmdW5jYCBhdCBtb3N0IG9uY2UgcGVyXG4gKiBldmVyeSBgd2FpdGAgbWlsbGlzZWNvbmRzLiBUaGUgdGhyb3R0bGVkIGZ1bmN0aW9uIGNvbWVzIHdpdGggYSBgY2FuY2VsYFxuICogbWV0aG9kIHRvIGNhbmNlbCBkZWxheWVkIGBmdW5jYCBpbnZvY2F0aW9ucyBhbmQgYSBgZmx1c2hgIG1ldGhvZCB0b1xuICogaW1tZWRpYXRlbHkgaW52b2tlIHRoZW0uIFByb3ZpZGUgYG9wdGlvbnNgIHRvIGluZGljYXRlIHdoZXRoZXIgYGZ1bmNgXG4gKiBzaG91bGQgYmUgaW52b2tlZCBvbiB0aGUgbGVhZGluZyBhbmQvb3IgdHJhaWxpbmcgZWRnZSBvZiB0aGUgYHdhaXRgXG4gKiB0aW1lb3V0LiBUaGUgYGZ1bmNgIGlzIGludm9rZWQgd2l0aCB0aGUgbGFzdCBhcmd1bWVudHMgcHJvdmlkZWQgdG8gdGhlXG4gKiB0aHJvdHRsZWQgZnVuY3Rpb24uIFN1YnNlcXVlbnQgY2FsbHMgdG8gdGhlIHRocm90dGxlZCBmdW5jdGlvbiByZXR1cm4gdGhlXG4gKiByZXN1bHQgb2YgdGhlIGxhc3QgYGZ1bmNgIGludm9jYXRpb24uXG4gKlxuICogKipOb3RlOioqIElmIGBsZWFkaW5nYCBhbmQgYHRyYWlsaW5nYCBvcHRpb25zIGFyZSBgdHJ1ZWAsIGBmdW5jYCBpc1xuICogaW52b2tlZCBvbiB0aGUgdHJhaWxpbmcgZWRnZSBvZiB0aGUgdGltZW91dCBvbmx5IGlmIHRoZSB0aHJvdHRsZWQgZnVuY3Rpb25cbiAqIGlzIGludm9rZWQgbW9yZSB0aGFuIG9uY2UgZHVyaW5nIHRoZSBgd2FpdGAgdGltZW91dC5cbiAqXG4gKiBJZiBgd2FpdGAgaXMgYDBgIGFuZCBgbGVhZGluZ2AgaXMgYGZhbHNlYCwgYGZ1bmNgIGludm9jYXRpb24gaXMgZGVmZXJyZWRcbiAqIHVudGlsIHRvIHRoZSBuZXh0IHRpY2ssIHNpbWlsYXIgdG8gYHNldFRpbWVvdXRgIHdpdGggYSB0aW1lb3V0IG9mIGAwYC5cbiAqXG4gKiBTZWUgW0RhdmlkIENvcmJhY2hvJ3MgYXJ0aWNsZV0oaHR0cHM6Ly9jc3MtdHJpY2tzLmNvbS9kZWJvdW5jaW5nLXRocm90dGxpbmctZXhwbGFpbmVkLWV4YW1wbGVzLylcbiAqIGZvciBkZXRhaWxzIG92ZXIgdGhlIGRpZmZlcmVuY2VzIGJldHdlZW4gYF8udGhyb3R0bGVgIGFuZCBgXy5kZWJvdW5jZWAuXG4gKlxuICogQHN0YXRpY1xuICogQG1lbWJlck9mIF9cbiAqIEBzaW5jZSAwLjEuMFxuICogQGNhdGVnb3J5IEZ1bmN0aW9uXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBmdW5jIFRoZSBmdW5jdGlvbiB0byB0aHJvdHRsZS5cbiAqIEBwYXJhbSB7bnVtYmVyfSBbd2FpdD0wXSBUaGUgbnVtYmVyIG9mIG1pbGxpc2Vjb25kcyB0byB0aHJvdHRsZSBpbnZvY2F0aW9ucyB0by5cbiAqIEBwYXJhbSB7T2JqZWN0fSBbb3B0aW9ucz17fV0gVGhlIG9wdGlvbnMgb2JqZWN0LlxuICogQHBhcmFtIHtib29sZWFufSBbb3B0aW9ucy5sZWFkaW5nPXRydWVdXG4gKiAgU3BlY2lmeSBpbnZva2luZyBvbiB0aGUgbGVhZGluZyBlZGdlIG9mIHRoZSB0aW1lb3V0LlxuICogQHBhcmFtIHtib29sZWFufSBbb3B0aW9ucy50cmFpbGluZz10cnVlXVxuICogIFNwZWNpZnkgaW52b2tpbmcgb24gdGhlIHRyYWlsaW5nIGVkZ2Ugb2YgdGhlIHRpbWVvdXQuXG4gKiBAcmV0dXJucyB7RnVuY3Rpb259IFJldHVybnMgdGhlIG5ldyB0aHJvdHRsZWQgZnVuY3Rpb24uXG4gKiBAZXhhbXBsZVxuICpcbiAqIC8vIEF2b2lkIGV4Y2Vzc2l2ZWx5IHVwZGF0aW5nIHRoZSBwb3NpdGlvbiB3aGlsZSBzY3JvbGxpbmcuXG4gKiBqUXVlcnkod2luZG93KS5vbignc2Nyb2xsJywgXy50aHJvdHRsZSh1cGRhdGVQb3NpdGlvbiwgMTAwKSk7XG4gKlxuICogLy8gSW52b2tlIGByZW5ld1Rva2VuYCB3aGVuIHRoZSBjbGljayBldmVudCBpcyBmaXJlZCwgYnV0IG5vdCBtb3JlIHRoYW4gb25jZSBldmVyeSA1IG1pbnV0ZXMuXG4gKiB2YXIgdGhyb3R0bGVkID0gXy50aHJvdHRsZShyZW5ld1Rva2VuLCAzMDAwMDAsIHsgJ3RyYWlsaW5nJzogZmFsc2UgfSk7XG4gKiBqUXVlcnkoZWxlbWVudCkub24oJ2NsaWNrJywgdGhyb3R0bGVkKTtcbiAqXG4gKiAvLyBDYW5jZWwgdGhlIHRyYWlsaW5nIHRocm90dGxlZCBpbnZvY2F0aW9uLlxuICogalF1ZXJ5KHdpbmRvdykub24oJ3BvcHN0YXRlJywgdGhyb3R0bGVkLmNhbmNlbCk7XG4gKi9cbmZ1bmN0aW9uIHRocm90dGxlKGZ1bmMsIHdhaXQsIG9wdGlvbnMpIHtcbiAgdmFyIGxlYWRpbmcgPSB0cnVlLFxuICAgICAgdHJhaWxpbmcgPSB0cnVlO1xuXG4gIGlmICh0eXBlb2YgZnVuYyAhPSAnZnVuY3Rpb24nKSB7XG4gICAgdGhyb3cgbmV3IFR5cGVFcnJvcihGVU5DX0VSUk9SX1RFWFQpO1xuICB9XG4gIGlmIChpc09iamVjdChvcHRpb25zKSkge1xuICAgIGxlYWRpbmcgPSAnbGVhZGluZycgaW4gb3B0aW9ucyA/ICEhb3B0aW9ucy5sZWFkaW5nIDogbGVhZGluZztcbiAgICB0cmFpbGluZyA9ICd0cmFpbGluZycgaW4gb3B0aW9ucyA/ICEhb3B0aW9ucy50cmFpbGluZyA6IHRyYWlsaW5nO1xuICB9XG4gIHJldHVybiBkZWJvdW5jZShmdW5jLCB3YWl0LCB7XG4gICAgJ2xlYWRpbmcnOiBsZWFkaW5nLFxuICAgICdtYXhXYWl0Jzogd2FpdCxcbiAgICAndHJhaWxpbmcnOiB0cmFpbGluZ1xuICB9KTtcbn1cblxuLyoqXG4gKiBDaGVja3MgaWYgYHZhbHVlYCBpcyB0aGVcbiAqIFtsYW5ndWFnZSB0eXBlXShodHRwOi8vd3d3LmVjbWEtaW50ZXJuYXRpb25hbC5vcmcvZWNtYS0yNjIvNy4wLyNzZWMtZWNtYXNjcmlwdC1sYW5ndWFnZS10eXBlcylcbiAqIG9mIGBPYmplY3RgLiAoZS5nLiBhcnJheXMsIGZ1bmN0aW9ucywgb2JqZWN0cywgcmVnZXhlcywgYG5ldyBOdW1iZXIoMClgLCBhbmQgYG5ldyBTdHJpbmcoJycpYClcbiAqXG4gKiBAc3RhdGljXG4gKiBAbWVtYmVyT2YgX1xuICogQHNpbmNlIDAuMS4wXG4gKiBAY2F0ZWdvcnkgTGFuZ1xuICogQHBhcmFtIHsqfSB2YWx1ZSBUaGUgdmFsdWUgdG8gY2hlY2suXG4gKiBAcmV0dXJucyB7Ym9vbGVhbn0gUmV0dXJucyBgdHJ1ZWAgaWYgYHZhbHVlYCBpcyBhbiBvYmplY3QsIGVsc2UgYGZhbHNlYC5cbiAqIEBleGFtcGxlXG4gKlxuICogXy5pc09iamVjdCh7fSk7XG4gKiAvLyA9PiB0cnVlXG4gKlxuICogXy5pc09iamVjdChbMSwgMiwgM10pO1xuICogLy8gPT4gdHJ1ZVxuICpcbiAqIF8uaXNPYmplY3QoXy5ub29wKTtcbiAqIC8vID0+IHRydWVcbiAqXG4gKiBfLmlzT2JqZWN0KG51bGwpO1xuICogLy8gPT4gZmFsc2VcbiAqL1xuZnVuY3Rpb24gaXNPYmplY3QodmFsdWUpIHtcbiAgdmFyIHR5cGUgPSB0eXBlb2YgdmFsdWU7XG4gIHJldHVybiAhIXZhbHVlICYmICh0eXBlID09ICdvYmplY3QnIHx8IHR5cGUgPT0gJ2Z1bmN0aW9uJyk7XG59XG5cbi8qKlxuICogQ2hlY2tzIGlmIGB2YWx1ZWAgaXMgb2JqZWN0LWxpa2UuIEEgdmFsdWUgaXMgb2JqZWN0LWxpa2UgaWYgaXQncyBub3QgYG51bGxgXG4gKiBhbmQgaGFzIGEgYHR5cGVvZmAgcmVzdWx0IG9mIFwib2JqZWN0XCIuXG4gKlxuICogQHN0YXRpY1xuICogQG1lbWJlck9mIF9cbiAqIEBzaW5jZSA0LjAuMFxuICogQGNhdGVnb3J5IExhbmdcbiAqIEBwYXJhbSB7Kn0gdmFsdWUgVGhlIHZhbHVlIHRvIGNoZWNrLlxuICogQHJldHVybnMge2Jvb2xlYW59IFJldHVybnMgYHRydWVgIGlmIGB2YWx1ZWAgaXMgb2JqZWN0LWxpa2UsIGVsc2UgYGZhbHNlYC5cbiAqIEBleGFtcGxlXG4gKlxuICogXy5pc09iamVjdExpa2Uoe30pO1xuICogLy8gPT4gdHJ1ZVxuICpcbiAqIF8uaXNPYmplY3RMaWtlKFsxLCAyLCAzXSk7XG4gKiAvLyA9PiB0cnVlXG4gKlxuICogXy5pc09iamVjdExpa2UoXy5ub29wKTtcbiAqIC8vID0+IGZhbHNlXG4gKlxuICogXy5pc09iamVjdExpa2UobnVsbCk7XG4gKiAvLyA9PiBmYWxzZVxuICovXG5mdW5jdGlvbiBpc09iamVjdExpa2UodmFsdWUpIHtcbiAgcmV0dXJuICEhdmFsdWUgJiYgdHlwZW9mIHZhbHVlID09ICdvYmplY3QnO1xufVxuXG4vKipcbiAqIENoZWNrcyBpZiBgdmFsdWVgIGlzIGNsYXNzaWZpZWQgYXMgYSBgU3ltYm9sYCBwcmltaXRpdmUgb3Igb2JqZWN0LlxuICpcbiAqIEBzdGF0aWNcbiAqIEBtZW1iZXJPZiBfXG4gKiBAc2luY2UgNC4wLjBcbiAqIEBjYXRlZ29yeSBMYW5nXG4gKiBAcGFyYW0geyp9IHZhbHVlIFRoZSB2YWx1ZSB0byBjaGVjay5cbiAqIEByZXR1cm5zIHtib29sZWFufSBSZXR1cm5zIGB0cnVlYCBpZiBgdmFsdWVgIGlzIGEgc3ltYm9sLCBlbHNlIGBmYWxzZWAuXG4gKiBAZXhhbXBsZVxuICpcbiAqIF8uaXNTeW1ib2woU3ltYm9sLml0ZXJhdG9yKTtcbiAqIC8vID0+IHRydWVcbiAqXG4gKiBfLmlzU3ltYm9sKCdhYmMnKTtcbiAqIC8vID0+IGZhbHNlXG4gKi9cbmZ1bmN0aW9uIGlzU3ltYm9sKHZhbHVlKSB7XG4gIHJldHVybiB0eXBlb2YgdmFsdWUgPT0gJ3N5bWJvbCcgfHxcbiAgICAoaXNPYmplY3RMaWtlKHZhbHVlKSAmJiBvYmplY3RUb1N0cmluZy5jYWxsKHZhbHVlKSA9PSBzeW1ib2xUYWcpO1xufVxuXG4vKipcbiAqIENvbnZlcnRzIGB2YWx1ZWAgdG8gYSBudW1iZXIuXG4gKlxuICogQHN0YXRpY1xuICogQG1lbWJlck9mIF9cbiAqIEBzaW5jZSA0LjAuMFxuICogQGNhdGVnb3J5IExhbmdcbiAqIEBwYXJhbSB7Kn0gdmFsdWUgVGhlIHZhbHVlIHRvIHByb2Nlc3MuXG4gKiBAcmV0dXJucyB7bnVtYmVyfSBSZXR1cm5zIHRoZSBudW1iZXIuXG4gKiBAZXhhbXBsZVxuICpcbiAqIF8udG9OdW1iZXIoMy4yKTtcbiAqIC8vID0+IDMuMlxuICpcbiAqIF8udG9OdW1iZXIoTnVtYmVyLk1JTl9WQUxVRSk7XG4gKiAvLyA9PiA1ZS0zMjRcbiAqXG4gKiBfLnRvTnVtYmVyKEluZmluaXR5KTtcbiAqIC8vID0+IEluZmluaXR5XG4gKlxuICogXy50b051bWJlcignMy4yJyk7XG4gKiAvLyA9PiAzLjJcbiAqL1xuZnVuY3Rpb24gdG9OdW1iZXIodmFsdWUpIHtcbiAgaWYgKHR5cGVvZiB2YWx1ZSA9PSAnbnVtYmVyJykge1xuICAgIHJldHVybiB2YWx1ZTtcbiAgfVxuICBpZiAoaXNTeW1ib2wodmFsdWUpKSB7XG4gICAgcmV0dXJuIE5BTjtcbiAgfVxuICBpZiAoaXNPYmplY3QodmFsdWUpKSB7XG4gICAgdmFyIG90aGVyID0gdHlwZW9mIHZhbHVlLnZhbHVlT2YgPT0gJ2Z1bmN0aW9uJyA/IHZhbHVlLnZhbHVlT2YoKSA6IHZhbHVlO1xuICAgIHZhbHVlID0gaXNPYmplY3Qob3RoZXIpID8gKG90aGVyICsgJycpIDogb3RoZXI7XG4gIH1cbiAgaWYgKHR5cGVvZiB2YWx1ZSAhPSAnc3RyaW5nJykge1xuICAgIHJldHVybiB2YWx1ZSA9PT0gMCA/IHZhbHVlIDogK3ZhbHVlO1xuICB9XG4gIHZhbHVlID0gdmFsdWUucmVwbGFjZShyZVRyaW0sICcnKTtcbiAgdmFyIGlzQmluYXJ5ID0gcmVJc0JpbmFyeS50ZXN0KHZhbHVlKTtcbiAgcmV0dXJuIChpc0JpbmFyeSB8fCByZUlzT2N0YWwudGVzdCh2YWx1ZSkpXG4gICAgPyBmcmVlUGFyc2VJbnQodmFsdWUuc2xpY2UoMiksIGlzQmluYXJ5ID8gMiA6IDgpXG4gICAgOiAocmVJc0JhZEhleC50ZXN0KHZhbHVlKSA/IE5BTiA6ICt2YWx1ZSk7XG59XG5cbm1vZHVsZS5leHBvcnRzID0gdGhyb3R0bGU7XG4iLCJ2YXIgd2lsZGNhcmQgPSByZXF1aXJlKCd3aWxkY2FyZCcpO1xudmFyIHJlTWltZVBhcnRTcGxpdCA9IC9bXFwvXFwrXFwuXS87XG5cbi8qKlxuICAjIG1pbWUtbWF0Y2hcblxuICBBIHNpbXBsZSBmdW5jdGlvbiB0byBjaGVja2VyIHdoZXRoZXIgYSB0YXJnZXQgbWltZSB0eXBlIG1hdGNoZXMgYSBtaW1lLXR5cGVcbiAgcGF0dGVybiAoZS5nLiBpbWFnZS9qcGVnIG1hdGNoZXMgaW1hZ2UvanBlZyBPUiBpbWFnZS8qKS5cblxuICAjIyBFeGFtcGxlIFVzYWdlXG5cbiAgPDw8IGV4YW1wbGUuanNcblxuKiovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uKHRhcmdldCwgcGF0dGVybikge1xuICBmdW5jdGlvbiB0ZXN0KHBhdHRlcm4pIHtcbiAgICB2YXIgcmVzdWx0ID0gd2lsZGNhcmQocGF0dGVybiwgdGFyZ2V0LCByZU1pbWVQYXJ0U3BsaXQpO1xuXG4gICAgLy8gZW5zdXJlIHRoYXQgd2UgaGF2ZSBhIHZhbGlkIG1pbWUgdHlwZSAoc2hvdWxkIGhhdmUgdHdvIHBhcnRzKVxuICAgIHJldHVybiByZXN1bHQgJiYgcmVzdWx0Lmxlbmd0aCA+PSAyO1xuICB9XG5cbiAgcmV0dXJuIHBhdHRlcm4gPyB0ZXN0KHBhdHRlcm4uc3BsaXQoJzsnKVswXSkgOiB0ZXN0O1xufTtcbiIsIi8qKlxuKiBDcmVhdGUgYW4gZXZlbnQgZW1pdHRlciB3aXRoIG5hbWVzcGFjZXNcbiogQG5hbWUgY3JlYXRlTmFtZXNwYWNlRW1pdHRlclxuKiBAZXhhbXBsZVxuKiB2YXIgZW1pdHRlciA9IHJlcXVpcmUoJy4vaW5kZXgnKSgpXG4qXG4qIGVtaXR0ZXIub24oJyonLCBmdW5jdGlvbiAoKSB7XG4qICAgY29uc29sZS5sb2coJ2FsbCBldmVudHMgZW1pdHRlZCcsIHRoaXMuZXZlbnQpXG4qIH0pXG4qXG4qIGVtaXR0ZXIub24oJ2V4YW1wbGUnLCBmdW5jdGlvbiAoKSB7XG4qICAgY29uc29sZS5sb2coJ2V4YW1wbGUgZXZlbnQgZW1pdHRlZCcpXG4qIH0pXG4qL1xubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBjcmVhdGVOYW1lc3BhY2VFbWl0dGVyICgpIHtcbiAgdmFyIGVtaXR0ZXIgPSB7fVxuICB2YXIgX2ZucyA9IGVtaXR0ZXIuX2ZucyA9IHt9XG5cbiAgLyoqXG4gICogRW1pdCBhbiBldmVudC4gT3B0aW9uYWxseSBuYW1lc3BhY2UgdGhlIGV2ZW50LiBIYW5kbGVycyBhcmUgZmlyZWQgaW4gdGhlIG9yZGVyIGluIHdoaWNoIHRoZXkgd2VyZSBhZGRlZCB3aXRoIGV4YWN0IG1hdGNoZXMgdGFraW5nIHByZWNlZGVuY2UuIFNlcGFyYXRlIHRoZSBuYW1lc3BhY2UgYW5kIGV2ZW50IHdpdGggYSBgOmBcbiAgKiBAbmFtZSBlbWl0XG4gICogQHBhcmFtIHtTdHJpbmd9IGV2ZW50IOKAkyB0aGUgbmFtZSBvZiB0aGUgZXZlbnQsIHdpdGggb3B0aW9uYWwgbmFtZXNwYWNlXG4gICogQHBhcmFtIHsuLi4qfSBkYXRhIOKAkyB1cCB0byA2IGFyZ3VtZW50cyB0aGF0IGFyZSBwYXNzZWQgdG8gdGhlIGV2ZW50IGxpc3RlbmVyXG4gICogQGV4YW1wbGVcbiAgKiBlbWl0dGVyLmVtaXQoJ2V4YW1wbGUnKVxuICAqIGVtaXR0ZXIuZW1pdCgnZGVtbzp0ZXN0JylcbiAgKiBlbWl0dGVyLmVtaXQoJ2RhdGEnLCB7IGV4YW1wbGU6IHRydWV9LCAnYSBzdHJpbmcnLCAxKVxuICAqL1xuICBlbWl0dGVyLmVtaXQgPSBmdW5jdGlvbiBlbWl0IChldmVudCwgYXJnMSwgYXJnMiwgYXJnMywgYXJnNCwgYXJnNSwgYXJnNikge1xuICAgIHZhciB0b0VtaXQgPSBnZXRMaXN0ZW5lcnMoZXZlbnQpXG5cbiAgICBpZiAodG9FbWl0Lmxlbmd0aCkge1xuICAgICAgZW1pdEFsbChldmVudCwgdG9FbWl0LCBbYXJnMSwgYXJnMiwgYXJnMywgYXJnNCwgYXJnNSwgYXJnNl0pXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICogQ3JlYXRlIGVuIGV2ZW50IGxpc3RlbmVyLlxuICAqIEBuYW1lIG9uXG4gICogQHBhcmFtIHtTdHJpbmd9IGV2ZW50XG4gICogQHBhcmFtIHtGdW5jdGlvbn0gZm5cbiAgKiBAZXhhbXBsZVxuICAqIGVtaXR0ZXIub24oJ2V4YW1wbGUnLCBmdW5jdGlvbiAoKSB7fSlcbiAgKiBlbWl0dGVyLm9uKCdkZW1vJywgZnVuY3Rpb24gKCkge30pXG4gICovXG4gIGVtaXR0ZXIub24gPSBmdW5jdGlvbiBvbiAoZXZlbnQsIGZuKSB7XG4gICAgaWYgKCFfZm5zW2V2ZW50XSkge1xuICAgICAgX2Zuc1tldmVudF0gPSBbXVxuICAgIH1cblxuICAgIF9mbnNbZXZlbnRdLnB1c2goZm4pXG4gIH1cblxuICAvKipcbiAgKiBDcmVhdGUgZW4gZXZlbnQgbGlzdGVuZXIgdGhhdCBmaXJlcyBvbmNlLlxuICAqIEBuYW1lIG9uY2VcbiAgKiBAcGFyYW0ge1N0cmluZ30gZXZlbnRcbiAgKiBAcGFyYW0ge0Z1bmN0aW9ufSBmblxuICAqIEBleGFtcGxlXG4gICogZW1pdHRlci5vbmNlKCdleGFtcGxlJywgZnVuY3Rpb24gKCkge30pXG4gICogZW1pdHRlci5vbmNlKCdkZW1vJywgZnVuY3Rpb24gKCkge30pXG4gICovXG4gIGVtaXR0ZXIub25jZSA9IGZ1bmN0aW9uIG9uY2UgKGV2ZW50LCBmbikge1xuICAgIGZ1bmN0aW9uIG9uZSAoKSB7XG4gICAgICBmbi5hcHBseSh0aGlzLCBhcmd1bWVudHMpXG4gICAgICBlbWl0dGVyLm9mZihldmVudCwgb25lKVxuICAgIH1cbiAgICB0aGlzLm9uKGV2ZW50LCBvbmUpXG4gIH1cblxuICAvKipcbiAgKiBTdG9wIGxpc3RlbmluZyB0byBhbiBldmVudC4gU3RvcCBhbGwgbGlzdGVuZXJzIG9uIGFuIGV2ZW50IGJ5IG9ubHkgcGFzc2luZyB0aGUgZXZlbnQgbmFtZS4gU3RvcCBhIHNpbmdsZSBsaXN0ZW5lciBieSBwYXNzaW5nIHRoYXQgZXZlbnQgaGFuZGxlciBhcyBhIGNhbGxiYWNrLlxuICAqIFlvdSBtdXN0IGJlIGV4cGxpY2l0IGFib3V0IHdoYXQgd2lsbCBiZSB1bnN1YnNjcmliZWQ6IGBlbWl0dGVyLm9mZignZGVtbycpYCB3aWxsIHVuc3Vic2NyaWJlIGFuIGBlbWl0dGVyLm9uKCdkZW1vJylgIGxpc3RlbmVyLFxuICAqIGBlbWl0dGVyLm9mZignZGVtbzpleGFtcGxlJylgIHdpbGwgdW5zdWJzY3JpYmUgYW4gYGVtaXR0ZXIub24oJ2RlbW86ZXhhbXBsZScpYCBsaXN0ZW5lclxuICAqIEBuYW1lIG9mZlxuICAqIEBwYXJhbSB7U3RyaW5nfSBldmVudFxuICAqIEBwYXJhbSB7RnVuY3Rpb259IFtmbl0g4oCTIHRoZSBzcGVjaWZpYyBoYW5kbGVyXG4gICogQGV4YW1wbGVcbiAgKiBlbWl0dGVyLm9mZignZXhhbXBsZScpXG4gICogZW1pdHRlci5vZmYoJ2RlbW8nLCBmdW5jdGlvbiAoKSB7fSlcbiAgKi9cbiAgZW1pdHRlci5vZmYgPSBmdW5jdGlvbiBvZmYgKGV2ZW50LCBmbikge1xuICAgIHZhciBrZWVwID0gW11cblxuICAgIGlmIChldmVudCAmJiBmbikge1xuICAgICAgdmFyIGZucyA9IHRoaXMuX2Zuc1tldmVudF1cbiAgICAgIHZhciBpID0gMFxuICAgICAgdmFyIGwgPSBmbnMgPyBmbnMubGVuZ3RoIDogMFxuXG4gICAgICBmb3IgKGk7IGkgPCBsOyBpKyspIHtcbiAgICAgICAgaWYgKGZuc1tpXSAhPT0gZm4pIHtcbiAgICAgICAgICBrZWVwLnB1c2goZm5zW2ldKVxuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAga2VlcC5sZW5ndGggPyB0aGlzLl9mbnNbZXZlbnRdID0ga2VlcCA6IGRlbGV0ZSB0aGlzLl9mbnNbZXZlbnRdXG4gIH1cblxuICBmdW5jdGlvbiBnZXRMaXN0ZW5lcnMgKGUpIHtcbiAgICB2YXIgb3V0ID0gX2Zuc1tlXSA/IF9mbnNbZV0gOiBbXVxuICAgIHZhciBpZHggPSBlLmluZGV4T2YoJzonKVxuICAgIHZhciBhcmdzID0gKGlkeCA9PT0gLTEpID8gW2VdIDogW2Uuc3Vic3RyaW5nKDAsIGlkeCksIGUuc3Vic3RyaW5nKGlkeCArIDEpXVxuXG4gICAgdmFyIGtleXMgPSBPYmplY3Qua2V5cyhfZm5zKVxuICAgIHZhciBpID0gMFxuICAgIHZhciBsID0ga2V5cy5sZW5ndGhcblxuICAgIGZvciAoaTsgaSA8IGw7IGkrKykge1xuICAgICAgdmFyIGtleSA9IGtleXNbaV1cbiAgICAgIGlmIChrZXkgPT09ICcqJykge1xuICAgICAgICBvdXQgPSBvdXQuY29uY2F0KF9mbnNba2V5XSlcbiAgICAgIH1cblxuICAgICAgaWYgKGFyZ3MubGVuZ3RoID09PSAyICYmIGFyZ3NbMF0gPT09IGtleSkge1xuICAgICAgICBvdXQgPSBvdXQuY29uY2F0KF9mbnNba2V5XSlcbiAgICAgICAgYnJlYWtcbiAgICAgIH1cbiAgICB9XG5cbiAgICByZXR1cm4gb3V0XG4gIH1cblxuICBmdW5jdGlvbiBlbWl0QWxsIChlLCBmbnMsIGFyZ3MpIHtcbiAgICB2YXIgaSA9IDBcbiAgICB2YXIgbCA9IGZucy5sZW5ndGhcblxuICAgIGZvciAoaTsgaSA8IGw7IGkrKykge1xuICAgICAgaWYgKCFmbnNbaV0pIGJyZWFrXG4gICAgICBmbnNbaV0uZXZlbnQgPSBlXG4gICAgICBmbnNbaV0uYXBwbHkoZm5zW2ldLCBhcmdzKVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBlbWl0dGVyXG59XG4iLCIhZnVuY3Rpb24oKSB7XG4gICAgJ3VzZSBzdHJpY3QnO1xuICAgIGZ1bmN0aW9uIFZOb2RlKCkge31cbiAgICBmdW5jdGlvbiBoKG5vZGVOYW1lLCBhdHRyaWJ1dGVzKSB7XG4gICAgICAgIHZhciBsYXN0U2ltcGxlLCBjaGlsZCwgc2ltcGxlLCBpLCBjaGlsZHJlbiA9IEVNUFRZX0NISUxEUkVOO1xuICAgICAgICBmb3IgKGkgPSBhcmd1bWVudHMubGVuZ3RoOyBpLS0gPiAyOyApIHN0YWNrLnB1c2goYXJndW1lbnRzW2ldKTtcbiAgICAgICAgaWYgKGF0dHJpYnV0ZXMgJiYgbnVsbCAhPSBhdHRyaWJ1dGVzLmNoaWxkcmVuKSB7XG4gICAgICAgICAgICBpZiAoIXN0YWNrLmxlbmd0aCkgc3RhY2sucHVzaChhdHRyaWJ1dGVzLmNoaWxkcmVuKTtcbiAgICAgICAgICAgIGRlbGV0ZSBhdHRyaWJ1dGVzLmNoaWxkcmVuO1xuICAgICAgICB9XG4gICAgICAgIHdoaWxlIChzdGFjay5sZW5ndGgpIGlmICgoY2hpbGQgPSBzdGFjay5wb3AoKSkgJiYgdm9pZCAwICE9PSBjaGlsZC5wb3ApIGZvciAoaSA9IGNoaWxkLmxlbmd0aDsgaS0tOyApIHN0YWNrLnB1c2goY2hpbGRbaV0pOyBlbHNlIHtcbiAgICAgICAgICAgIGlmICgnYm9vbGVhbicgPT0gdHlwZW9mIGNoaWxkKSBjaGlsZCA9IG51bGw7XG4gICAgICAgICAgICBpZiAoc2ltcGxlID0gJ2Z1bmN0aW9uJyAhPSB0eXBlb2Ygbm9kZU5hbWUpIGlmIChudWxsID09IGNoaWxkKSBjaGlsZCA9ICcnOyBlbHNlIGlmICgnbnVtYmVyJyA9PSB0eXBlb2YgY2hpbGQpIGNoaWxkID0gU3RyaW5nKGNoaWxkKTsgZWxzZSBpZiAoJ3N0cmluZycgIT0gdHlwZW9mIGNoaWxkKSBzaW1wbGUgPSAhMTtcbiAgICAgICAgICAgIGlmIChzaW1wbGUgJiYgbGFzdFNpbXBsZSkgY2hpbGRyZW5bY2hpbGRyZW4ubGVuZ3RoIC0gMV0gKz0gY2hpbGQ7IGVsc2UgaWYgKGNoaWxkcmVuID09PSBFTVBUWV9DSElMRFJFTikgY2hpbGRyZW4gPSBbIGNoaWxkIF07IGVsc2UgY2hpbGRyZW4ucHVzaChjaGlsZCk7XG4gICAgICAgICAgICBsYXN0U2ltcGxlID0gc2ltcGxlO1xuICAgICAgICB9XG4gICAgICAgIHZhciBwID0gbmV3IFZOb2RlKCk7XG4gICAgICAgIHAubm9kZU5hbWUgPSBub2RlTmFtZTtcbiAgICAgICAgcC5jaGlsZHJlbiA9IGNoaWxkcmVuO1xuICAgICAgICBwLmF0dHJpYnV0ZXMgPSBudWxsID09IGF0dHJpYnV0ZXMgPyB2b2lkIDAgOiBhdHRyaWJ1dGVzO1xuICAgICAgICBwLmtleSA9IG51bGwgPT0gYXR0cmlidXRlcyA/IHZvaWQgMCA6IGF0dHJpYnV0ZXMua2V5O1xuICAgICAgICBpZiAodm9pZCAwICE9PSBvcHRpb25zLnZub2RlKSBvcHRpb25zLnZub2RlKHApO1xuICAgICAgICByZXR1cm4gcDtcbiAgICB9XG4gICAgZnVuY3Rpb24gZXh0ZW5kKG9iaiwgcHJvcHMpIHtcbiAgICAgICAgZm9yICh2YXIgaSBpbiBwcm9wcykgb2JqW2ldID0gcHJvcHNbaV07XG4gICAgICAgIHJldHVybiBvYmo7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGNsb25lRWxlbWVudCh2bm9kZSwgcHJvcHMpIHtcbiAgICAgICAgcmV0dXJuIGgodm5vZGUubm9kZU5hbWUsIGV4dGVuZChleHRlbmQoe30sIHZub2RlLmF0dHJpYnV0ZXMpLCBwcm9wcyksIGFyZ3VtZW50cy5sZW5ndGggPiAyID8gW10uc2xpY2UuY2FsbChhcmd1bWVudHMsIDIpIDogdm5vZGUuY2hpbGRyZW4pO1xuICAgIH1cbiAgICBmdW5jdGlvbiBlbnF1ZXVlUmVuZGVyKGNvbXBvbmVudCkge1xuICAgICAgICBpZiAoIWNvbXBvbmVudC5fX2QgJiYgKGNvbXBvbmVudC5fX2QgPSAhMCkgJiYgMSA9PSBpdGVtcy5wdXNoKGNvbXBvbmVudCkpIChvcHRpb25zLmRlYm91bmNlUmVuZGVyaW5nIHx8IGRlZmVyKShyZXJlbmRlcik7XG4gICAgfVxuICAgIGZ1bmN0aW9uIHJlcmVuZGVyKCkge1xuICAgICAgICB2YXIgcCwgbGlzdCA9IGl0ZW1zO1xuICAgICAgICBpdGVtcyA9IFtdO1xuICAgICAgICB3aGlsZSAocCA9IGxpc3QucG9wKCkpIGlmIChwLl9fZCkgcmVuZGVyQ29tcG9uZW50KHApO1xuICAgIH1cbiAgICBmdW5jdGlvbiBpc1NhbWVOb2RlVHlwZShub2RlLCB2bm9kZSwgaHlkcmF0aW5nKSB7XG4gICAgICAgIGlmICgnc3RyaW5nJyA9PSB0eXBlb2Ygdm5vZGUgfHwgJ251bWJlcicgPT0gdHlwZW9mIHZub2RlKSByZXR1cm4gdm9pZCAwICE9PSBub2RlLnNwbGl0VGV4dDtcbiAgICAgICAgaWYgKCdzdHJpbmcnID09IHR5cGVvZiB2bm9kZS5ub2RlTmFtZSkgcmV0dXJuICFub2RlLl9jb21wb25lbnRDb25zdHJ1Y3RvciAmJiBpc05hbWVkTm9kZShub2RlLCB2bm9kZS5ub2RlTmFtZSk7IGVsc2UgcmV0dXJuIGh5ZHJhdGluZyB8fCBub2RlLl9jb21wb25lbnRDb25zdHJ1Y3RvciA9PT0gdm5vZGUubm9kZU5hbWU7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGlzTmFtZWROb2RlKG5vZGUsIG5vZGVOYW1lKSB7XG4gICAgICAgIHJldHVybiBub2RlLl9fbiA9PT0gbm9kZU5hbWUgfHwgbm9kZS5ub2RlTmFtZS50b0xvd2VyQ2FzZSgpID09PSBub2RlTmFtZS50b0xvd2VyQ2FzZSgpO1xuICAgIH1cbiAgICBmdW5jdGlvbiBnZXROb2RlUHJvcHModm5vZGUpIHtcbiAgICAgICAgdmFyIHByb3BzID0gZXh0ZW5kKHt9LCB2bm9kZS5hdHRyaWJ1dGVzKTtcbiAgICAgICAgcHJvcHMuY2hpbGRyZW4gPSB2bm9kZS5jaGlsZHJlbjtcbiAgICAgICAgdmFyIGRlZmF1bHRQcm9wcyA9IHZub2RlLm5vZGVOYW1lLmRlZmF1bHRQcm9wcztcbiAgICAgICAgaWYgKHZvaWQgMCAhPT0gZGVmYXVsdFByb3BzKSBmb3IgKHZhciBpIGluIGRlZmF1bHRQcm9wcykgaWYgKHZvaWQgMCA9PT0gcHJvcHNbaV0pIHByb3BzW2ldID0gZGVmYXVsdFByb3BzW2ldO1xuICAgICAgICByZXR1cm4gcHJvcHM7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGNyZWF0ZU5vZGUobm9kZU5hbWUsIGlzU3ZnKSB7XG4gICAgICAgIHZhciBub2RlID0gaXNTdmcgPyBkb2N1bWVudC5jcmVhdGVFbGVtZW50TlMoJ2h0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnJywgbm9kZU5hbWUpIDogZG9jdW1lbnQuY3JlYXRlRWxlbWVudChub2RlTmFtZSk7XG4gICAgICAgIG5vZGUuX19uID0gbm9kZU5hbWU7XG4gICAgICAgIHJldHVybiBub2RlO1xuICAgIH1cbiAgICBmdW5jdGlvbiByZW1vdmVOb2RlKG5vZGUpIHtcbiAgICAgICAgdmFyIHBhcmVudE5vZGUgPSBub2RlLnBhcmVudE5vZGU7XG4gICAgICAgIGlmIChwYXJlbnROb2RlKSBwYXJlbnROb2RlLnJlbW92ZUNoaWxkKG5vZGUpO1xuICAgIH1cbiAgICBmdW5jdGlvbiBzZXRBY2Nlc3Nvcihub2RlLCBuYW1lLCBvbGQsIHZhbHVlLCBpc1N2Zykge1xuICAgICAgICBpZiAoJ2NsYXNzTmFtZScgPT09IG5hbWUpIG5hbWUgPSAnY2xhc3MnO1xuICAgICAgICBpZiAoJ2tleScgPT09IG5hbWUpIDsgZWxzZSBpZiAoJ3JlZicgPT09IG5hbWUpIHtcbiAgICAgICAgICAgIGlmIChvbGQpIG9sZChudWxsKTtcbiAgICAgICAgICAgIGlmICh2YWx1ZSkgdmFsdWUobm9kZSk7XG4gICAgICAgIH0gZWxzZSBpZiAoJ2NsYXNzJyA9PT0gbmFtZSAmJiAhaXNTdmcpIG5vZGUuY2xhc3NOYW1lID0gdmFsdWUgfHwgJyc7IGVsc2UgaWYgKCdzdHlsZScgPT09IG5hbWUpIHtcbiAgICAgICAgICAgIGlmICghdmFsdWUgfHwgJ3N0cmluZycgPT0gdHlwZW9mIHZhbHVlIHx8ICdzdHJpbmcnID09IHR5cGVvZiBvbGQpIG5vZGUuc3R5bGUuY3NzVGV4dCA9IHZhbHVlIHx8ICcnO1xuICAgICAgICAgICAgaWYgKHZhbHVlICYmICdvYmplY3QnID09IHR5cGVvZiB2YWx1ZSkge1xuICAgICAgICAgICAgICAgIGlmICgnc3RyaW5nJyAhPSB0eXBlb2Ygb2xkKSBmb3IgKHZhciBpIGluIG9sZCkgaWYgKCEoaSBpbiB2YWx1ZSkpIG5vZGUuc3R5bGVbaV0gPSAnJztcbiAgICAgICAgICAgICAgICBmb3IgKHZhciBpIGluIHZhbHVlKSBub2RlLnN0eWxlW2ldID0gJ251bWJlcicgPT0gdHlwZW9mIHZhbHVlW2ldICYmICExID09PSBJU19OT05fRElNRU5TSU9OQUwudGVzdChpKSA/IHZhbHVlW2ldICsgJ3B4JyA6IHZhbHVlW2ldO1xuICAgICAgICAgICAgfVxuICAgICAgICB9IGVsc2UgaWYgKCdkYW5nZXJvdXNseVNldElubmVySFRNTCcgPT09IG5hbWUpIHtcbiAgICAgICAgICAgIGlmICh2YWx1ZSkgbm9kZS5pbm5lckhUTUwgPSB2YWx1ZS5fX2h0bWwgfHwgJyc7XG4gICAgICAgIH0gZWxzZSBpZiAoJ28nID09IG5hbWVbMF0gJiYgJ24nID09IG5hbWVbMV0pIHtcbiAgICAgICAgICAgIHZhciB1c2VDYXB0dXJlID0gbmFtZSAhPT0gKG5hbWUgPSBuYW1lLnJlcGxhY2UoL0NhcHR1cmUkLywgJycpKTtcbiAgICAgICAgICAgIG5hbWUgPSBuYW1lLnRvTG93ZXJDYXNlKCkuc3Vic3RyaW5nKDIpO1xuICAgICAgICAgICAgaWYgKHZhbHVlKSB7XG4gICAgICAgICAgICAgICAgaWYgKCFvbGQpIG5vZGUuYWRkRXZlbnRMaXN0ZW5lcihuYW1lLCBldmVudFByb3h5LCB1c2VDYXB0dXJlKTtcbiAgICAgICAgICAgIH0gZWxzZSBub2RlLnJlbW92ZUV2ZW50TGlzdGVuZXIobmFtZSwgZXZlbnRQcm94eSwgdXNlQ2FwdHVyZSk7XG4gICAgICAgICAgICAobm9kZS5fX2wgfHwgKG5vZGUuX19sID0ge30pKVtuYW1lXSA9IHZhbHVlO1xuICAgICAgICB9IGVsc2UgaWYgKCdsaXN0JyAhPT0gbmFtZSAmJiAndHlwZScgIT09IG5hbWUgJiYgIWlzU3ZnICYmIG5hbWUgaW4gbm9kZSkge1xuICAgICAgICAgICAgc2V0UHJvcGVydHkobm9kZSwgbmFtZSwgbnVsbCA9PSB2YWx1ZSA/ICcnIDogdmFsdWUpO1xuICAgICAgICAgICAgaWYgKG51bGwgPT0gdmFsdWUgfHwgITEgPT09IHZhbHVlKSBub2RlLnJlbW92ZUF0dHJpYnV0ZShuYW1lKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHZhciBucyA9IGlzU3ZnICYmIG5hbWUgIT09IChuYW1lID0gbmFtZS5yZXBsYWNlKC9eeGxpbms6Py8sICcnKSk7XG4gICAgICAgICAgICBpZiAobnVsbCA9PSB2YWx1ZSB8fCAhMSA9PT0gdmFsdWUpIGlmIChucykgbm9kZS5yZW1vdmVBdHRyaWJ1dGVOUygnaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluaycsIG5hbWUudG9Mb3dlckNhc2UoKSk7IGVsc2Ugbm9kZS5yZW1vdmVBdHRyaWJ1dGUobmFtZSk7IGVsc2UgaWYgKCdmdW5jdGlvbicgIT0gdHlwZW9mIHZhbHVlKSBpZiAobnMpIG5vZGUuc2V0QXR0cmlidXRlTlMoJ2h0dHA6Ly93d3cudzMub3JnLzE5OTkveGxpbmsnLCBuYW1lLnRvTG93ZXJDYXNlKCksIHZhbHVlKTsgZWxzZSBub2RlLnNldEF0dHJpYnV0ZShuYW1lLCB2YWx1ZSk7XG4gICAgICAgIH1cbiAgICB9XG4gICAgZnVuY3Rpb24gc2V0UHJvcGVydHkobm9kZSwgbmFtZSwgdmFsdWUpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICAgIG5vZGVbbmFtZV0gPSB2YWx1ZTtcbiAgICAgICAgfSBjYXRjaCAoZSkge31cbiAgICB9XG4gICAgZnVuY3Rpb24gZXZlbnRQcm94eShlKSB7XG4gICAgICAgIHJldHVybiB0aGlzLl9fbFtlLnR5cGVdKG9wdGlvbnMuZXZlbnQgJiYgb3B0aW9ucy5ldmVudChlKSB8fCBlKTtcbiAgICB9XG4gICAgZnVuY3Rpb24gZmx1c2hNb3VudHMoKSB7XG4gICAgICAgIHZhciBjO1xuICAgICAgICB3aGlsZSAoYyA9IG1vdW50cy5wb3AoKSkge1xuICAgICAgICAgICAgaWYgKG9wdGlvbnMuYWZ0ZXJNb3VudCkgb3B0aW9ucy5hZnRlck1vdW50KGMpO1xuICAgICAgICAgICAgaWYgKGMuY29tcG9uZW50RGlkTW91bnQpIGMuY29tcG9uZW50RGlkTW91bnQoKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiBkaWZmKGRvbSwgdm5vZGUsIGNvbnRleHQsIG1vdW50QWxsLCBwYXJlbnQsIGNvbXBvbmVudFJvb3QpIHtcbiAgICAgICAgaWYgKCFkaWZmTGV2ZWwrKykge1xuICAgICAgICAgICAgaXNTdmdNb2RlID0gbnVsbCAhPSBwYXJlbnQgJiYgdm9pZCAwICE9PSBwYXJlbnQub3duZXJTVkdFbGVtZW50O1xuICAgICAgICAgICAgaHlkcmF0aW5nID0gbnVsbCAhPSBkb20gJiYgISgnX19wcmVhY3RhdHRyXycgaW4gZG9tKTtcbiAgICAgICAgfVxuICAgICAgICB2YXIgcmV0ID0gaWRpZmYoZG9tLCB2bm9kZSwgY29udGV4dCwgbW91bnRBbGwsIGNvbXBvbmVudFJvb3QpO1xuICAgICAgICBpZiAocGFyZW50ICYmIHJldC5wYXJlbnROb2RlICE9PSBwYXJlbnQpIHBhcmVudC5hcHBlbmRDaGlsZChyZXQpO1xuICAgICAgICBpZiAoIS0tZGlmZkxldmVsKSB7XG4gICAgICAgICAgICBoeWRyYXRpbmcgPSAhMTtcbiAgICAgICAgICAgIGlmICghY29tcG9uZW50Um9vdCkgZmx1c2hNb3VudHMoKTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gcmV0O1xuICAgIH1cbiAgICBmdW5jdGlvbiBpZGlmZihkb20sIHZub2RlLCBjb250ZXh0LCBtb3VudEFsbCwgY29tcG9uZW50Um9vdCkge1xuICAgICAgICB2YXIgb3V0ID0gZG9tLCBwcmV2U3ZnTW9kZSA9IGlzU3ZnTW9kZTtcbiAgICAgICAgaWYgKG51bGwgPT0gdm5vZGUgfHwgJ2Jvb2xlYW4nID09IHR5cGVvZiB2bm9kZSkgdm5vZGUgPSAnJztcbiAgICAgICAgaWYgKCdzdHJpbmcnID09IHR5cGVvZiB2bm9kZSB8fCAnbnVtYmVyJyA9PSB0eXBlb2Ygdm5vZGUpIHtcbiAgICAgICAgICAgIGlmIChkb20gJiYgdm9pZCAwICE9PSBkb20uc3BsaXRUZXh0ICYmIGRvbS5wYXJlbnROb2RlICYmICghZG9tLl9jb21wb25lbnQgfHwgY29tcG9uZW50Um9vdCkpIHtcbiAgICAgICAgICAgICAgICBpZiAoZG9tLm5vZGVWYWx1ZSAhPSB2bm9kZSkgZG9tLm5vZGVWYWx1ZSA9IHZub2RlO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICBvdXQgPSBkb2N1bWVudC5jcmVhdGVUZXh0Tm9kZSh2bm9kZSk7XG4gICAgICAgICAgICAgICAgaWYgKGRvbSkge1xuICAgICAgICAgICAgICAgICAgICBpZiAoZG9tLnBhcmVudE5vZGUpIGRvbS5wYXJlbnROb2RlLnJlcGxhY2VDaGlsZChvdXQsIGRvbSk7XG4gICAgICAgICAgICAgICAgICAgIHJlY29sbGVjdE5vZGVUcmVlKGRvbSwgITApO1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIG91dC5fX3ByZWFjdGF0dHJfID0gITA7XG4gICAgICAgICAgICByZXR1cm4gb3V0O1xuICAgICAgICB9XG4gICAgICAgIHZhciB2bm9kZU5hbWUgPSB2bm9kZS5ub2RlTmFtZTtcbiAgICAgICAgaWYgKCdmdW5jdGlvbicgPT0gdHlwZW9mIHZub2RlTmFtZSkgcmV0dXJuIGJ1aWxkQ29tcG9uZW50RnJvbVZOb2RlKGRvbSwgdm5vZGUsIGNvbnRleHQsIG1vdW50QWxsKTtcbiAgICAgICAgaXNTdmdNb2RlID0gJ3N2ZycgPT09IHZub2RlTmFtZSA/ICEwIDogJ2ZvcmVpZ25PYmplY3QnID09PSB2bm9kZU5hbWUgPyAhMSA6IGlzU3ZnTW9kZTtcbiAgICAgICAgdm5vZGVOYW1lID0gU3RyaW5nKHZub2RlTmFtZSk7XG4gICAgICAgIGlmICghZG9tIHx8ICFpc05hbWVkTm9kZShkb20sIHZub2RlTmFtZSkpIHtcbiAgICAgICAgICAgIG91dCA9IGNyZWF0ZU5vZGUodm5vZGVOYW1lLCBpc1N2Z01vZGUpO1xuICAgICAgICAgICAgaWYgKGRvbSkge1xuICAgICAgICAgICAgICAgIHdoaWxlIChkb20uZmlyc3RDaGlsZCkgb3V0LmFwcGVuZENoaWxkKGRvbS5maXJzdENoaWxkKTtcbiAgICAgICAgICAgICAgICBpZiAoZG9tLnBhcmVudE5vZGUpIGRvbS5wYXJlbnROb2RlLnJlcGxhY2VDaGlsZChvdXQsIGRvbSk7XG4gICAgICAgICAgICAgICAgcmVjb2xsZWN0Tm9kZVRyZWUoZG9tLCAhMCk7XG4gICAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgdmFyIGZjID0gb3V0LmZpcnN0Q2hpbGQsIHByb3BzID0gb3V0Ll9fcHJlYWN0YXR0cl8sIHZjaGlsZHJlbiA9IHZub2RlLmNoaWxkcmVuO1xuICAgICAgICBpZiAobnVsbCA9PSBwcm9wcykge1xuICAgICAgICAgICAgcHJvcHMgPSBvdXQuX19wcmVhY3RhdHRyXyA9IHt9O1xuICAgICAgICAgICAgZm9yICh2YXIgYSA9IG91dC5hdHRyaWJ1dGVzLCBpID0gYS5sZW5ndGg7IGktLTsgKSBwcm9wc1thW2ldLm5hbWVdID0gYVtpXS52YWx1ZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoIWh5ZHJhdGluZyAmJiB2Y2hpbGRyZW4gJiYgMSA9PT0gdmNoaWxkcmVuLmxlbmd0aCAmJiAnc3RyaW5nJyA9PSB0eXBlb2YgdmNoaWxkcmVuWzBdICYmIG51bGwgIT0gZmMgJiYgdm9pZCAwICE9PSBmYy5zcGxpdFRleHQgJiYgbnVsbCA9PSBmYy5uZXh0U2libGluZykge1xuICAgICAgICAgICAgaWYgKGZjLm5vZGVWYWx1ZSAhPSB2Y2hpbGRyZW5bMF0pIGZjLm5vZGVWYWx1ZSA9IHZjaGlsZHJlblswXTtcbiAgICAgICAgfSBlbHNlIGlmICh2Y2hpbGRyZW4gJiYgdmNoaWxkcmVuLmxlbmd0aCB8fCBudWxsICE9IGZjKSBpbm5lckRpZmZOb2RlKG91dCwgdmNoaWxkcmVuLCBjb250ZXh0LCBtb3VudEFsbCwgaHlkcmF0aW5nIHx8IG51bGwgIT0gcHJvcHMuZGFuZ2Vyb3VzbHlTZXRJbm5lckhUTUwpO1xuICAgICAgICBkaWZmQXR0cmlidXRlcyhvdXQsIHZub2RlLmF0dHJpYnV0ZXMsIHByb3BzKTtcbiAgICAgICAgaXNTdmdNb2RlID0gcHJldlN2Z01vZGU7XG4gICAgICAgIHJldHVybiBvdXQ7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGlubmVyRGlmZk5vZGUoZG9tLCB2Y2hpbGRyZW4sIGNvbnRleHQsIG1vdW50QWxsLCBpc0h5ZHJhdGluZykge1xuICAgICAgICB2YXIgaiwgYywgZiwgdmNoaWxkLCBjaGlsZCwgb3JpZ2luYWxDaGlsZHJlbiA9IGRvbS5jaGlsZE5vZGVzLCBjaGlsZHJlbiA9IFtdLCBrZXllZCA9IHt9LCBrZXllZExlbiA9IDAsIG1pbiA9IDAsIGxlbiA9IG9yaWdpbmFsQ2hpbGRyZW4ubGVuZ3RoLCBjaGlsZHJlbkxlbiA9IDAsIHZsZW4gPSB2Y2hpbGRyZW4gPyB2Y2hpbGRyZW4ubGVuZ3RoIDogMDtcbiAgICAgICAgaWYgKDAgIT09IGxlbikgZm9yICh2YXIgaSA9IDA7IGkgPCBsZW47IGkrKykge1xuICAgICAgICAgICAgdmFyIF9jaGlsZCA9IG9yaWdpbmFsQ2hpbGRyZW5baV0sIHByb3BzID0gX2NoaWxkLl9fcHJlYWN0YXR0cl8sIGtleSA9IHZsZW4gJiYgcHJvcHMgPyBfY2hpbGQuX2NvbXBvbmVudCA/IF9jaGlsZC5fY29tcG9uZW50Ll9fayA6IHByb3BzLmtleSA6IG51bGw7XG4gICAgICAgICAgICBpZiAobnVsbCAhPSBrZXkpIHtcbiAgICAgICAgICAgICAgICBrZXllZExlbisrO1xuICAgICAgICAgICAgICAgIGtleWVkW2tleV0gPSBfY2hpbGQ7XG4gICAgICAgICAgICB9IGVsc2UgaWYgKHByb3BzIHx8ICh2b2lkIDAgIT09IF9jaGlsZC5zcGxpdFRleHQgPyBpc0h5ZHJhdGluZyA/IF9jaGlsZC5ub2RlVmFsdWUudHJpbSgpIDogITAgOiBpc0h5ZHJhdGluZykpIGNoaWxkcmVuW2NoaWxkcmVuTGVuKytdID0gX2NoaWxkO1xuICAgICAgICB9XG4gICAgICAgIGlmICgwICE9PSB2bGVuKSBmb3IgKHZhciBpID0gMDsgaSA8IHZsZW47IGkrKykge1xuICAgICAgICAgICAgdmNoaWxkID0gdmNoaWxkcmVuW2ldO1xuICAgICAgICAgICAgY2hpbGQgPSBudWxsO1xuICAgICAgICAgICAgdmFyIGtleSA9IHZjaGlsZC5rZXk7XG4gICAgICAgICAgICBpZiAobnVsbCAhPSBrZXkpIHtcbiAgICAgICAgICAgICAgICBpZiAoa2V5ZWRMZW4gJiYgdm9pZCAwICE9PSBrZXllZFtrZXldKSB7XG4gICAgICAgICAgICAgICAgICAgIGNoaWxkID0ga2V5ZWRba2V5XTtcbiAgICAgICAgICAgICAgICAgICAga2V5ZWRba2V5XSA9IHZvaWQgMDtcbiAgICAgICAgICAgICAgICAgICAga2V5ZWRMZW4tLTtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9IGVsc2UgaWYgKCFjaGlsZCAmJiBtaW4gPCBjaGlsZHJlbkxlbikgZm9yIChqID0gbWluOyBqIDwgY2hpbGRyZW5MZW47IGorKykgaWYgKHZvaWQgMCAhPT0gY2hpbGRyZW5bal0gJiYgaXNTYW1lTm9kZVR5cGUoYyA9IGNoaWxkcmVuW2pdLCB2Y2hpbGQsIGlzSHlkcmF0aW5nKSkge1xuICAgICAgICAgICAgICAgIGNoaWxkID0gYztcbiAgICAgICAgICAgICAgICBjaGlsZHJlbltqXSA9IHZvaWQgMDtcbiAgICAgICAgICAgICAgICBpZiAoaiA9PT0gY2hpbGRyZW5MZW4gLSAxKSBjaGlsZHJlbkxlbi0tO1xuICAgICAgICAgICAgICAgIGlmIChqID09PSBtaW4pIG1pbisrO1xuICAgICAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgY2hpbGQgPSBpZGlmZihjaGlsZCwgdmNoaWxkLCBjb250ZXh0LCBtb3VudEFsbCk7XG4gICAgICAgICAgICBmID0gb3JpZ2luYWxDaGlsZHJlbltpXTtcbiAgICAgICAgICAgIGlmIChjaGlsZCAmJiBjaGlsZCAhPT0gZG9tICYmIGNoaWxkICE9PSBmKSBpZiAobnVsbCA9PSBmKSBkb20uYXBwZW5kQ2hpbGQoY2hpbGQpOyBlbHNlIGlmIChjaGlsZCA9PT0gZi5uZXh0U2libGluZykgcmVtb3ZlTm9kZShmKTsgZWxzZSBkb20uaW5zZXJ0QmVmb3JlKGNoaWxkLCBmKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoa2V5ZWRMZW4pIGZvciAodmFyIGkgaW4ga2V5ZWQpIGlmICh2b2lkIDAgIT09IGtleWVkW2ldKSByZWNvbGxlY3ROb2RlVHJlZShrZXllZFtpXSwgITEpO1xuICAgICAgICB3aGlsZSAobWluIDw9IGNoaWxkcmVuTGVuKSBpZiAodm9pZCAwICE9PSAoY2hpbGQgPSBjaGlsZHJlbltjaGlsZHJlbkxlbi0tXSkpIHJlY29sbGVjdE5vZGVUcmVlKGNoaWxkLCAhMSk7XG4gICAgfVxuICAgIGZ1bmN0aW9uIHJlY29sbGVjdE5vZGVUcmVlKG5vZGUsIHVubW91bnRPbmx5KSB7XG4gICAgICAgIHZhciBjb21wb25lbnQgPSBub2RlLl9jb21wb25lbnQ7XG4gICAgICAgIGlmIChjb21wb25lbnQpIHVubW91bnRDb21wb25lbnQoY29tcG9uZW50KTsgZWxzZSB7XG4gICAgICAgICAgICBpZiAobnVsbCAhPSBub2RlLl9fcHJlYWN0YXR0cl8gJiYgbm9kZS5fX3ByZWFjdGF0dHJfLnJlZikgbm9kZS5fX3ByZWFjdGF0dHJfLnJlZihudWxsKTtcbiAgICAgICAgICAgIGlmICghMSA9PT0gdW5tb3VudE9ubHkgfHwgbnVsbCA9PSBub2RlLl9fcHJlYWN0YXR0cl8pIHJlbW92ZU5vZGUobm9kZSk7XG4gICAgICAgICAgICByZW1vdmVDaGlsZHJlbihub2RlKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiByZW1vdmVDaGlsZHJlbihub2RlKSB7XG4gICAgICAgIG5vZGUgPSBub2RlLmxhc3RDaGlsZDtcbiAgICAgICAgd2hpbGUgKG5vZGUpIHtcbiAgICAgICAgICAgIHZhciBuZXh0ID0gbm9kZS5wcmV2aW91c1NpYmxpbmc7XG4gICAgICAgICAgICByZWNvbGxlY3ROb2RlVHJlZShub2RlLCAhMCk7XG4gICAgICAgICAgICBub2RlID0gbmV4dDtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiBkaWZmQXR0cmlidXRlcyhkb20sIGF0dHJzLCBvbGQpIHtcbiAgICAgICAgdmFyIG5hbWU7XG4gICAgICAgIGZvciAobmFtZSBpbiBvbGQpIGlmICgoIWF0dHJzIHx8IG51bGwgPT0gYXR0cnNbbmFtZV0pICYmIG51bGwgIT0gb2xkW25hbWVdKSBzZXRBY2Nlc3Nvcihkb20sIG5hbWUsIG9sZFtuYW1lXSwgb2xkW25hbWVdID0gdm9pZCAwLCBpc1N2Z01vZGUpO1xuICAgICAgICBmb3IgKG5hbWUgaW4gYXR0cnMpIGlmICghKCdjaGlsZHJlbicgPT09IG5hbWUgfHwgJ2lubmVySFRNTCcgPT09IG5hbWUgfHwgbmFtZSBpbiBvbGQgJiYgYXR0cnNbbmFtZV0gPT09ICgndmFsdWUnID09PSBuYW1lIHx8ICdjaGVja2VkJyA9PT0gbmFtZSA/IGRvbVtuYW1lXSA6IG9sZFtuYW1lXSkpKSBzZXRBY2Nlc3Nvcihkb20sIG5hbWUsIG9sZFtuYW1lXSwgb2xkW25hbWVdID0gYXR0cnNbbmFtZV0sIGlzU3ZnTW9kZSk7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGNvbGxlY3RDb21wb25lbnQoY29tcG9uZW50KSB7XG4gICAgICAgIHZhciBuYW1lID0gY29tcG9uZW50LmNvbnN0cnVjdG9yLm5hbWU7XG4gICAgICAgIChjb21wb25lbnRzW25hbWVdIHx8IChjb21wb25lbnRzW25hbWVdID0gW10pKS5wdXNoKGNvbXBvbmVudCk7XG4gICAgfVxuICAgIGZ1bmN0aW9uIGNyZWF0ZUNvbXBvbmVudChDdG9yLCBwcm9wcywgY29udGV4dCkge1xuICAgICAgICB2YXIgaW5zdCwgbGlzdCA9IGNvbXBvbmVudHNbQ3Rvci5uYW1lXTtcbiAgICAgICAgaWYgKEN0b3IucHJvdG90eXBlICYmIEN0b3IucHJvdG90eXBlLnJlbmRlcikge1xuICAgICAgICAgICAgaW5zdCA9IG5ldyBDdG9yKHByb3BzLCBjb250ZXh0KTtcbiAgICAgICAgICAgIENvbXBvbmVudC5jYWxsKGluc3QsIHByb3BzLCBjb250ZXh0KTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGluc3QgPSBuZXcgQ29tcG9uZW50KHByb3BzLCBjb250ZXh0KTtcbiAgICAgICAgICAgIGluc3QuY29uc3RydWN0b3IgPSBDdG9yO1xuICAgICAgICAgICAgaW5zdC5yZW5kZXIgPSBkb1JlbmRlcjtcbiAgICAgICAgfVxuICAgICAgICBpZiAobGlzdCkgZm9yICh2YXIgaSA9IGxpc3QubGVuZ3RoOyBpLS07ICkgaWYgKGxpc3RbaV0uY29uc3RydWN0b3IgPT09IEN0b3IpIHtcbiAgICAgICAgICAgIGluc3QuX19iID0gbGlzdFtpXS5fX2I7XG4gICAgICAgICAgICBsaXN0LnNwbGljZShpLCAxKTtcbiAgICAgICAgICAgIGJyZWFrO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBpbnN0O1xuICAgIH1cbiAgICBmdW5jdGlvbiBkb1JlbmRlcihwcm9wcywgc3RhdGUsIGNvbnRleHQpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMuY29uc3RydWN0b3IocHJvcHMsIGNvbnRleHQpO1xuICAgIH1cbiAgICBmdW5jdGlvbiBzZXRDb21wb25lbnRQcm9wcyhjb21wb25lbnQsIHByb3BzLCBvcHRzLCBjb250ZXh0LCBtb3VudEFsbCkge1xuICAgICAgICBpZiAoIWNvbXBvbmVudC5fX3gpIHtcbiAgICAgICAgICAgIGNvbXBvbmVudC5fX3ggPSAhMDtcbiAgICAgICAgICAgIGlmIChjb21wb25lbnQuX19yID0gcHJvcHMucmVmKSBkZWxldGUgcHJvcHMucmVmO1xuICAgICAgICAgICAgaWYgKGNvbXBvbmVudC5fX2sgPSBwcm9wcy5rZXkpIGRlbGV0ZSBwcm9wcy5rZXk7XG4gICAgICAgICAgICBpZiAoIWNvbXBvbmVudC5iYXNlIHx8IG1vdW50QWxsKSB7XG4gICAgICAgICAgICAgICAgaWYgKGNvbXBvbmVudC5jb21wb25lbnRXaWxsTW91bnQpIGNvbXBvbmVudC5jb21wb25lbnRXaWxsTW91bnQoKTtcbiAgICAgICAgICAgIH0gZWxzZSBpZiAoY29tcG9uZW50LmNvbXBvbmVudFdpbGxSZWNlaXZlUHJvcHMpIGNvbXBvbmVudC5jb21wb25lbnRXaWxsUmVjZWl2ZVByb3BzKHByb3BzLCBjb250ZXh0KTtcbiAgICAgICAgICAgIGlmIChjb250ZXh0ICYmIGNvbnRleHQgIT09IGNvbXBvbmVudC5jb250ZXh0KSB7XG4gICAgICAgICAgICAgICAgaWYgKCFjb21wb25lbnQuX19jKSBjb21wb25lbnQuX19jID0gY29tcG9uZW50LmNvbnRleHQ7XG4gICAgICAgICAgICAgICAgY29tcG9uZW50LmNvbnRleHQgPSBjb250ZXh0O1xuICAgICAgICAgICAgfVxuICAgICAgICAgICAgaWYgKCFjb21wb25lbnQuX19wKSBjb21wb25lbnQuX19wID0gY29tcG9uZW50LnByb3BzO1xuICAgICAgICAgICAgY29tcG9uZW50LnByb3BzID0gcHJvcHM7XG4gICAgICAgICAgICBjb21wb25lbnQuX194ID0gITE7XG4gICAgICAgICAgICBpZiAoMCAhPT0gb3B0cykgaWYgKDEgPT09IG9wdHMgfHwgITEgIT09IG9wdGlvbnMuc3luY0NvbXBvbmVudFVwZGF0ZXMgfHwgIWNvbXBvbmVudC5iYXNlKSByZW5kZXJDb21wb25lbnQoY29tcG9uZW50LCAxLCBtb3VudEFsbCk7IGVsc2UgZW5xdWV1ZVJlbmRlcihjb21wb25lbnQpO1xuICAgICAgICAgICAgaWYgKGNvbXBvbmVudC5fX3IpIGNvbXBvbmVudC5fX3IoY29tcG9uZW50KTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiByZW5kZXJDb21wb25lbnQoY29tcG9uZW50LCBvcHRzLCBtb3VudEFsbCwgaXNDaGlsZCkge1xuICAgICAgICBpZiAoIWNvbXBvbmVudC5fX3gpIHtcbiAgICAgICAgICAgIHZhciByZW5kZXJlZCwgaW5zdCwgY2Jhc2UsIHByb3BzID0gY29tcG9uZW50LnByb3BzLCBzdGF0ZSA9IGNvbXBvbmVudC5zdGF0ZSwgY29udGV4dCA9IGNvbXBvbmVudC5jb250ZXh0LCBwcmV2aW91c1Byb3BzID0gY29tcG9uZW50Ll9fcCB8fCBwcm9wcywgcHJldmlvdXNTdGF0ZSA9IGNvbXBvbmVudC5fX3MgfHwgc3RhdGUsIHByZXZpb3VzQ29udGV4dCA9IGNvbXBvbmVudC5fX2MgfHwgY29udGV4dCwgaXNVcGRhdGUgPSBjb21wb25lbnQuYmFzZSwgbmV4dEJhc2UgPSBjb21wb25lbnQuX19iLCBpbml0aWFsQmFzZSA9IGlzVXBkYXRlIHx8IG5leHRCYXNlLCBpbml0aWFsQ2hpbGRDb21wb25lbnQgPSBjb21wb25lbnQuX2NvbXBvbmVudCwgc2tpcCA9ICExO1xuICAgICAgICAgICAgaWYgKGlzVXBkYXRlKSB7XG4gICAgICAgICAgICAgICAgY29tcG9uZW50LnByb3BzID0gcHJldmlvdXNQcm9wcztcbiAgICAgICAgICAgICAgICBjb21wb25lbnQuc3RhdGUgPSBwcmV2aW91c1N0YXRlO1xuICAgICAgICAgICAgICAgIGNvbXBvbmVudC5jb250ZXh0ID0gcHJldmlvdXNDb250ZXh0O1xuICAgICAgICAgICAgICAgIGlmICgyICE9PSBvcHRzICYmIGNvbXBvbmVudC5zaG91bGRDb21wb25lbnRVcGRhdGUgJiYgITEgPT09IGNvbXBvbmVudC5zaG91bGRDb21wb25lbnRVcGRhdGUocHJvcHMsIHN0YXRlLCBjb250ZXh0KSkgc2tpcCA9ICEwOyBlbHNlIGlmIChjb21wb25lbnQuY29tcG9uZW50V2lsbFVwZGF0ZSkgY29tcG9uZW50LmNvbXBvbmVudFdpbGxVcGRhdGUocHJvcHMsIHN0YXRlLCBjb250ZXh0KTtcbiAgICAgICAgICAgICAgICBjb21wb25lbnQucHJvcHMgPSBwcm9wcztcbiAgICAgICAgICAgICAgICBjb21wb25lbnQuc3RhdGUgPSBzdGF0ZTtcbiAgICAgICAgICAgICAgICBjb21wb25lbnQuY29udGV4dCA9IGNvbnRleHQ7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjb21wb25lbnQuX19wID0gY29tcG9uZW50Ll9fcyA9IGNvbXBvbmVudC5fX2MgPSBjb21wb25lbnQuX19iID0gbnVsbDtcbiAgICAgICAgICAgIGNvbXBvbmVudC5fX2QgPSAhMTtcbiAgICAgICAgICAgIGlmICghc2tpcCkge1xuICAgICAgICAgICAgICAgIHJlbmRlcmVkID0gY29tcG9uZW50LnJlbmRlcihwcm9wcywgc3RhdGUsIGNvbnRleHQpO1xuICAgICAgICAgICAgICAgIGlmIChjb21wb25lbnQuZ2V0Q2hpbGRDb250ZXh0KSBjb250ZXh0ID0gZXh0ZW5kKGV4dGVuZCh7fSwgY29udGV4dCksIGNvbXBvbmVudC5nZXRDaGlsZENvbnRleHQoKSk7XG4gICAgICAgICAgICAgICAgdmFyIHRvVW5tb3VudCwgYmFzZSwgY2hpbGRDb21wb25lbnQgPSByZW5kZXJlZCAmJiByZW5kZXJlZC5ub2RlTmFtZTtcbiAgICAgICAgICAgICAgICBpZiAoJ2Z1bmN0aW9uJyA9PSB0eXBlb2YgY2hpbGRDb21wb25lbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgdmFyIGNoaWxkUHJvcHMgPSBnZXROb2RlUHJvcHMocmVuZGVyZWQpO1xuICAgICAgICAgICAgICAgICAgICBpbnN0ID0gaW5pdGlhbENoaWxkQ29tcG9uZW50O1xuICAgICAgICAgICAgICAgICAgICBpZiAoaW5zdCAmJiBpbnN0LmNvbnN0cnVjdG9yID09PSBjaGlsZENvbXBvbmVudCAmJiBjaGlsZFByb3BzLmtleSA9PSBpbnN0Ll9faykgc2V0Q29tcG9uZW50UHJvcHMoaW5zdCwgY2hpbGRQcm9wcywgMSwgY29udGV4dCwgITEpOyBlbHNlIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIHRvVW5tb3VudCA9IGluc3Q7XG4gICAgICAgICAgICAgICAgICAgICAgICBjb21wb25lbnQuX2NvbXBvbmVudCA9IGluc3QgPSBjcmVhdGVDb21wb25lbnQoY2hpbGRDb21wb25lbnQsIGNoaWxkUHJvcHMsIGNvbnRleHQpO1xuICAgICAgICAgICAgICAgICAgICAgICAgaW5zdC5fX2IgPSBpbnN0Ll9fYiB8fCBuZXh0QmFzZTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGluc3QuX191ID0gY29tcG9uZW50O1xuICAgICAgICAgICAgICAgICAgICAgICAgc2V0Q29tcG9uZW50UHJvcHMoaW5zdCwgY2hpbGRQcm9wcywgMCwgY29udGV4dCwgITEpO1xuICAgICAgICAgICAgICAgICAgICAgICAgcmVuZGVyQ29tcG9uZW50KGluc3QsIDEsIG1vdW50QWxsLCAhMCk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgYmFzZSA9IGluc3QuYmFzZTtcbiAgICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgICAgICBjYmFzZSA9IGluaXRpYWxCYXNlO1xuICAgICAgICAgICAgICAgICAgICB0b1VubW91bnQgPSBpbml0aWFsQ2hpbGRDb21wb25lbnQ7XG4gICAgICAgICAgICAgICAgICAgIGlmICh0b1VubW91bnQpIGNiYXNlID0gY29tcG9uZW50Ll9jb21wb25lbnQgPSBudWxsO1xuICAgICAgICAgICAgICAgICAgICBpZiAoaW5pdGlhbEJhc2UgfHwgMSA9PT0gb3B0cykge1xuICAgICAgICAgICAgICAgICAgICAgICAgaWYgKGNiYXNlKSBjYmFzZS5fY29tcG9uZW50ID0gbnVsbDtcbiAgICAgICAgICAgICAgICAgICAgICAgIGJhc2UgPSBkaWZmKGNiYXNlLCByZW5kZXJlZCwgY29udGV4dCwgbW91bnRBbGwgfHwgIWlzVXBkYXRlLCBpbml0aWFsQmFzZSAmJiBpbml0aWFsQmFzZS5wYXJlbnROb2RlLCAhMCk7XG4gICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgaWYgKGluaXRpYWxCYXNlICYmIGJhc2UgIT09IGluaXRpYWxCYXNlICYmIGluc3QgIT09IGluaXRpYWxDaGlsZENvbXBvbmVudCkge1xuICAgICAgICAgICAgICAgICAgICB2YXIgYmFzZVBhcmVudCA9IGluaXRpYWxCYXNlLnBhcmVudE5vZGU7XG4gICAgICAgICAgICAgICAgICAgIGlmIChiYXNlUGFyZW50ICYmIGJhc2UgIT09IGJhc2VQYXJlbnQpIHtcbiAgICAgICAgICAgICAgICAgICAgICAgIGJhc2VQYXJlbnQucmVwbGFjZUNoaWxkKGJhc2UsIGluaXRpYWxCYXNlKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIGlmICghdG9Vbm1vdW50KSB7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgaW5pdGlhbEJhc2UuX2NvbXBvbmVudCA9IG51bGw7XG4gICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVjb2xsZWN0Tm9kZVRyZWUoaW5pdGlhbEJhc2UsICExKTtcbiAgICAgICAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBpZiAodG9Vbm1vdW50KSB1bm1vdW50Q29tcG9uZW50KHRvVW5tb3VudCk7XG4gICAgICAgICAgICAgICAgY29tcG9uZW50LmJhc2UgPSBiYXNlO1xuICAgICAgICAgICAgICAgIGlmIChiYXNlICYmICFpc0NoaWxkKSB7XG4gICAgICAgICAgICAgICAgICAgIHZhciBjb21wb25lbnRSZWYgPSBjb21wb25lbnQsIHQgPSBjb21wb25lbnQ7XG4gICAgICAgICAgICAgICAgICAgIHdoaWxlICh0ID0gdC5fX3UpIChjb21wb25lbnRSZWYgPSB0KS5iYXNlID0gYmFzZTtcbiAgICAgICAgICAgICAgICAgICAgYmFzZS5fY29tcG9uZW50ID0gY29tcG9uZW50UmVmO1xuICAgICAgICAgICAgICAgICAgICBiYXNlLl9jb21wb25lbnRDb25zdHJ1Y3RvciA9IGNvbXBvbmVudFJlZi5jb25zdHJ1Y3RvcjtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBpZiAoIWlzVXBkYXRlIHx8IG1vdW50QWxsKSBtb3VudHMudW5zaGlmdChjb21wb25lbnQpOyBlbHNlIGlmICghc2tpcCkge1xuICAgICAgICAgICAgICAgIGlmIChjb21wb25lbnQuY29tcG9uZW50RGlkVXBkYXRlKSBjb21wb25lbnQuY29tcG9uZW50RGlkVXBkYXRlKHByZXZpb3VzUHJvcHMsIHByZXZpb3VzU3RhdGUsIHByZXZpb3VzQ29udGV4dCk7XG4gICAgICAgICAgICAgICAgaWYgKG9wdGlvbnMuYWZ0ZXJVcGRhdGUpIG9wdGlvbnMuYWZ0ZXJVcGRhdGUoY29tcG9uZW50KTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGlmIChudWxsICE9IGNvbXBvbmVudC5fX2gpIHdoaWxlIChjb21wb25lbnQuX19oLmxlbmd0aCkgY29tcG9uZW50Ll9faC5wb3AoKS5jYWxsKGNvbXBvbmVudCk7XG4gICAgICAgICAgICBpZiAoIWRpZmZMZXZlbCAmJiAhaXNDaGlsZCkgZmx1c2hNb3VudHMoKTtcbiAgICAgICAgfVxuICAgIH1cbiAgICBmdW5jdGlvbiBidWlsZENvbXBvbmVudEZyb21WTm9kZShkb20sIHZub2RlLCBjb250ZXh0LCBtb3VudEFsbCkge1xuICAgICAgICB2YXIgYyA9IGRvbSAmJiBkb20uX2NvbXBvbmVudCwgb3JpZ2luYWxDb21wb25lbnQgPSBjLCBvbGREb20gPSBkb20sIGlzRGlyZWN0T3duZXIgPSBjICYmIGRvbS5fY29tcG9uZW50Q29uc3RydWN0b3IgPT09IHZub2RlLm5vZGVOYW1lLCBpc093bmVyID0gaXNEaXJlY3RPd25lciwgcHJvcHMgPSBnZXROb2RlUHJvcHModm5vZGUpO1xuICAgICAgICB3aGlsZSAoYyAmJiAhaXNPd25lciAmJiAoYyA9IGMuX191KSkgaXNPd25lciA9IGMuY29uc3RydWN0b3IgPT09IHZub2RlLm5vZGVOYW1lO1xuICAgICAgICBpZiAoYyAmJiBpc093bmVyICYmICghbW91bnRBbGwgfHwgYy5fY29tcG9uZW50KSkge1xuICAgICAgICAgICAgc2V0Q29tcG9uZW50UHJvcHMoYywgcHJvcHMsIDMsIGNvbnRleHQsIG1vdW50QWxsKTtcbiAgICAgICAgICAgIGRvbSA9IGMuYmFzZTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGlmIChvcmlnaW5hbENvbXBvbmVudCAmJiAhaXNEaXJlY3RPd25lcikge1xuICAgICAgICAgICAgICAgIHVubW91bnRDb21wb25lbnQob3JpZ2luYWxDb21wb25lbnQpO1xuICAgICAgICAgICAgICAgIGRvbSA9IG9sZERvbSA9IG51bGw7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBjID0gY3JlYXRlQ29tcG9uZW50KHZub2RlLm5vZGVOYW1lLCBwcm9wcywgY29udGV4dCk7XG4gICAgICAgICAgICBpZiAoZG9tICYmICFjLl9fYikge1xuICAgICAgICAgICAgICAgIGMuX19iID0gZG9tO1xuICAgICAgICAgICAgICAgIG9sZERvbSA9IG51bGw7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBzZXRDb21wb25lbnRQcm9wcyhjLCBwcm9wcywgMSwgY29udGV4dCwgbW91bnRBbGwpO1xuICAgICAgICAgICAgZG9tID0gYy5iYXNlO1xuICAgICAgICAgICAgaWYgKG9sZERvbSAmJiBkb20gIT09IG9sZERvbSkge1xuICAgICAgICAgICAgICAgIG9sZERvbS5fY29tcG9uZW50ID0gbnVsbDtcbiAgICAgICAgICAgICAgICByZWNvbGxlY3ROb2RlVHJlZShvbGREb20sICExKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gZG9tO1xuICAgIH1cbiAgICBmdW5jdGlvbiB1bm1vdW50Q29tcG9uZW50KGNvbXBvbmVudCkge1xuICAgICAgICBpZiAob3B0aW9ucy5iZWZvcmVVbm1vdW50KSBvcHRpb25zLmJlZm9yZVVubW91bnQoY29tcG9uZW50KTtcbiAgICAgICAgdmFyIGJhc2UgPSBjb21wb25lbnQuYmFzZTtcbiAgICAgICAgY29tcG9uZW50Ll9feCA9ICEwO1xuICAgICAgICBpZiAoY29tcG9uZW50LmNvbXBvbmVudFdpbGxVbm1vdW50KSBjb21wb25lbnQuY29tcG9uZW50V2lsbFVubW91bnQoKTtcbiAgICAgICAgY29tcG9uZW50LmJhc2UgPSBudWxsO1xuICAgICAgICB2YXIgaW5uZXIgPSBjb21wb25lbnQuX2NvbXBvbmVudDtcbiAgICAgICAgaWYgKGlubmVyKSB1bm1vdW50Q29tcG9uZW50KGlubmVyKTsgZWxzZSBpZiAoYmFzZSkge1xuICAgICAgICAgICAgaWYgKGJhc2UuX19wcmVhY3RhdHRyXyAmJiBiYXNlLl9fcHJlYWN0YXR0cl8ucmVmKSBiYXNlLl9fcHJlYWN0YXR0cl8ucmVmKG51bGwpO1xuICAgICAgICAgICAgY29tcG9uZW50Ll9fYiA9IGJhc2U7XG4gICAgICAgICAgICByZW1vdmVOb2RlKGJhc2UpO1xuICAgICAgICAgICAgY29sbGVjdENvbXBvbmVudChjb21wb25lbnQpO1xuICAgICAgICAgICAgcmVtb3ZlQ2hpbGRyZW4oYmFzZSk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGNvbXBvbmVudC5fX3IpIGNvbXBvbmVudC5fX3IobnVsbCk7XG4gICAgfVxuICAgIGZ1bmN0aW9uIENvbXBvbmVudChwcm9wcywgY29udGV4dCkge1xuICAgICAgICB0aGlzLl9fZCA9ICEwO1xuICAgICAgICB0aGlzLmNvbnRleHQgPSBjb250ZXh0O1xuICAgICAgICB0aGlzLnByb3BzID0gcHJvcHM7XG4gICAgICAgIHRoaXMuc3RhdGUgPSB0aGlzLnN0YXRlIHx8IHt9O1xuICAgIH1cbiAgICBmdW5jdGlvbiByZW5kZXIodm5vZGUsIHBhcmVudCwgbWVyZ2UpIHtcbiAgICAgICAgcmV0dXJuIGRpZmYobWVyZ2UsIHZub2RlLCB7fSwgITEsIHBhcmVudCwgITEpO1xuICAgIH1cbiAgICB2YXIgb3B0aW9ucyA9IHt9O1xuICAgIHZhciBzdGFjayA9IFtdO1xuICAgIHZhciBFTVBUWV9DSElMRFJFTiA9IFtdO1xuICAgIHZhciBkZWZlciA9ICdmdW5jdGlvbicgPT0gdHlwZW9mIFByb21pc2UgPyBQcm9taXNlLnJlc29sdmUoKS50aGVuLmJpbmQoUHJvbWlzZS5yZXNvbHZlKCkpIDogc2V0VGltZW91dDtcbiAgICB2YXIgSVNfTk9OX0RJTUVOU0lPTkFMID0gL2FjaXR8ZXgoPzpzfGd8bnxwfCQpfHJwaHxvd3N8bW5jfG50d3xpbmVbY2hdfHpvb3xeb3JkL2k7XG4gICAgdmFyIGl0ZW1zID0gW107XG4gICAgdmFyIG1vdW50cyA9IFtdO1xuICAgIHZhciBkaWZmTGV2ZWwgPSAwO1xuICAgIHZhciBpc1N2Z01vZGUgPSAhMTtcbiAgICB2YXIgaHlkcmF0aW5nID0gITE7XG4gICAgdmFyIGNvbXBvbmVudHMgPSB7fTtcbiAgICBleHRlbmQoQ29tcG9uZW50LnByb3RvdHlwZSwge1xuICAgICAgICBzZXRTdGF0ZTogZnVuY3Rpb24oc3RhdGUsIGNhbGxiYWNrKSB7XG4gICAgICAgICAgICB2YXIgcyA9IHRoaXMuc3RhdGU7XG4gICAgICAgICAgICBpZiAoIXRoaXMuX19zKSB0aGlzLl9fcyA9IGV4dGVuZCh7fSwgcyk7XG4gICAgICAgICAgICBleHRlbmQocywgJ2Z1bmN0aW9uJyA9PSB0eXBlb2Ygc3RhdGUgPyBzdGF0ZShzLCB0aGlzLnByb3BzKSA6IHN0YXRlKTtcbiAgICAgICAgICAgIGlmIChjYWxsYmFjaykgKHRoaXMuX19oID0gdGhpcy5fX2ggfHwgW10pLnB1c2goY2FsbGJhY2spO1xuICAgICAgICAgICAgZW5xdWV1ZVJlbmRlcih0aGlzKTtcbiAgICAgICAgfSxcbiAgICAgICAgZm9yY2VVcGRhdGU6IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICAgICAgICBpZiAoY2FsbGJhY2spICh0aGlzLl9faCA9IHRoaXMuX19oIHx8IFtdKS5wdXNoKGNhbGxiYWNrKTtcbiAgICAgICAgICAgIHJlbmRlckNvbXBvbmVudCh0aGlzLCAyKTtcbiAgICAgICAgfSxcbiAgICAgICAgcmVuZGVyOiBmdW5jdGlvbigpIHt9XG4gICAgfSk7XG4gICAgdmFyIHByZWFjdCA9IHtcbiAgICAgICAgaDogaCxcbiAgICAgICAgY3JlYXRlRWxlbWVudDogaCxcbiAgICAgICAgY2xvbmVFbGVtZW50OiBjbG9uZUVsZW1lbnQsXG4gICAgICAgIENvbXBvbmVudDogQ29tcG9uZW50LFxuICAgICAgICByZW5kZXI6IHJlbmRlcixcbiAgICAgICAgcmVyZW5kZXI6IHJlcmVuZGVyLFxuICAgICAgICBvcHRpb25zOiBvcHRpb25zXG4gICAgfTtcbiAgICBpZiAoJ3VuZGVmaW5lZCcgIT0gdHlwZW9mIG1vZHVsZSkgbW9kdWxlLmV4cG9ydHMgPSBwcmVhY3Q7IGVsc2Ugc2VsZi5wcmVhY3QgPSBwcmVhY3Q7XG59KCk7XG4vLyMgc291cmNlTWFwcGluZ1VSTD1wcmVhY3QuanMubWFwIiwiLy8gc2hpbSBmb3IgdXNpbmcgcHJvY2VzcyBpbiBicm93c2VyXG52YXIgcHJvY2VzcyA9IG1vZHVsZS5leHBvcnRzID0ge307XG5cbi8vIGNhY2hlZCBmcm9tIHdoYXRldmVyIGdsb2JhbCBpcyBwcmVzZW50IHNvIHRoYXQgdGVzdCBydW5uZXJzIHRoYXQgc3R1YiBpdFxuLy8gZG9uJ3QgYnJlYWsgdGhpbmdzLiAgQnV0IHdlIG5lZWQgdG8gd3JhcCBpdCBpbiBhIHRyeSBjYXRjaCBpbiBjYXNlIGl0IGlzXG4vLyB3cmFwcGVkIGluIHN0cmljdCBtb2RlIGNvZGUgd2hpY2ggZG9lc24ndCBkZWZpbmUgYW55IGdsb2JhbHMuICBJdCdzIGluc2lkZSBhXG4vLyBmdW5jdGlvbiBiZWNhdXNlIHRyeS9jYXRjaGVzIGRlb3B0aW1pemUgaW4gY2VydGFpbiBlbmdpbmVzLlxuXG52YXIgY2FjaGVkU2V0VGltZW91dDtcbnZhciBjYWNoZWRDbGVhclRpbWVvdXQ7XG5cbmZ1bmN0aW9uIGRlZmF1bHRTZXRUaW1vdXQoKSB7XG4gICAgdGhyb3cgbmV3IEVycm9yKCdzZXRUaW1lb3V0IGhhcyBub3QgYmVlbiBkZWZpbmVkJyk7XG59XG5mdW5jdGlvbiBkZWZhdWx0Q2xlYXJUaW1lb3V0ICgpIHtcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ2NsZWFyVGltZW91dCBoYXMgbm90IGJlZW4gZGVmaW5lZCcpO1xufVxuKGZ1bmN0aW9uICgpIHtcbiAgICB0cnkge1xuICAgICAgICBpZiAodHlwZW9mIHNldFRpbWVvdXQgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgICAgIGNhY2hlZFNldFRpbWVvdXQgPSBzZXRUaW1lb3V0O1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY2FjaGVkU2V0VGltZW91dCA9IGRlZmF1bHRTZXRUaW1vdXQ7XG4gICAgICAgIH1cbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgIGNhY2hlZFNldFRpbWVvdXQgPSBkZWZhdWx0U2V0VGltb3V0O1xuICAgIH1cbiAgICB0cnkge1xuICAgICAgICBpZiAodHlwZW9mIGNsZWFyVGltZW91dCA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICAgICAgY2FjaGVkQ2xlYXJUaW1lb3V0ID0gY2xlYXJUaW1lb3V0O1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY2FjaGVkQ2xlYXJUaW1lb3V0ID0gZGVmYXVsdENsZWFyVGltZW91dDtcbiAgICAgICAgfVxuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgY2FjaGVkQ2xlYXJUaW1lb3V0ID0gZGVmYXVsdENsZWFyVGltZW91dDtcbiAgICB9XG59ICgpKVxuZnVuY3Rpb24gcnVuVGltZW91dChmdW4pIHtcbiAgICBpZiAoY2FjaGVkU2V0VGltZW91dCA9PT0gc2V0VGltZW91dCkge1xuICAgICAgICAvL25vcm1hbCBlbnZpcm9tZW50cyBpbiBzYW5lIHNpdHVhdGlvbnNcbiAgICAgICAgcmV0dXJuIHNldFRpbWVvdXQoZnVuLCAwKTtcbiAgICB9XG4gICAgLy8gaWYgc2V0VGltZW91dCB3YXNuJ3QgYXZhaWxhYmxlIGJ1dCB3YXMgbGF0dGVyIGRlZmluZWRcbiAgICBpZiAoKGNhY2hlZFNldFRpbWVvdXQgPT09IGRlZmF1bHRTZXRUaW1vdXQgfHwgIWNhY2hlZFNldFRpbWVvdXQpICYmIHNldFRpbWVvdXQpIHtcbiAgICAgICAgY2FjaGVkU2V0VGltZW91dCA9IHNldFRpbWVvdXQ7XG4gICAgICAgIHJldHVybiBzZXRUaW1lb3V0KGZ1biwgMCk7XG4gICAgfVxuICAgIHRyeSB7XG4gICAgICAgIC8vIHdoZW4gd2hlbiBzb21lYm9keSBoYXMgc2NyZXdlZCB3aXRoIHNldFRpbWVvdXQgYnV0IG5vIEkuRS4gbWFkZG5lc3NcbiAgICAgICAgcmV0dXJuIGNhY2hlZFNldFRpbWVvdXQoZnVuLCAwKTtcbiAgICB9IGNhdGNoKGUpe1xuICAgICAgICB0cnkge1xuICAgICAgICAgICAgLy8gV2hlbiB3ZSBhcmUgaW4gSS5FLiBidXQgdGhlIHNjcmlwdCBoYXMgYmVlbiBldmFsZWQgc28gSS5FLiBkb2Vzbid0IHRydXN0IHRoZSBnbG9iYWwgb2JqZWN0IHdoZW4gY2FsbGVkIG5vcm1hbGx5XG4gICAgICAgICAgICByZXR1cm4gY2FjaGVkU2V0VGltZW91dC5jYWxsKG51bGwsIGZ1biwgMCk7XG4gICAgICAgIH0gY2F0Y2goZSl7XG4gICAgICAgICAgICAvLyBzYW1lIGFzIGFib3ZlIGJ1dCB3aGVuIGl0J3MgYSB2ZXJzaW9uIG9mIEkuRS4gdGhhdCBtdXN0IGhhdmUgdGhlIGdsb2JhbCBvYmplY3QgZm9yICd0aGlzJywgaG9wZnVsbHkgb3VyIGNvbnRleHQgY29ycmVjdCBvdGhlcndpc2UgaXQgd2lsbCB0aHJvdyBhIGdsb2JhbCBlcnJvclxuICAgICAgICAgICAgcmV0dXJuIGNhY2hlZFNldFRpbWVvdXQuY2FsbCh0aGlzLCBmdW4sIDApO1xuICAgICAgICB9XG4gICAgfVxuXG5cbn1cbmZ1bmN0aW9uIHJ1bkNsZWFyVGltZW91dChtYXJrZXIpIHtcbiAgICBpZiAoY2FjaGVkQ2xlYXJUaW1lb3V0ID09PSBjbGVhclRpbWVvdXQpIHtcbiAgICAgICAgLy9ub3JtYWwgZW52aXJvbWVudHMgaW4gc2FuZSBzaXR1YXRpb25zXG4gICAgICAgIHJldHVybiBjbGVhclRpbWVvdXQobWFya2VyKTtcbiAgICB9XG4gICAgLy8gaWYgY2xlYXJUaW1lb3V0IHdhc24ndCBhdmFpbGFibGUgYnV0IHdhcyBsYXR0ZXIgZGVmaW5lZFxuICAgIGlmICgoY2FjaGVkQ2xlYXJUaW1lb3V0ID09PSBkZWZhdWx0Q2xlYXJUaW1lb3V0IHx8ICFjYWNoZWRDbGVhclRpbWVvdXQpICYmIGNsZWFyVGltZW91dCkge1xuICAgICAgICBjYWNoZWRDbGVhclRpbWVvdXQgPSBjbGVhclRpbWVvdXQ7XG4gICAgICAgIHJldHVybiBjbGVhclRpbWVvdXQobWFya2VyKTtcbiAgICB9XG4gICAgdHJ5IHtcbiAgICAgICAgLy8gd2hlbiB3aGVuIHNvbWVib2R5IGhhcyBzY3Jld2VkIHdpdGggc2V0VGltZW91dCBidXQgbm8gSS5FLiBtYWRkbmVzc1xuICAgICAgICByZXR1cm4gY2FjaGVkQ2xlYXJUaW1lb3V0KG1hcmtlcik7XG4gICAgfSBjYXRjaCAoZSl7XG4gICAgICAgIHRyeSB7XG4gICAgICAgICAgICAvLyBXaGVuIHdlIGFyZSBpbiBJLkUuIGJ1dCB0aGUgc2NyaXB0IGhhcyBiZWVuIGV2YWxlZCBzbyBJLkUuIGRvZXNuJ3QgIHRydXN0IHRoZSBnbG9iYWwgb2JqZWN0IHdoZW4gY2FsbGVkIG5vcm1hbGx5XG4gICAgICAgICAgICByZXR1cm4gY2FjaGVkQ2xlYXJUaW1lb3V0LmNhbGwobnVsbCwgbWFya2VyKTtcbiAgICAgICAgfSBjYXRjaCAoZSl7XG4gICAgICAgICAgICAvLyBzYW1lIGFzIGFib3ZlIGJ1dCB3aGVuIGl0J3MgYSB2ZXJzaW9uIG9mIEkuRS4gdGhhdCBtdXN0IGhhdmUgdGhlIGdsb2JhbCBvYmplY3QgZm9yICd0aGlzJywgaG9wZnVsbHkgb3VyIGNvbnRleHQgY29ycmVjdCBvdGhlcndpc2UgaXQgd2lsbCB0aHJvdyBhIGdsb2JhbCBlcnJvci5cbiAgICAgICAgICAgIC8vIFNvbWUgdmVyc2lvbnMgb2YgSS5FLiBoYXZlIGRpZmZlcmVudCBydWxlcyBmb3IgY2xlYXJUaW1lb3V0IHZzIHNldFRpbWVvdXRcbiAgICAgICAgICAgIHJldHVybiBjYWNoZWRDbGVhclRpbWVvdXQuY2FsbCh0aGlzLCBtYXJrZXIpO1xuICAgICAgICB9XG4gICAgfVxuXG5cblxufVxudmFyIHF1ZXVlID0gW107XG52YXIgZHJhaW5pbmcgPSBmYWxzZTtcbnZhciBjdXJyZW50UXVldWU7XG52YXIgcXVldWVJbmRleCA9IC0xO1xuXG5mdW5jdGlvbiBjbGVhblVwTmV4dFRpY2soKSB7XG4gICAgaWYgKCFkcmFpbmluZyB8fCAhY3VycmVudFF1ZXVlKSB7XG4gICAgICAgIHJldHVybjtcbiAgICB9XG4gICAgZHJhaW5pbmcgPSBmYWxzZTtcbiAgICBpZiAoY3VycmVudFF1ZXVlLmxlbmd0aCkge1xuICAgICAgICBxdWV1ZSA9IGN1cnJlbnRRdWV1ZS5jb25jYXQocXVldWUpO1xuICAgIH0gZWxzZSB7XG4gICAgICAgIHF1ZXVlSW5kZXggPSAtMTtcbiAgICB9XG4gICAgaWYgKHF1ZXVlLmxlbmd0aCkge1xuICAgICAgICBkcmFpblF1ZXVlKCk7XG4gICAgfVxufVxuXG5mdW5jdGlvbiBkcmFpblF1ZXVlKCkge1xuICAgIGlmIChkcmFpbmluZykge1xuICAgICAgICByZXR1cm47XG4gICAgfVxuICAgIHZhciB0aW1lb3V0ID0gcnVuVGltZW91dChjbGVhblVwTmV4dFRpY2spO1xuICAgIGRyYWluaW5nID0gdHJ1ZTtcblxuICAgIHZhciBsZW4gPSBxdWV1ZS5sZW5ndGg7XG4gICAgd2hpbGUobGVuKSB7XG4gICAgICAgIGN1cnJlbnRRdWV1ZSA9IHF1ZXVlO1xuICAgICAgICBxdWV1ZSA9IFtdO1xuICAgICAgICB3aGlsZSAoKytxdWV1ZUluZGV4IDwgbGVuKSB7XG4gICAgICAgICAgICBpZiAoY3VycmVudFF1ZXVlKSB7XG4gICAgICAgICAgICAgICAgY3VycmVudFF1ZXVlW3F1ZXVlSW5kZXhdLnJ1bigpO1xuICAgICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIHF1ZXVlSW5kZXggPSAtMTtcbiAgICAgICAgbGVuID0gcXVldWUubGVuZ3RoO1xuICAgIH1cbiAgICBjdXJyZW50UXVldWUgPSBudWxsO1xuICAgIGRyYWluaW5nID0gZmFsc2U7XG4gICAgcnVuQ2xlYXJUaW1lb3V0KHRpbWVvdXQpO1xufVxuXG5wcm9jZXNzLm5leHRUaWNrID0gZnVuY3Rpb24gKGZ1bikge1xuICAgIHZhciBhcmdzID0gbmV3IEFycmF5KGFyZ3VtZW50cy5sZW5ndGggLSAxKTtcbiAgICBpZiAoYXJndW1lbnRzLmxlbmd0aCA+IDEpIHtcbiAgICAgICAgZm9yICh2YXIgaSA9IDE7IGkgPCBhcmd1bWVudHMubGVuZ3RoOyBpKyspIHtcbiAgICAgICAgICAgIGFyZ3NbaSAtIDFdID0gYXJndW1lbnRzW2ldO1xuICAgICAgICB9XG4gICAgfVxuICAgIHF1ZXVlLnB1c2gobmV3IEl0ZW0oZnVuLCBhcmdzKSk7XG4gICAgaWYgKHF1ZXVlLmxlbmd0aCA9PT0gMSAmJiAhZHJhaW5pbmcpIHtcbiAgICAgICAgcnVuVGltZW91dChkcmFpblF1ZXVlKTtcbiAgICB9XG59O1xuXG4vLyB2OCBsaWtlcyBwcmVkaWN0aWJsZSBvYmplY3RzXG5mdW5jdGlvbiBJdGVtKGZ1biwgYXJyYXkpIHtcbiAgICB0aGlzLmZ1biA9IGZ1bjtcbiAgICB0aGlzLmFycmF5ID0gYXJyYXk7XG59XG5JdGVtLnByb3RvdHlwZS5ydW4gPSBmdW5jdGlvbiAoKSB7XG4gICAgdGhpcy5mdW4uYXBwbHkobnVsbCwgdGhpcy5hcnJheSk7XG59O1xucHJvY2Vzcy50aXRsZSA9ICdicm93c2VyJztcbnByb2Nlc3MuYnJvd3NlciA9IHRydWU7XG5wcm9jZXNzLmVudiA9IHt9O1xucHJvY2Vzcy5hcmd2ID0gW107XG5wcm9jZXNzLnZlcnNpb24gPSAnJzsgLy8gZW1wdHkgc3RyaW5nIHRvIGF2b2lkIHJlZ2V4cCBpc3N1ZXNcbnByb2Nlc3MudmVyc2lvbnMgPSB7fTtcblxuZnVuY3Rpb24gbm9vcCgpIHt9XG5cbnByb2Nlc3Mub24gPSBub29wO1xucHJvY2Vzcy5hZGRMaXN0ZW5lciA9IG5vb3A7XG5wcm9jZXNzLm9uY2UgPSBub29wO1xucHJvY2Vzcy5vZmYgPSBub29wO1xucHJvY2Vzcy5yZW1vdmVMaXN0ZW5lciA9IG5vb3A7XG5wcm9jZXNzLnJlbW92ZUFsbExpc3RlbmVycyA9IG5vb3A7XG5wcm9jZXNzLmVtaXQgPSBub29wO1xucHJvY2Vzcy5wcmVwZW5kTGlzdGVuZXIgPSBub29wO1xucHJvY2Vzcy5wcmVwZW5kT25jZUxpc3RlbmVyID0gbm9vcDtcblxucHJvY2Vzcy5saXN0ZW5lcnMgPSBmdW5jdGlvbiAobmFtZSkgeyByZXR1cm4gW10gfVxuXG5wcm9jZXNzLmJpbmRpbmcgPSBmdW5jdGlvbiAobmFtZSkge1xuICAgIHRocm93IG5ldyBFcnJvcigncHJvY2Vzcy5iaW5kaW5nIGlzIG5vdCBzdXBwb3J0ZWQnKTtcbn07XG5cbnByb2Nlc3MuY3dkID0gZnVuY3Rpb24gKCkgeyByZXR1cm4gJy8nIH07XG5wcm9jZXNzLmNoZGlyID0gZnVuY3Rpb24gKGRpcikge1xuICAgIHRocm93IG5ldyBFcnJvcigncHJvY2Vzcy5jaGRpciBpcyBub3Qgc3VwcG9ydGVkJyk7XG59O1xucHJvY2Vzcy51bWFzayA9IGZ1bmN0aW9uKCkgeyByZXR1cm4gMDsgfTtcbiIsIid1c2Ugc3RyaWN0JztcblxudmFyIGhhcyA9IE9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHlcbiAgLCB1bmRlZjtcblxuLyoqXG4gKiBEZWNvZGUgYSBVUkkgZW5jb2RlZCBzdHJpbmcuXG4gKlxuICogQHBhcmFtIHtTdHJpbmd9IGlucHV0IFRoZSBVUkkgZW5jb2RlZCBzdHJpbmcuXG4gKiBAcmV0dXJucyB7U3RyaW5nfE51bGx9IFRoZSBkZWNvZGVkIHN0cmluZy5cbiAqIEBhcGkgcHJpdmF0ZVxuICovXG5mdW5jdGlvbiBkZWNvZGUoaW5wdXQpIHtcbiAgdHJ5IHtcbiAgICByZXR1cm4gZGVjb2RlVVJJQ29tcG9uZW50KGlucHV0LnJlcGxhY2UoL1xcKy9nLCAnICcpKTtcbiAgfSBjYXRjaCAoZSkge1xuICAgIHJldHVybiBudWxsO1xuICB9XG59XG5cbi8qKlxuICogQXR0ZW1wdHMgdG8gZW5jb2RlIGEgZ2l2ZW4gaW5wdXQuXG4gKlxuICogQHBhcmFtIHtTdHJpbmd9IGlucHV0IFRoZSBzdHJpbmcgdGhhdCBuZWVkcyB0byBiZSBlbmNvZGVkLlxuICogQHJldHVybnMge1N0cmluZ3xOdWxsfSBUaGUgZW5jb2RlZCBzdHJpbmcuXG4gKiBAYXBpIHByaXZhdGVcbiAqL1xuZnVuY3Rpb24gZW5jb2RlKGlucHV0KSB7XG4gIHRyeSB7XG4gICAgcmV0dXJuIGVuY29kZVVSSUNvbXBvbmVudChpbnB1dCk7XG4gIH0gY2F0Y2ggKGUpIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxufVxuXG4vKipcbiAqIFNpbXBsZSBxdWVyeSBzdHJpbmcgcGFyc2VyLlxuICpcbiAqIEBwYXJhbSB7U3RyaW5nfSBxdWVyeSBUaGUgcXVlcnkgc3RyaW5nIHRoYXQgbmVlZHMgdG8gYmUgcGFyc2VkLlxuICogQHJldHVybnMge09iamVjdH1cbiAqIEBhcGkgcHVibGljXG4gKi9cbmZ1bmN0aW9uIHF1ZXJ5c3RyaW5nKHF1ZXJ5KSB7XG4gIHZhciBwYXJzZXIgPSAvKFtePT8mXSspPT8oW14mXSopL2dcbiAgICAsIHJlc3VsdCA9IHt9XG4gICAgLCBwYXJ0O1xuXG4gIHdoaWxlIChwYXJ0ID0gcGFyc2VyLmV4ZWMocXVlcnkpKSB7XG4gICAgdmFyIGtleSA9IGRlY29kZShwYXJ0WzFdKVxuICAgICAgLCB2YWx1ZSA9IGRlY29kZShwYXJ0WzJdKTtcblxuICAgIC8vXG4gICAgLy8gUHJldmVudCBvdmVycmlkaW5nIG9mIGV4aXN0aW5nIHByb3BlcnRpZXMuIFRoaXMgZW5zdXJlcyB0aGF0IGJ1aWxkLWluXG4gICAgLy8gbWV0aG9kcyBsaWtlIGB0b1N0cmluZ2Agb3IgX19wcm90b19fIGFyZSBub3Qgb3ZlcnJpZGVuIGJ5IG1hbGljaW91c1xuICAgIC8vIHF1ZXJ5c3RyaW5ncy5cbiAgICAvL1xuICAgIC8vIEluIHRoZSBjYXNlIGlmIGZhaWxlZCBkZWNvZGluZywgd2Ugd2FudCB0byBvbWl0IHRoZSBrZXkvdmFsdWUgcGFpcnNcbiAgICAvLyBmcm9tIHRoZSByZXN1bHQuXG4gICAgLy9cbiAgICBpZiAoa2V5ID09PSBudWxsIHx8IHZhbHVlID09PSBudWxsIHx8IGtleSBpbiByZXN1bHQpIGNvbnRpbnVlO1xuICAgIHJlc3VsdFtrZXldID0gdmFsdWU7XG4gIH1cblxuICByZXR1cm4gcmVzdWx0O1xufVxuXG4vKipcbiAqIFRyYW5zZm9ybSBhIHF1ZXJ5IHN0cmluZyB0byBhbiBvYmplY3QuXG4gKlxuICogQHBhcmFtIHtPYmplY3R9IG9iaiBPYmplY3QgdGhhdCBzaG91bGQgYmUgdHJhbnNmb3JtZWQuXG4gKiBAcGFyYW0ge1N0cmluZ30gcHJlZml4IE9wdGlvbmFsIHByZWZpeC5cbiAqIEByZXR1cm5zIHtTdHJpbmd9XG4gKiBAYXBpIHB1YmxpY1xuICovXG5mdW5jdGlvbiBxdWVyeXN0cmluZ2lmeShvYmosIHByZWZpeCkge1xuICBwcmVmaXggPSBwcmVmaXggfHwgJyc7XG5cbiAgdmFyIHBhaXJzID0gW11cbiAgICAsIHZhbHVlXG4gICAgLCBrZXk7XG5cbiAgLy9cbiAgLy8gT3B0aW9uYWxseSBwcmVmaXggd2l0aCBhICc/JyBpZiBuZWVkZWRcbiAgLy9cbiAgaWYgKCdzdHJpbmcnICE9PSB0eXBlb2YgcHJlZml4KSBwcmVmaXggPSAnPyc7XG5cbiAgZm9yIChrZXkgaW4gb2JqKSB7XG4gICAgaWYgKGhhcy5jYWxsKG9iaiwga2V5KSkge1xuICAgICAgdmFsdWUgPSBvYmpba2V5XTtcblxuICAgICAgLy9cbiAgICAgIC8vIEVkZ2UgY2FzZXMgd2hlcmUgd2UgYWN0dWFsbHkgd2FudCB0byBlbmNvZGUgdGhlIHZhbHVlIHRvIGFuIGVtcHR5XG4gICAgICAvLyBzdHJpbmcgaW5zdGVhZCBvZiB0aGUgc3RyaW5naWZpZWQgdmFsdWUuXG4gICAgICAvL1xuICAgICAgaWYgKCF2YWx1ZSAmJiAodmFsdWUgPT09IG51bGwgfHwgdmFsdWUgPT09IHVuZGVmIHx8IGlzTmFOKHZhbHVlKSkpIHtcbiAgICAgICAgdmFsdWUgPSAnJztcbiAgICAgIH1cblxuICAgICAga2V5ID0gZW5jb2RlVVJJQ29tcG9uZW50KGtleSk7XG4gICAgICB2YWx1ZSA9IGVuY29kZVVSSUNvbXBvbmVudCh2YWx1ZSk7XG5cbiAgICAgIC8vXG4gICAgICAvLyBJZiB3ZSBmYWlsZWQgdG8gZW5jb2RlIHRoZSBzdHJpbmdzLCB3ZSBzaG91bGQgYmFpbCBvdXQgYXMgd2UgZG9uJ3RcbiAgICAgIC8vIHdhbnQgdG8gYWRkIGludmFsaWQgc3RyaW5ncyB0byB0aGUgcXVlcnkuXG4gICAgICAvL1xuICAgICAgaWYgKGtleSA9PT0gbnVsbCB8fCB2YWx1ZSA9PT0gbnVsbCkgY29udGludWU7XG4gICAgICBwYWlycy5wdXNoKGtleSArJz0nKyB2YWx1ZSk7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIHBhaXJzLmxlbmd0aCA/IHByZWZpeCArIHBhaXJzLmpvaW4oJyYnKSA6ICcnO1xufVxuXG4vL1xuLy8gRXhwb3NlIHRoZSBtb2R1bGUuXG4vL1xuZXhwb3J0cy5zdHJpbmdpZnkgPSBxdWVyeXN0cmluZ2lmeTtcbmV4cG9ydHMucGFyc2UgPSBxdWVyeXN0cmluZztcbiIsIid1c2Ugc3RyaWN0JztcblxuLyoqXG4gKiBDaGVjayBpZiB3ZSdyZSByZXF1aXJlZCB0byBhZGQgYSBwb3J0IG51bWJlci5cbiAqXG4gKiBAc2VlIGh0dHBzOi8vdXJsLnNwZWMud2hhdHdnLm9yZy8jZGVmYXVsdC1wb3J0XG4gKiBAcGFyYW0ge051bWJlcnxTdHJpbmd9IHBvcnQgUG9ydCBudW1iZXIgd2UgbmVlZCB0byBjaGVja1xuICogQHBhcmFtIHtTdHJpbmd9IHByb3RvY29sIFByb3RvY29sIHdlIG5lZWQgdG8gY2hlY2sgYWdhaW5zdC5cbiAqIEByZXR1cm5zIHtCb29sZWFufSBJcyBpdCBhIGRlZmF1bHQgcG9ydCBmb3IgdGhlIGdpdmVuIHByb3RvY29sXG4gKiBAYXBpIHByaXZhdGVcbiAqL1xubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiByZXF1aXJlZChwb3J0LCBwcm90b2NvbCkge1xuICBwcm90b2NvbCA9IHByb3RvY29sLnNwbGl0KCc6JylbMF07XG4gIHBvcnQgPSArcG9ydDtcblxuICBpZiAoIXBvcnQpIHJldHVybiBmYWxzZTtcblxuICBzd2l0Y2ggKHByb3RvY29sKSB7XG4gICAgY2FzZSAnaHR0cCc6XG4gICAgY2FzZSAnd3MnOlxuICAgIHJldHVybiBwb3J0ICE9PSA4MDtcblxuICAgIGNhc2UgJ2h0dHBzJzpcbiAgICBjYXNlICd3c3MnOlxuICAgIHJldHVybiBwb3J0ICE9PSA0NDM7XG5cbiAgICBjYXNlICdmdHAnOlxuICAgIHJldHVybiBwb3J0ICE9PSAyMTtcblxuICAgIGNhc2UgJ2dvcGhlcic6XG4gICAgcmV0dXJuIHBvcnQgIT09IDcwO1xuXG4gICAgY2FzZSAnZmlsZSc6XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgcmV0dXJuIHBvcnQgIT09IDA7XG59O1xuIiwiJ3VzZSBzdHJpY3QnO1xuXG52YXIgcmVxdWlyZWQgPSByZXF1aXJlKCdyZXF1aXJlcy1wb3J0JylcbiAgLCBxcyA9IHJlcXVpcmUoJ3F1ZXJ5c3RyaW5naWZ5JylcbiAgLCBzbGFzaGVzID0gL15bQS1aYS16XVtBLVphLXowLTkrLS5dKjpcXC9cXC8vXG4gICwgcHJvdG9jb2xyZSA9IC9eKFthLXpdW2EtejAtOS4rLV0qOik/KFxcL1xcLyk/KFtcXFNcXHNdKikvaVxuICAsIHdoaXRlc3BhY2UgPSAnW1xcXFx4MDlcXFxceDBBXFxcXHgwQlxcXFx4MENcXFxceDBEXFxcXHgyMFxcXFx4QTBcXFxcdTE2ODBcXFxcdTE4MEVcXFxcdTIwMDBcXFxcdTIwMDFcXFxcdTIwMDJcXFxcdTIwMDNcXFxcdTIwMDRcXFxcdTIwMDVcXFxcdTIwMDZcXFxcdTIwMDdcXFxcdTIwMDhcXFxcdTIwMDlcXFxcdTIwMEFcXFxcdTIwMkZcXFxcdTIwNUZcXFxcdTMwMDBcXFxcdTIwMjhcXFxcdTIwMjlcXFxcdUZFRkZdJ1xuICAsIGxlZnQgPSBuZXcgUmVnRXhwKCdeJysgd2hpdGVzcGFjZSArJysnKTtcblxuLyoqXG4gKiBUcmltIGEgZ2l2ZW4gc3RyaW5nLlxuICpcbiAqIEBwYXJhbSB7U3RyaW5nfSBzdHIgU3RyaW5nIHRvIHRyaW0uXG4gKiBAcHVibGljXG4gKi9cbmZ1bmN0aW9uIHRyaW1MZWZ0KHN0cikge1xuICByZXR1cm4gKHN0ciA/IHN0ciA6ICcnKS50b1N0cmluZygpLnJlcGxhY2UobGVmdCwgJycpO1xufVxuXG4vKipcbiAqIFRoZXNlIGFyZSB0aGUgcGFyc2UgcnVsZXMgZm9yIHRoZSBVUkwgcGFyc2VyLCBpdCBpbmZvcm1zIHRoZSBwYXJzZXJcbiAqIGFib3V0OlxuICpcbiAqIDAuIFRoZSBjaGFyIGl0IE5lZWRzIHRvIHBhcnNlLCBpZiBpdCdzIGEgc3RyaW5nIGl0IHNob3VsZCBiZSBkb25lIHVzaW5nXG4gKiAgICBpbmRleE9mLCBSZWdFeHAgdXNpbmcgZXhlYyBhbmQgTmFOIG1lYW5zIHNldCBhcyBjdXJyZW50IHZhbHVlLlxuICogMS4gVGhlIHByb3BlcnR5IHdlIHNob3VsZCBzZXQgd2hlbiBwYXJzaW5nIHRoaXMgdmFsdWUuXG4gKiAyLiBJbmRpY2F0aW9uIGlmIGl0J3MgYmFja3dhcmRzIG9yIGZvcndhcmQgcGFyc2luZywgd2hlbiBzZXQgYXMgbnVtYmVyIGl0J3NcbiAqICAgIHRoZSB2YWx1ZSBvZiBleHRyYSBjaGFycyB0aGF0IHNob3VsZCBiZSBzcGxpdCBvZmYuXG4gKiAzLiBJbmhlcml0IGZyb20gbG9jYXRpb24gaWYgbm9uIGV4aXN0aW5nIGluIHRoZSBwYXJzZXIuXG4gKiA0LiBgdG9Mb3dlckNhc2VgIHRoZSByZXN1bHRpbmcgdmFsdWUuXG4gKi9cbnZhciBydWxlcyA9IFtcbiAgWycjJywgJ2hhc2gnXSwgICAgICAgICAgICAgICAgICAgICAgICAvLyBFeHRyYWN0IGZyb20gdGhlIGJhY2suXG4gIFsnPycsICdxdWVyeSddLCAgICAgICAgICAgICAgICAgICAgICAgLy8gRXh0cmFjdCBmcm9tIHRoZSBiYWNrLlxuICBmdW5jdGlvbiBzYW5pdGl6ZShhZGRyZXNzKSB7ICAgICAgICAgIC8vIFNhbml0aXplIHdoYXQgaXMgbGVmdCBvZiB0aGUgYWRkcmVzc1xuICAgIHJldHVybiBhZGRyZXNzLnJlcGxhY2UoJ1xcXFwnLCAnLycpO1xuICB9LFxuICBbJy8nLCAncGF0aG5hbWUnXSwgICAgICAgICAgICAgICAgICAgIC8vIEV4dHJhY3QgZnJvbSB0aGUgYmFjay5cbiAgWydAJywgJ2F1dGgnLCAxXSwgICAgICAgICAgICAgICAgICAgICAvLyBFeHRyYWN0IGZyb20gdGhlIGZyb250LlxuICBbTmFOLCAnaG9zdCcsIHVuZGVmaW5lZCwgMSwgMV0sICAgICAgIC8vIFNldCBsZWZ0IG92ZXIgdmFsdWUuXG4gIFsvOihcXGQrKSQvLCAncG9ydCcsIHVuZGVmaW5lZCwgMV0sICAgIC8vIFJlZ0V4cCB0aGUgYmFjay5cbiAgW05hTiwgJ2hvc3RuYW1lJywgdW5kZWZpbmVkLCAxLCAxXSAgICAvLyBTZXQgbGVmdCBvdmVyLlxuXTtcblxuLyoqXG4gKiBUaGVzZSBwcm9wZXJ0aWVzIHNob3VsZCBub3QgYmUgY29waWVkIG9yIGluaGVyaXRlZCBmcm9tLiBUaGlzIGlzIG9ubHkgbmVlZGVkXG4gKiBmb3IgYWxsIG5vbiBibG9iIFVSTCdzIGFzIGEgYmxvYiBVUkwgZG9lcyBub3QgaW5jbHVkZSBhIGhhc2gsIG9ubHkgdGhlXG4gKiBvcmlnaW4uXG4gKlxuICogQHR5cGUge09iamVjdH1cbiAqIEBwcml2YXRlXG4gKi9cbnZhciBpZ25vcmUgPSB7IGhhc2g6IDEsIHF1ZXJ5OiAxIH07XG5cbi8qKlxuICogVGhlIGxvY2F0aW9uIG9iamVjdCBkaWZmZXJzIHdoZW4geW91ciBjb2RlIGlzIGxvYWRlZCB0aHJvdWdoIGEgbm9ybWFsIHBhZ2UsXG4gKiBXb3JrZXIgb3IgdGhyb3VnaCBhIHdvcmtlciB1c2luZyBhIGJsb2IuIEFuZCB3aXRoIHRoZSBibG9iYmxlIGJlZ2lucyB0aGVcbiAqIHRyb3VibGUgYXMgdGhlIGxvY2F0aW9uIG9iamVjdCB3aWxsIGNvbnRhaW4gdGhlIFVSTCBvZiB0aGUgYmxvYiwgbm90IHRoZVxuICogbG9jYXRpb24gb2YgdGhlIHBhZ2Ugd2hlcmUgb3VyIGNvZGUgaXMgbG9hZGVkIGluLiBUaGUgYWN0dWFsIG9yaWdpbiBpc1xuICogZW5jb2RlZCBpbiB0aGUgYHBhdGhuYW1lYCBzbyB3ZSBjYW4gdGhhbmtmdWxseSBnZW5lcmF0ZSBhIGdvb2QgXCJkZWZhdWx0XCJcbiAqIGxvY2F0aW9uIGZyb20gaXQgc28gd2UgY2FuIGdlbmVyYXRlIHByb3BlciByZWxhdGl2ZSBVUkwncyBhZ2Fpbi5cbiAqXG4gKiBAcGFyYW0ge09iamVjdHxTdHJpbmd9IGxvYyBPcHRpb25hbCBkZWZhdWx0IGxvY2F0aW9uIG9iamVjdC5cbiAqIEByZXR1cm5zIHtPYmplY3R9IGxvbGNhdGlvbiBvYmplY3QuXG4gKiBAcHVibGljXG4gKi9cbmZ1bmN0aW9uIGxvbGNhdGlvbihsb2MpIHtcbiAgdmFyIGdsb2JhbFZhcjtcblxuICBpZiAodHlwZW9mIHdpbmRvdyAhPT0gJ3VuZGVmaW5lZCcpIGdsb2JhbFZhciA9IHdpbmRvdztcbiAgZWxzZSBpZiAodHlwZW9mIGdsb2JhbCAhPT0gJ3VuZGVmaW5lZCcpIGdsb2JhbFZhciA9IGdsb2JhbDtcbiAgZWxzZSBpZiAodHlwZW9mIHNlbGYgIT09ICd1bmRlZmluZWQnKSBnbG9iYWxWYXIgPSBzZWxmO1xuICBlbHNlIGdsb2JhbFZhciA9IHt9O1xuXG4gIHZhciBsb2NhdGlvbiA9IGdsb2JhbFZhci5sb2NhdGlvbiB8fCB7fTtcbiAgbG9jID0gbG9jIHx8IGxvY2F0aW9uO1xuXG4gIHZhciBmaW5hbGRlc3RpbmF0aW9uID0ge31cbiAgICAsIHR5cGUgPSB0eXBlb2YgbG9jXG4gICAgLCBrZXk7XG5cbiAgaWYgKCdibG9iOicgPT09IGxvYy5wcm90b2NvbCkge1xuICAgIGZpbmFsZGVzdGluYXRpb24gPSBuZXcgVXJsKHVuZXNjYXBlKGxvYy5wYXRobmFtZSksIHt9KTtcbiAgfSBlbHNlIGlmICgnc3RyaW5nJyA9PT0gdHlwZSkge1xuICAgIGZpbmFsZGVzdGluYXRpb24gPSBuZXcgVXJsKGxvYywge30pO1xuICAgIGZvciAoa2V5IGluIGlnbm9yZSkgZGVsZXRlIGZpbmFsZGVzdGluYXRpb25ba2V5XTtcbiAgfSBlbHNlIGlmICgnb2JqZWN0JyA9PT0gdHlwZSkge1xuICAgIGZvciAoa2V5IGluIGxvYykge1xuICAgICAgaWYgKGtleSBpbiBpZ25vcmUpIGNvbnRpbnVlO1xuICAgICAgZmluYWxkZXN0aW5hdGlvbltrZXldID0gbG9jW2tleV07XG4gICAgfVxuXG4gICAgaWYgKGZpbmFsZGVzdGluYXRpb24uc2xhc2hlcyA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBmaW5hbGRlc3RpbmF0aW9uLnNsYXNoZXMgPSBzbGFzaGVzLnRlc3QobG9jLmhyZWYpO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiBmaW5hbGRlc3RpbmF0aW9uO1xufVxuXG4vKipcbiAqIEB0eXBlZGVmIFByb3RvY29sRXh0cmFjdFxuICogQHR5cGUgT2JqZWN0XG4gKiBAcHJvcGVydHkge1N0cmluZ30gcHJvdG9jb2wgUHJvdG9jb2wgbWF0Y2hlZCBpbiB0aGUgVVJMLCBpbiBsb3dlcmNhc2UuXG4gKiBAcHJvcGVydHkge0Jvb2xlYW59IHNsYXNoZXMgYHRydWVgIGlmIHByb3RvY29sIGlzIGZvbGxvd2VkIGJ5IFwiLy9cIiwgZWxzZSBgZmFsc2VgLlxuICogQHByb3BlcnR5IHtTdHJpbmd9IHJlc3QgUmVzdCBvZiB0aGUgVVJMIHRoYXQgaXMgbm90IHBhcnQgb2YgdGhlIHByb3RvY29sLlxuICovXG5cbi8qKlxuICogRXh0cmFjdCBwcm90b2NvbCBpbmZvcm1hdGlvbiBmcm9tIGEgVVJMIHdpdGgvd2l0aG91dCBkb3VibGUgc2xhc2ggKFwiLy9cIikuXG4gKlxuICogQHBhcmFtIHtTdHJpbmd9IGFkZHJlc3MgVVJMIHdlIHdhbnQgdG8gZXh0cmFjdCBmcm9tLlxuICogQHJldHVybiB7UHJvdG9jb2xFeHRyYWN0fSBFeHRyYWN0ZWQgaW5mb3JtYXRpb24uXG4gKiBAcHJpdmF0ZVxuICovXG5mdW5jdGlvbiBleHRyYWN0UHJvdG9jb2woYWRkcmVzcykge1xuICBhZGRyZXNzID0gdHJpbUxlZnQoYWRkcmVzcyk7XG4gIHZhciBtYXRjaCA9IHByb3RvY29scmUuZXhlYyhhZGRyZXNzKTtcblxuICByZXR1cm4ge1xuICAgIHByb3RvY29sOiBtYXRjaFsxXSA/IG1hdGNoWzFdLnRvTG93ZXJDYXNlKCkgOiAnJyxcbiAgICBzbGFzaGVzOiAhIW1hdGNoWzJdLFxuICAgIHJlc3Q6IG1hdGNoWzNdXG4gIH07XG59XG5cbi8qKlxuICogUmVzb2x2ZSBhIHJlbGF0aXZlIFVSTCBwYXRobmFtZSBhZ2FpbnN0IGEgYmFzZSBVUkwgcGF0aG5hbWUuXG4gKlxuICogQHBhcmFtIHtTdHJpbmd9IHJlbGF0aXZlIFBhdGhuYW1lIG9mIHRoZSByZWxhdGl2ZSBVUkwuXG4gKiBAcGFyYW0ge1N0cmluZ30gYmFzZSBQYXRobmFtZSBvZiB0aGUgYmFzZSBVUkwuXG4gKiBAcmV0dXJuIHtTdHJpbmd9IFJlc29sdmVkIHBhdGhuYW1lLlxuICogQHByaXZhdGVcbiAqL1xuZnVuY3Rpb24gcmVzb2x2ZShyZWxhdGl2ZSwgYmFzZSkge1xuICBpZiAocmVsYXRpdmUgPT09ICcnKSByZXR1cm4gYmFzZTtcblxuICB2YXIgcGF0aCA9IChiYXNlIHx8ICcvJykuc3BsaXQoJy8nKS5zbGljZSgwLCAtMSkuY29uY2F0KHJlbGF0aXZlLnNwbGl0KCcvJykpXG4gICAgLCBpID0gcGF0aC5sZW5ndGhcbiAgICAsIGxhc3QgPSBwYXRoW2kgLSAxXVxuICAgICwgdW5zaGlmdCA9IGZhbHNlXG4gICAgLCB1cCA9IDA7XG5cbiAgd2hpbGUgKGktLSkge1xuICAgIGlmIChwYXRoW2ldID09PSAnLicpIHtcbiAgICAgIHBhdGguc3BsaWNlKGksIDEpO1xuICAgIH0gZWxzZSBpZiAocGF0aFtpXSA9PT0gJy4uJykge1xuICAgICAgcGF0aC5zcGxpY2UoaSwgMSk7XG4gICAgICB1cCsrO1xuICAgIH0gZWxzZSBpZiAodXApIHtcbiAgICAgIGlmIChpID09PSAwKSB1bnNoaWZ0ID0gdHJ1ZTtcbiAgICAgIHBhdGguc3BsaWNlKGksIDEpO1xuICAgICAgdXAtLTtcbiAgICB9XG4gIH1cblxuICBpZiAodW5zaGlmdCkgcGF0aC51bnNoaWZ0KCcnKTtcbiAgaWYgKGxhc3QgPT09ICcuJyB8fCBsYXN0ID09PSAnLi4nKSBwYXRoLnB1c2goJycpO1xuXG4gIHJldHVybiBwYXRoLmpvaW4oJy8nKTtcbn1cblxuLyoqXG4gKiBUaGUgYWN0dWFsIFVSTCBpbnN0YW5jZS4gSW5zdGVhZCBvZiByZXR1cm5pbmcgYW4gb2JqZWN0IHdlJ3ZlIG9wdGVkLWluIHRvXG4gKiBjcmVhdGUgYW4gYWN0dWFsIGNvbnN0cnVjdG9yIGFzIGl0J3MgbXVjaCBtb3JlIG1lbW9yeSBlZmZpY2llbnQgYW5kXG4gKiBmYXN0ZXIgYW5kIGl0IHBsZWFzZXMgbXkgT0NELlxuICpcbiAqIEl0IGlzIHdvcnRoIG5vdGluZyB0aGF0IHdlIHNob3VsZCBub3QgdXNlIGBVUkxgIGFzIGNsYXNzIG5hbWUgdG8gcHJldmVudFxuICogY2xhc2hlcyB3aXRoIHRoZSBnbG9iYWwgVVJMIGluc3RhbmNlIHRoYXQgZ290IGludHJvZHVjZWQgaW4gYnJvd3NlcnMuXG4gKlxuICogQGNvbnN0cnVjdG9yXG4gKiBAcGFyYW0ge1N0cmluZ30gYWRkcmVzcyBVUkwgd2Ugd2FudCB0byBwYXJzZS5cbiAqIEBwYXJhbSB7T2JqZWN0fFN0cmluZ30gW2xvY2F0aW9uXSBMb2NhdGlvbiBkZWZhdWx0cyBmb3IgcmVsYXRpdmUgcGF0aHMuXG4gKiBAcGFyYW0ge0Jvb2xlYW58RnVuY3Rpb259IFtwYXJzZXJdIFBhcnNlciBmb3IgdGhlIHF1ZXJ5IHN0cmluZy5cbiAqIEBwcml2YXRlXG4gKi9cbmZ1bmN0aW9uIFVybChhZGRyZXNzLCBsb2NhdGlvbiwgcGFyc2VyKSB7XG4gIGFkZHJlc3MgPSB0cmltTGVmdChhZGRyZXNzKTtcblxuICBpZiAoISh0aGlzIGluc3RhbmNlb2YgVXJsKSkge1xuICAgIHJldHVybiBuZXcgVXJsKGFkZHJlc3MsIGxvY2F0aW9uLCBwYXJzZXIpO1xuICB9XG5cbiAgdmFyIHJlbGF0aXZlLCBleHRyYWN0ZWQsIHBhcnNlLCBpbnN0cnVjdGlvbiwgaW5kZXgsIGtleVxuICAgICwgaW5zdHJ1Y3Rpb25zID0gcnVsZXMuc2xpY2UoKVxuICAgICwgdHlwZSA9IHR5cGVvZiBsb2NhdGlvblxuICAgICwgdXJsID0gdGhpc1xuICAgICwgaSA9IDA7XG5cbiAgLy9cbiAgLy8gVGhlIGZvbGxvd2luZyBpZiBzdGF0ZW1lbnRzIGFsbG93cyB0aGlzIG1vZHVsZSB0d28gaGF2ZSBjb21wYXRpYmlsaXR5IHdpdGhcbiAgLy8gMiBkaWZmZXJlbnQgQVBJOlxuICAvL1xuICAvLyAxLiBOb2RlLmpzJ3MgYHVybC5wYXJzZWAgYXBpIHdoaWNoIGFjY2VwdHMgYSBVUkwsIGJvb2xlYW4gYXMgYXJndW1lbnRzXG4gIC8vICAgIHdoZXJlIHRoZSBib29sZWFuIGluZGljYXRlcyB0aGF0IHRoZSBxdWVyeSBzdHJpbmcgc2hvdWxkIGFsc28gYmUgcGFyc2VkLlxuICAvL1xuICAvLyAyLiBUaGUgYFVSTGAgaW50ZXJmYWNlIG9mIHRoZSBicm93c2VyIHdoaWNoIGFjY2VwdHMgYSBVUkwsIG9iamVjdCBhc1xuICAvLyAgICBhcmd1bWVudHMuIFRoZSBzdXBwbGllZCBvYmplY3Qgd2lsbCBiZSB1c2VkIGFzIGRlZmF1bHQgdmFsdWVzIC8gZmFsbC1iYWNrXG4gIC8vICAgIGZvciByZWxhdGl2ZSBwYXRocy5cbiAgLy9cbiAgaWYgKCdvYmplY3QnICE9PSB0eXBlICYmICdzdHJpbmcnICE9PSB0eXBlKSB7XG4gICAgcGFyc2VyID0gbG9jYXRpb247XG4gICAgbG9jYXRpb24gPSBudWxsO1xuICB9XG5cbiAgaWYgKHBhcnNlciAmJiAnZnVuY3Rpb24nICE9PSB0eXBlb2YgcGFyc2VyKSBwYXJzZXIgPSBxcy5wYXJzZTtcblxuICBsb2NhdGlvbiA9IGxvbGNhdGlvbihsb2NhdGlvbik7XG5cbiAgLy9cbiAgLy8gRXh0cmFjdCBwcm90b2NvbCBpbmZvcm1hdGlvbiBiZWZvcmUgcnVubmluZyB0aGUgaW5zdHJ1Y3Rpb25zLlxuICAvL1xuICBleHRyYWN0ZWQgPSBleHRyYWN0UHJvdG9jb2woYWRkcmVzcyB8fCAnJyk7XG4gIHJlbGF0aXZlID0gIWV4dHJhY3RlZC5wcm90b2NvbCAmJiAhZXh0cmFjdGVkLnNsYXNoZXM7XG4gIHVybC5zbGFzaGVzID0gZXh0cmFjdGVkLnNsYXNoZXMgfHwgcmVsYXRpdmUgJiYgbG9jYXRpb24uc2xhc2hlcztcbiAgdXJsLnByb3RvY29sID0gZXh0cmFjdGVkLnByb3RvY29sIHx8IGxvY2F0aW9uLnByb3RvY29sIHx8ICcnO1xuICBhZGRyZXNzID0gZXh0cmFjdGVkLnJlc3Q7XG5cbiAgLy9cbiAgLy8gV2hlbiB0aGUgYXV0aG9yaXR5IGNvbXBvbmVudCBpcyBhYnNlbnQgdGhlIFVSTCBzdGFydHMgd2l0aCBhIHBhdGhcbiAgLy8gY29tcG9uZW50LlxuICAvL1xuICBpZiAoIWV4dHJhY3RlZC5zbGFzaGVzKSBpbnN0cnVjdGlvbnNbM10gPSBbLyguKikvLCAncGF0aG5hbWUnXTtcblxuICBmb3IgKDsgaSA8IGluc3RydWN0aW9ucy5sZW5ndGg7IGkrKykge1xuICAgIGluc3RydWN0aW9uID0gaW5zdHJ1Y3Rpb25zW2ldO1xuXG4gICAgaWYgKHR5cGVvZiBpbnN0cnVjdGlvbiA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgYWRkcmVzcyA9IGluc3RydWN0aW9uKGFkZHJlc3MpO1xuICAgICAgY29udGludWU7XG4gICAgfVxuXG4gICAgcGFyc2UgPSBpbnN0cnVjdGlvblswXTtcbiAgICBrZXkgPSBpbnN0cnVjdGlvblsxXTtcblxuICAgIGlmIChwYXJzZSAhPT0gcGFyc2UpIHtcbiAgICAgIHVybFtrZXldID0gYWRkcmVzcztcbiAgICB9IGVsc2UgaWYgKCdzdHJpbmcnID09PSB0eXBlb2YgcGFyc2UpIHtcbiAgICAgIGlmICh+KGluZGV4ID0gYWRkcmVzcy5pbmRleE9mKHBhcnNlKSkpIHtcbiAgICAgICAgaWYgKCdudW1iZXInID09PSB0eXBlb2YgaW5zdHJ1Y3Rpb25bMl0pIHtcbiAgICAgICAgICB1cmxba2V5XSA9IGFkZHJlc3Muc2xpY2UoMCwgaW5kZXgpO1xuICAgICAgICAgIGFkZHJlc3MgPSBhZGRyZXNzLnNsaWNlKGluZGV4ICsgaW5zdHJ1Y3Rpb25bMl0pO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHVybFtrZXldID0gYWRkcmVzcy5zbGljZShpbmRleCk7XG4gICAgICAgICAgYWRkcmVzcyA9IGFkZHJlc3Muc2xpY2UoMCwgaW5kZXgpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfSBlbHNlIGlmICgoaW5kZXggPSBwYXJzZS5leGVjKGFkZHJlc3MpKSkge1xuICAgICAgdXJsW2tleV0gPSBpbmRleFsxXTtcbiAgICAgIGFkZHJlc3MgPSBhZGRyZXNzLnNsaWNlKDAsIGluZGV4LmluZGV4KTtcbiAgICB9XG5cbiAgICB1cmxba2V5XSA9IHVybFtrZXldIHx8IChcbiAgICAgIHJlbGF0aXZlICYmIGluc3RydWN0aW9uWzNdID8gbG9jYXRpb25ba2V5XSB8fCAnJyA6ICcnXG4gICAgKTtcblxuICAgIC8vXG4gICAgLy8gSG9zdG5hbWUsIGhvc3QgYW5kIHByb3RvY29sIHNob3VsZCBiZSBsb3dlcmNhc2VkIHNvIHRoZXkgY2FuIGJlIHVzZWQgdG9cbiAgICAvLyBjcmVhdGUgYSBwcm9wZXIgYG9yaWdpbmAuXG4gICAgLy9cbiAgICBpZiAoaW5zdHJ1Y3Rpb25bNF0pIHVybFtrZXldID0gdXJsW2tleV0udG9Mb3dlckNhc2UoKTtcbiAgfVxuXG4gIC8vXG4gIC8vIEFsc28gcGFyc2UgdGhlIHN1cHBsaWVkIHF1ZXJ5IHN0cmluZyBpbiB0byBhbiBvYmplY3QuIElmIHdlJ3JlIHN1cHBsaWVkXG4gIC8vIHdpdGggYSBjdXN0b20gcGFyc2VyIGFzIGZ1bmN0aW9uIHVzZSB0aGF0IGluc3RlYWQgb2YgdGhlIGRlZmF1bHQgYnVpbGQtaW5cbiAgLy8gcGFyc2VyLlxuICAvL1xuICBpZiAocGFyc2VyKSB1cmwucXVlcnkgPSBwYXJzZXIodXJsLnF1ZXJ5KTtcblxuICAvL1xuICAvLyBJZiB0aGUgVVJMIGlzIHJlbGF0aXZlLCByZXNvbHZlIHRoZSBwYXRobmFtZSBhZ2FpbnN0IHRoZSBiYXNlIFVSTC5cbiAgLy9cbiAgaWYgKFxuICAgICAgcmVsYXRpdmVcbiAgICAmJiBsb2NhdGlvbi5zbGFzaGVzXG4gICAgJiYgdXJsLnBhdGhuYW1lLmNoYXJBdCgwKSAhPT0gJy8nXG4gICAgJiYgKHVybC5wYXRobmFtZSAhPT0gJycgfHwgbG9jYXRpb24ucGF0aG5hbWUgIT09ICcnKVxuICApIHtcbiAgICB1cmwucGF0aG5hbWUgPSByZXNvbHZlKHVybC5wYXRobmFtZSwgbG9jYXRpb24ucGF0aG5hbWUpO1xuICB9XG5cbiAgLy9cbiAgLy8gV2Ugc2hvdWxkIG5vdCBhZGQgcG9ydCBudW1iZXJzIGlmIHRoZXkgYXJlIGFscmVhZHkgdGhlIGRlZmF1bHQgcG9ydCBudW1iZXJcbiAgLy8gZm9yIGEgZ2l2ZW4gcHJvdG9jb2wuIEFzIHRoZSBob3N0IGFsc28gY29udGFpbnMgdGhlIHBvcnQgbnVtYmVyIHdlJ3JlIGdvaW5nXG4gIC8vIG92ZXJyaWRlIGl0IHdpdGggdGhlIGhvc3RuYW1lIHdoaWNoIGNvbnRhaW5zIG5vIHBvcnQgbnVtYmVyLlxuICAvL1xuICBpZiAoIXJlcXVpcmVkKHVybC5wb3J0LCB1cmwucHJvdG9jb2wpKSB7XG4gICAgdXJsLmhvc3QgPSB1cmwuaG9zdG5hbWU7XG4gICAgdXJsLnBvcnQgPSAnJztcbiAgfVxuXG4gIC8vXG4gIC8vIFBhcnNlIGRvd24gdGhlIGBhdXRoYCBmb3IgdGhlIHVzZXJuYW1lIGFuZCBwYXNzd29yZC5cbiAgLy9cbiAgdXJsLnVzZXJuYW1lID0gdXJsLnBhc3N3b3JkID0gJyc7XG4gIGlmICh1cmwuYXV0aCkge1xuICAgIGluc3RydWN0aW9uID0gdXJsLmF1dGguc3BsaXQoJzonKTtcbiAgICB1cmwudXNlcm5hbWUgPSBpbnN0cnVjdGlvblswXSB8fCAnJztcbiAgICB1cmwucGFzc3dvcmQgPSBpbnN0cnVjdGlvblsxXSB8fCAnJztcbiAgfVxuXG4gIHVybC5vcmlnaW4gPSB1cmwucHJvdG9jb2wgJiYgdXJsLmhvc3QgJiYgdXJsLnByb3RvY29sICE9PSAnZmlsZTonXG4gICAgPyB1cmwucHJvdG9jb2wgKycvLycrIHVybC5ob3N0XG4gICAgOiAnbnVsbCc7XG5cbiAgLy9cbiAgLy8gVGhlIGhyZWYgaXMganVzdCB0aGUgY29tcGlsZWQgcmVzdWx0LlxuICAvL1xuICB1cmwuaHJlZiA9IHVybC50b1N0cmluZygpO1xufVxuXG4vKipcbiAqIFRoaXMgaXMgY29udmVuaWVuY2UgbWV0aG9kIGZvciBjaGFuZ2luZyBwcm9wZXJ0aWVzIGluIHRoZSBVUkwgaW5zdGFuY2UgdG9cbiAqIGluc3VyZSB0aGF0IHRoZXkgYWxsIHByb3BhZ2F0ZSBjb3JyZWN0bHkuXG4gKlxuICogQHBhcmFtIHtTdHJpbmd9IHBhcnQgICAgICAgICAgUHJvcGVydHkgd2UgbmVlZCB0byBhZGp1c3QuXG4gKiBAcGFyYW0ge01peGVkfSB2YWx1ZSAgICAgICAgICBUaGUgbmV3bHkgYXNzaWduZWQgdmFsdWUuXG4gKiBAcGFyYW0ge0Jvb2xlYW58RnVuY3Rpb259IGZuICBXaGVuIHNldHRpbmcgdGhlIHF1ZXJ5LCBpdCB3aWxsIGJlIHRoZSBmdW5jdGlvblxuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgdXNlZCB0byBwYXJzZSB0aGUgcXVlcnkuXG4gKiAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBXaGVuIHNldHRpbmcgdGhlIHByb3RvY29sLCBkb3VibGUgc2xhc2ggd2lsbCBiZVxuICogICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgcmVtb3ZlZCBmcm9tIHRoZSBmaW5hbCB1cmwgaWYgaXQgaXMgdHJ1ZS5cbiAqIEByZXR1cm5zIHtVUkx9IFVSTCBpbnN0YW5jZSBmb3IgY2hhaW5pbmcuXG4gKiBAcHVibGljXG4gKi9cbmZ1bmN0aW9uIHNldChwYXJ0LCB2YWx1ZSwgZm4pIHtcbiAgdmFyIHVybCA9IHRoaXM7XG5cbiAgc3dpdGNoIChwYXJ0KSB7XG4gICAgY2FzZSAncXVlcnknOlxuICAgICAgaWYgKCdzdHJpbmcnID09PSB0eXBlb2YgdmFsdWUgJiYgdmFsdWUubGVuZ3RoKSB7XG4gICAgICAgIHZhbHVlID0gKGZuIHx8IHFzLnBhcnNlKSh2YWx1ZSk7XG4gICAgICB9XG5cbiAgICAgIHVybFtwYXJ0XSA9IHZhbHVlO1xuICAgICAgYnJlYWs7XG5cbiAgICBjYXNlICdwb3J0JzpcbiAgICAgIHVybFtwYXJ0XSA9IHZhbHVlO1xuXG4gICAgICBpZiAoIXJlcXVpcmVkKHZhbHVlLCB1cmwucHJvdG9jb2wpKSB7XG4gICAgICAgIHVybC5ob3N0ID0gdXJsLmhvc3RuYW1lO1xuICAgICAgICB1cmxbcGFydF0gPSAnJztcbiAgICAgIH0gZWxzZSBpZiAodmFsdWUpIHtcbiAgICAgICAgdXJsLmhvc3QgPSB1cmwuaG9zdG5hbWUgKyc6JysgdmFsdWU7XG4gICAgICB9XG5cbiAgICAgIGJyZWFrO1xuXG4gICAgY2FzZSAnaG9zdG5hbWUnOlxuICAgICAgdXJsW3BhcnRdID0gdmFsdWU7XG5cbiAgICAgIGlmICh1cmwucG9ydCkgdmFsdWUgKz0gJzonKyB1cmwucG9ydDtcbiAgICAgIHVybC5ob3N0ID0gdmFsdWU7XG4gICAgICBicmVhaztcblxuICAgIGNhc2UgJ2hvc3QnOlxuICAgICAgdXJsW3BhcnRdID0gdmFsdWU7XG5cbiAgICAgIGlmICgvOlxcZCskLy50ZXN0KHZhbHVlKSkge1xuICAgICAgICB2YWx1ZSA9IHZhbHVlLnNwbGl0KCc6Jyk7XG4gICAgICAgIHVybC5wb3J0ID0gdmFsdWUucG9wKCk7XG4gICAgICAgIHVybC5ob3N0bmFtZSA9IHZhbHVlLmpvaW4oJzonKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHVybC5ob3N0bmFtZSA9IHZhbHVlO1xuICAgICAgICB1cmwucG9ydCA9ICcnO1xuICAgICAgfVxuXG4gICAgICBicmVhaztcblxuICAgIGNhc2UgJ3Byb3RvY29sJzpcbiAgICAgIHVybC5wcm90b2NvbCA9IHZhbHVlLnRvTG93ZXJDYXNlKCk7XG4gICAgICB1cmwuc2xhc2hlcyA9ICFmbjtcbiAgICAgIGJyZWFrO1xuXG4gICAgY2FzZSAncGF0aG5hbWUnOlxuICAgIGNhc2UgJ2hhc2gnOlxuICAgICAgaWYgKHZhbHVlKSB7XG4gICAgICAgIHZhciBjaGFyID0gcGFydCA9PT0gJ3BhdGhuYW1lJyA/ICcvJyA6ICcjJztcbiAgICAgICAgdXJsW3BhcnRdID0gdmFsdWUuY2hhckF0KDApICE9PSBjaGFyID8gY2hhciArIHZhbHVlIDogdmFsdWU7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB1cmxbcGFydF0gPSB2YWx1ZTtcbiAgICAgIH1cbiAgICAgIGJyZWFrO1xuXG4gICAgZGVmYXVsdDpcbiAgICAgIHVybFtwYXJ0XSA9IHZhbHVlO1xuICB9XG5cbiAgZm9yICh2YXIgaSA9IDA7IGkgPCBydWxlcy5sZW5ndGg7IGkrKykge1xuICAgIHZhciBpbnMgPSBydWxlc1tpXTtcblxuICAgIGlmIChpbnNbNF0pIHVybFtpbnNbMV1dID0gdXJsW2luc1sxXV0udG9Mb3dlckNhc2UoKTtcbiAgfVxuXG4gIHVybC5vcmlnaW4gPSB1cmwucHJvdG9jb2wgJiYgdXJsLmhvc3QgJiYgdXJsLnByb3RvY29sICE9PSAnZmlsZTonXG4gICAgPyB1cmwucHJvdG9jb2wgKycvLycrIHVybC5ob3N0XG4gICAgOiAnbnVsbCc7XG5cbiAgdXJsLmhyZWYgPSB1cmwudG9TdHJpbmcoKTtcblxuICByZXR1cm4gdXJsO1xufVxuXG4vKipcbiAqIFRyYW5zZm9ybSB0aGUgcHJvcGVydGllcyBiYWNrIGluIHRvIGEgdmFsaWQgYW5kIGZ1bGwgVVJMIHN0cmluZy5cbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBzdHJpbmdpZnkgT3B0aW9uYWwgcXVlcnkgc3RyaW5naWZ5IGZ1bmN0aW9uLlxuICogQHJldHVybnMge1N0cmluZ30gQ29tcGlsZWQgdmVyc2lvbiBvZiB0aGUgVVJMLlxuICogQHB1YmxpY1xuICovXG5mdW5jdGlvbiB0b1N0cmluZyhzdHJpbmdpZnkpIHtcbiAgaWYgKCFzdHJpbmdpZnkgfHwgJ2Z1bmN0aW9uJyAhPT0gdHlwZW9mIHN0cmluZ2lmeSkgc3RyaW5naWZ5ID0gcXMuc3RyaW5naWZ5O1xuXG4gIHZhciBxdWVyeVxuICAgICwgdXJsID0gdGhpc1xuICAgICwgcHJvdG9jb2wgPSB1cmwucHJvdG9jb2w7XG5cbiAgaWYgKHByb3RvY29sICYmIHByb3RvY29sLmNoYXJBdChwcm90b2NvbC5sZW5ndGggLSAxKSAhPT0gJzonKSBwcm90b2NvbCArPSAnOic7XG5cbiAgdmFyIHJlc3VsdCA9IHByb3RvY29sICsgKHVybC5zbGFzaGVzID8gJy8vJyA6ICcnKTtcblxuICBpZiAodXJsLnVzZXJuYW1lKSB7XG4gICAgcmVzdWx0ICs9IHVybC51c2VybmFtZTtcbiAgICBpZiAodXJsLnBhc3N3b3JkKSByZXN1bHQgKz0gJzonKyB1cmwucGFzc3dvcmQ7XG4gICAgcmVzdWx0ICs9ICdAJztcbiAgfVxuXG4gIHJlc3VsdCArPSB1cmwuaG9zdCArIHVybC5wYXRobmFtZTtcblxuICBxdWVyeSA9ICdvYmplY3QnID09PSB0eXBlb2YgdXJsLnF1ZXJ5ID8gc3RyaW5naWZ5KHVybC5xdWVyeSkgOiB1cmwucXVlcnk7XG4gIGlmIChxdWVyeSkgcmVzdWx0ICs9ICc/JyAhPT0gcXVlcnkuY2hhckF0KDApID8gJz8nKyBxdWVyeSA6IHF1ZXJ5O1xuXG4gIGlmICh1cmwuaGFzaCkgcmVzdWx0ICs9IHVybC5oYXNoO1xuXG4gIHJldHVybiByZXN1bHQ7XG59XG5cblVybC5wcm90b3R5cGUgPSB7IHNldDogc2V0LCB0b1N0cmluZzogdG9TdHJpbmcgfTtcblxuLy9cbi8vIEV4cG9zZSB0aGUgVVJMIHBhcnNlciBhbmQgc29tZSBhZGRpdGlvbmFsIHByb3BlcnRpZXMgdGhhdCBtaWdodCBiZSB1c2VmdWwgZm9yXG4vLyBvdGhlcnMgb3IgdGVzdGluZy5cbi8vXG5VcmwuZXh0cmFjdFByb3RvY29sID0gZXh0cmFjdFByb3RvY29sO1xuVXJsLmxvY2F0aW9uID0gbG9sY2F0aW9uO1xuVXJsLnRyaW1MZWZ0ID0gdHJpbUxlZnQ7XG5VcmwucXMgPSBxcztcblxubW9kdWxlLmV4cG9ydHMgPSBVcmw7XG4iLCIoZnVuY3Rpb24gKGdsb2JhbCwgZmFjdG9yeSkge1xuICB0eXBlb2YgZXhwb3J0cyA9PT0gJ29iamVjdCcgJiYgdHlwZW9mIG1vZHVsZSAhPT0gJ3VuZGVmaW5lZCcgPyBmYWN0b3J5KGV4cG9ydHMpIDpcbiAgdHlwZW9mIGRlZmluZSA9PT0gJ2Z1bmN0aW9uJyAmJiBkZWZpbmUuYW1kID8gZGVmaW5lKFsnZXhwb3J0cyddLCBmYWN0b3J5KSA6XG4gIChmYWN0b3J5KChnbG9iYWwuV0hBVFdHRmV0Y2ggPSB7fSkpKTtcbn0odGhpcywgKGZ1bmN0aW9uIChleHBvcnRzKSB7ICd1c2Ugc3RyaWN0JztcblxuICB2YXIgc3VwcG9ydCA9IHtcbiAgICBzZWFyY2hQYXJhbXM6ICdVUkxTZWFyY2hQYXJhbXMnIGluIHNlbGYsXG4gICAgaXRlcmFibGU6ICdTeW1ib2wnIGluIHNlbGYgJiYgJ2l0ZXJhdG9yJyBpbiBTeW1ib2wsXG4gICAgYmxvYjpcbiAgICAgICdGaWxlUmVhZGVyJyBpbiBzZWxmICYmXG4gICAgICAnQmxvYicgaW4gc2VsZiAmJlxuICAgICAgKGZ1bmN0aW9uKCkge1xuICAgICAgICB0cnkge1xuICAgICAgICAgIG5ldyBCbG9iKCk7XG4gICAgICAgICAgcmV0dXJuIHRydWVcbiAgICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgICB9XG4gICAgICB9KSgpLFxuICAgIGZvcm1EYXRhOiAnRm9ybURhdGEnIGluIHNlbGYsXG4gICAgYXJyYXlCdWZmZXI6ICdBcnJheUJ1ZmZlcicgaW4gc2VsZlxuICB9O1xuXG4gIGZ1bmN0aW9uIGlzRGF0YVZpZXcob2JqKSB7XG4gICAgcmV0dXJuIG9iaiAmJiBEYXRhVmlldy5wcm90b3R5cGUuaXNQcm90b3R5cGVPZihvYmopXG4gIH1cblxuICBpZiAoc3VwcG9ydC5hcnJheUJ1ZmZlcikge1xuICAgIHZhciB2aWV3Q2xhc3NlcyA9IFtcbiAgICAgICdbb2JqZWN0IEludDhBcnJheV0nLFxuICAgICAgJ1tvYmplY3QgVWludDhBcnJheV0nLFxuICAgICAgJ1tvYmplY3QgVWludDhDbGFtcGVkQXJyYXldJyxcbiAgICAgICdbb2JqZWN0IEludDE2QXJyYXldJyxcbiAgICAgICdbb2JqZWN0IFVpbnQxNkFycmF5XScsXG4gICAgICAnW29iamVjdCBJbnQzMkFycmF5XScsXG4gICAgICAnW29iamVjdCBVaW50MzJBcnJheV0nLFxuICAgICAgJ1tvYmplY3QgRmxvYXQzMkFycmF5XScsXG4gICAgICAnW29iamVjdCBGbG9hdDY0QXJyYXldJ1xuICAgIF07XG5cbiAgICB2YXIgaXNBcnJheUJ1ZmZlclZpZXcgPVxuICAgICAgQXJyYXlCdWZmZXIuaXNWaWV3IHx8XG4gICAgICBmdW5jdGlvbihvYmopIHtcbiAgICAgICAgcmV0dXJuIG9iaiAmJiB2aWV3Q2xhc3Nlcy5pbmRleE9mKE9iamVjdC5wcm90b3R5cGUudG9TdHJpbmcuY2FsbChvYmopKSA+IC0xXG4gICAgICB9O1xuICB9XG5cbiAgZnVuY3Rpb24gbm9ybWFsaXplTmFtZShuYW1lKSB7XG4gICAgaWYgKHR5cGVvZiBuYW1lICE9PSAnc3RyaW5nJykge1xuICAgICAgbmFtZSA9IFN0cmluZyhuYW1lKTtcbiAgICB9XG4gICAgaWYgKC9bXmEtejAtOVxcLSMkJSYnKisuXl9gfH5dL2kudGVzdChuYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignSW52YWxpZCBjaGFyYWN0ZXIgaW4gaGVhZGVyIGZpZWxkIG5hbWUnKVxuICAgIH1cbiAgICByZXR1cm4gbmFtZS50b0xvd2VyQ2FzZSgpXG4gIH1cblxuICBmdW5jdGlvbiBub3JtYWxpemVWYWx1ZSh2YWx1ZSkge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgIT09ICdzdHJpbmcnKSB7XG4gICAgICB2YWx1ZSA9IFN0cmluZyh2YWx1ZSk7XG4gICAgfVxuICAgIHJldHVybiB2YWx1ZVxuICB9XG5cbiAgLy8gQnVpbGQgYSBkZXN0cnVjdGl2ZSBpdGVyYXRvciBmb3IgdGhlIHZhbHVlIGxpc3RcbiAgZnVuY3Rpb24gaXRlcmF0b3JGb3IoaXRlbXMpIHtcbiAgICB2YXIgaXRlcmF0b3IgPSB7XG4gICAgICBuZXh0OiBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHZhbHVlID0gaXRlbXMuc2hpZnQoKTtcbiAgICAgICAgcmV0dXJuIHtkb25lOiB2YWx1ZSA9PT0gdW5kZWZpbmVkLCB2YWx1ZTogdmFsdWV9XG4gICAgICB9XG4gICAgfTtcblxuICAgIGlmIChzdXBwb3J0Lml0ZXJhYmxlKSB7XG4gICAgICBpdGVyYXRvcltTeW1ib2wuaXRlcmF0b3JdID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiBpdGVyYXRvclxuICAgICAgfTtcbiAgICB9XG5cbiAgICByZXR1cm4gaXRlcmF0b3JcbiAgfVxuXG4gIGZ1bmN0aW9uIEhlYWRlcnMoaGVhZGVycykge1xuICAgIHRoaXMubWFwID0ge307XG5cbiAgICBpZiAoaGVhZGVycyBpbnN0YW5jZW9mIEhlYWRlcnMpIHtcbiAgICAgIGhlYWRlcnMuZm9yRWFjaChmdW5jdGlvbih2YWx1ZSwgbmFtZSkge1xuICAgICAgICB0aGlzLmFwcGVuZChuYW1lLCB2YWx1ZSk7XG4gICAgICB9LCB0aGlzKTtcbiAgICB9IGVsc2UgaWYgKEFycmF5LmlzQXJyYXkoaGVhZGVycykpIHtcbiAgICAgIGhlYWRlcnMuZm9yRWFjaChmdW5jdGlvbihoZWFkZXIpIHtcbiAgICAgICAgdGhpcy5hcHBlbmQoaGVhZGVyWzBdLCBoZWFkZXJbMV0pO1xuICAgICAgfSwgdGhpcyk7XG4gICAgfSBlbHNlIGlmIChoZWFkZXJzKSB7XG4gICAgICBPYmplY3QuZ2V0T3duUHJvcGVydHlOYW1lcyhoZWFkZXJzKS5mb3JFYWNoKGZ1bmN0aW9uKG5hbWUpIHtcbiAgICAgICAgdGhpcy5hcHBlbmQobmFtZSwgaGVhZGVyc1tuYW1lXSk7XG4gICAgICB9LCB0aGlzKTtcbiAgICB9XG4gIH1cblxuICBIZWFkZXJzLnByb3RvdHlwZS5hcHBlbmQgPSBmdW5jdGlvbihuYW1lLCB2YWx1ZSkge1xuICAgIG5hbWUgPSBub3JtYWxpemVOYW1lKG5hbWUpO1xuICAgIHZhbHVlID0gbm9ybWFsaXplVmFsdWUodmFsdWUpO1xuICAgIHZhciBvbGRWYWx1ZSA9IHRoaXMubWFwW25hbWVdO1xuICAgIHRoaXMubWFwW25hbWVdID0gb2xkVmFsdWUgPyBvbGRWYWx1ZSArICcsICcgKyB2YWx1ZSA6IHZhbHVlO1xuICB9O1xuXG4gIEhlYWRlcnMucHJvdG90eXBlWydkZWxldGUnXSA9IGZ1bmN0aW9uKG5hbWUpIHtcbiAgICBkZWxldGUgdGhpcy5tYXBbbm9ybWFsaXplTmFtZShuYW1lKV07XG4gIH07XG5cbiAgSGVhZGVycy5wcm90b3R5cGUuZ2V0ID0gZnVuY3Rpb24obmFtZSkge1xuICAgIG5hbWUgPSBub3JtYWxpemVOYW1lKG5hbWUpO1xuICAgIHJldHVybiB0aGlzLmhhcyhuYW1lKSA/IHRoaXMubWFwW25hbWVdIDogbnVsbFxuICB9O1xuXG4gIEhlYWRlcnMucHJvdG90eXBlLmhhcyA9IGZ1bmN0aW9uKG5hbWUpIHtcbiAgICByZXR1cm4gdGhpcy5tYXAuaGFzT3duUHJvcGVydHkobm9ybWFsaXplTmFtZShuYW1lKSlcbiAgfTtcblxuICBIZWFkZXJzLnByb3RvdHlwZS5zZXQgPSBmdW5jdGlvbihuYW1lLCB2YWx1ZSkge1xuICAgIHRoaXMubWFwW25vcm1hbGl6ZU5hbWUobmFtZSldID0gbm9ybWFsaXplVmFsdWUodmFsdWUpO1xuICB9O1xuXG4gIEhlYWRlcnMucHJvdG90eXBlLmZvckVhY2ggPSBmdW5jdGlvbihjYWxsYmFjaywgdGhpc0FyZykge1xuICAgIGZvciAodmFyIG5hbWUgaW4gdGhpcy5tYXApIHtcbiAgICAgIGlmICh0aGlzLm1hcC5oYXNPd25Qcm9wZXJ0eShuYW1lKSkge1xuICAgICAgICBjYWxsYmFjay5jYWxsKHRoaXNBcmcsIHRoaXMubWFwW25hbWVdLCBuYW1lLCB0aGlzKTtcbiAgICAgIH1cbiAgICB9XG4gIH07XG5cbiAgSGVhZGVycy5wcm90b3R5cGUua2V5cyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBpdGVtcyA9IFtdO1xuICAgIHRoaXMuZm9yRWFjaChmdW5jdGlvbih2YWx1ZSwgbmFtZSkge1xuICAgICAgaXRlbXMucHVzaChuYW1lKTtcbiAgICB9KTtcbiAgICByZXR1cm4gaXRlcmF0b3JGb3IoaXRlbXMpXG4gIH07XG5cbiAgSGVhZGVycy5wcm90b3R5cGUudmFsdWVzID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGl0ZW1zID0gW107XG4gICAgdGhpcy5mb3JFYWNoKGZ1bmN0aW9uKHZhbHVlKSB7XG4gICAgICBpdGVtcy5wdXNoKHZhbHVlKTtcbiAgICB9KTtcbiAgICByZXR1cm4gaXRlcmF0b3JGb3IoaXRlbXMpXG4gIH07XG5cbiAgSGVhZGVycy5wcm90b3R5cGUuZW50cmllcyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBpdGVtcyA9IFtdO1xuICAgIHRoaXMuZm9yRWFjaChmdW5jdGlvbih2YWx1ZSwgbmFtZSkge1xuICAgICAgaXRlbXMucHVzaChbbmFtZSwgdmFsdWVdKTtcbiAgICB9KTtcbiAgICByZXR1cm4gaXRlcmF0b3JGb3IoaXRlbXMpXG4gIH07XG5cbiAgaWYgKHN1cHBvcnQuaXRlcmFibGUpIHtcbiAgICBIZWFkZXJzLnByb3RvdHlwZVtTeW1ib2wuaXRlcmF0b3JdID0gSGVhZGVycy5wcm90b3R5cGUuZW50cmllcztcbiAgfVxuXG4gIGZ1bmN0aW9uIGNvbnN1bWVkKGJvZHkpIHtcbiAgICBpZiAoYm9keS5ib2R5VXNlZCkge1xuICAgICAgcmV0dXJuIFByb21pc2UucmVqZWN0KG5ldyBUeXBlRXJyb3IoJ0FscmVhZHkgcmVhZCcpKVxuICAgIH1cbiAgICBib2R5LmJvZHlVc2VkID0gdHJ1ZTtcbiAgfVxuXG4gIGZ1bmN0aW9uIGZpbGVSZWFkZXJSZWFkeShyZWFkZXIpIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2UoZnVuY3Rpb24ocmVzb2x2ZSwgcmVqZWN0KSB7XG4gICAgICByZWFkZXIub25sb2FkID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJlc29sdmUocmVhZGVyLnJlc3VsdCk7XG4gICAgICB9O1xuICAgICAgcmVhZGVyLm9uZXJyb3IgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmVqZWN0KHJlYWRlci5lcnJvcik7XG4gICAgICB9O1xuICAgIH0pXG4gIH1cblxuICBmdW5jdGlvbiByZWFkQmxvYkFzQXJyYXlCdWZmZXIoYmxvYikge1xuICAgIHZhciByZWFkZXIgPSBuZXcgRmlsZVJlYWRlcigpO1xuICAgIHZhciBwcm9taXNlID0gZmlsZVJlYWRlclJlYWR5KHJlYWRlcik7XG4gICAgcmVhZGVyLnJlYWRBc0FycmF5QnVmZmVyKGJsb2IpO1xuICAgIHJldHVybiBwcm9taXNlXG4gIH1cblxuICBmdW5jdGlvbiByZWFkQmxvYkFzVGV4dChibG9iKSB7XG4gICAgdmFyIHJlYWRlciA9IG5ldyBGaWxlUmVhZGVyKCk7XG4gICAgdmFyIHByb21pc2UgPSBmaWxlUmVhZGVyUmVhZHkocmVhZGVyKTtcbiAgICByZWFkZXIucmVhZEFzVGV4dChibG9iKTtcbiAgICByZXR1cm4gcHJvbWlzZVxuICB9XG5cbiAgZnVuY3Rpb24gcmVhZEFycmF5QnVmZmVyQXNUZXh0KGJ1Zikge1xuICAgIHZhciB2aWV3ID0gbmV3IFVpbnQ4QXJyYXkoYnVmKTtcbiAgICB2YXIgY2hhcnMgPSBuZXcgQXJyYXkodmlldy5sZW5ndGgpO1xuXG4gICAgZm9yICh2YXIgaSA9IDA7IGkgPCB2aWV3Lmxlbmd0aDsgaSsrKSB7XG4gICAgICBjaGFyc1tpXSA9IFN0cmluZy5mcm9tQ2hhckNvZGUodmlld1tpXSk7XG4gICAgfVxuICAgIHJldHVybiBjaGFycy5qb2luKCcnKVxuICB9XG5cbiAgZnVuY3Rpb24gYnVmZmVyQ2xvbmUoYnVmKSB7XG4gICAgaWYgKGJ1Zi5zbGljZSkge1xuICAgICAgcmV0dXJuIGJ1Zi5zbGljZSgwKVxuICAgIH0gZWxzZSB7XG4gICAgICB2YXIgdmlldyA9IG5ldyBVaW50OEFycmF5KGJ1Zi5ieXRlTGVuZ3RoKTtcbiAgICAgIHZpZXcuc2V0KG5ldyBVaW50OEFycmF5KGJ1ZikpO1xuICAgICAgcmV0dXJuIHZpZXcuYnVmZmVyXG4gICAgfVxuICB9XG5cbiAgZnVuY3Rpb24gQm9keSgpIHtcbiAgICB0aGlzLmJvZHlVc2VkID0gZmFsc2U7XG5cbiAgICB0aGlzLl9pbml0Qm9keSA9IGZ1bmN0aW9uKGJvZHkpIHtcbiAgICAgIHRoaXMuX2JvZHlJbml0ID0gYm9keTtcbiAgICAgIGlmICghYm9keSkge1xuICAgICAgICB0aGlzLl9ib2R5VGV4dCA9ICcnO1xuICAgICAgfSBlbHNlIGlmICh0eXBlb2YgYm9keSA9PT0gJ3N0cmluZycpIHtcbiAgICAgICAgdGhpcy5fYm9keVRleHQgPSBib2R5O1xuICAgICAgfSBlbHNlIGlmIChzdXBwb3J0LmJsb2IgJiYgQmxvYi5wcm90b3R5cGUuaXNQcm90b3R5cGVPZihib2R5KSkge1xuICAgICAgICB0aGlzLl9ib2R5QmxvYiA9IGJvZHk7XG4gICAgICB9IGVsc2UgaWYgKHN1cHBvcnQuZm9ybURhdGEgJiYgRm9ybURhdGEucHJvdG90eXBlLmlzUHJvdG90eXBlT2YoYm9keSkpIHtcbiAgICAgICAgdGhpcy5fYm9keUZvcm1EYXRhID0gYm9keTtcbiAgICAgIH0gZWxzZSBpZiAoc3VwcG9ydC5zZWFyY2hQYXJhbXMgJiYgVVJMU2VhcmNoUGFyYW1zLnByb3RvdHlwZS5pc1Byb3RvdHlwZU9mKGJvZHkpKSB7XG4gICAgICAgIHRoaXMuX2JvZHlUZXh0ID0gYm9keS50b1N0cmluZygpO1xuICAgICAgfSBlbHNlIGlmIChzdXBwb3J0LmFycmF5QnVmZmVyICYmIHN1cHBvcnQuYmxvYiAmJiBpc0RhdGFWaWV3KGJvZHkpKSB7XG4gICAgICAgIHRoaXMuX2JvZHlBcnJheUJ1ZmZlciA9IGJ1ZmZlckNsb25lKGJvZHkuYnVmZmVyKTtcbiAgICAgICAgLy8gSUUgMTAtMTEgY2FuJ3QgaGFuZGxlIGEgRGF0YVZpZXcgYm9keS5cbiAgICAgICAgdGhpcy5fYm9keUluaXQgPSBuZXcgQmxvYihbdGhpcy5fYm9keUFycmF5QnVmZmVyXSk7XG4gICAgICB9IGVsc2UgaWYgKHN1cHBvcnQuYXJyYXlCdWZmZXIgJiYgKEFycmF5QnVmZmVyLnByb3RvdHlwZS5pc1Byb3RvdHlwZU9mKGJvZHkpIHx8IGlzQXJyYXlCdWZmZXJWaWV3KGJvZHkpKSkge1xuICAgICAgICB0aGlzLl9ib2R5QXJyYXlCdWZmZXIgPSBidWZmZXJDbG9uZShib2R5KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX2JvZHlUZXh0ID0gYm9keSA9IE9iamVjdC5wcm90b3R5cGUudG9TdHJpbmcuY2FsbChib2R5KTtcbiAgICAgIH1cblxuICAgICAgaWYgKCF0aGlzLmhlYWRlcnMuZ2V0KCdjb250ZW50LXR5cGUnKSkge1xuICAgICAgICBpZiAodHlwZW9mIGJvZHkgPT09ICdzdHJpbmcnKSB7XG4gICAgICAgICAgdGhpcy5oZWFkZXJzLnNldCgnY29udGVudC10eXBlJywgJ3RleHQvcGxhaW47Y2hhcnNldD1VVEYtOCcpO1xuICAgICAgICB9IGVsc2UgaWYgKHRoaXMuX2JvZHlCbG9iICYmIHRoaXMuX2JvZHlCbG9iLnR5cGUpIHtcbiAgICAgICAgICB0aGlzLmhlYWRlcnMuc2V0KCdjb250ZW50LXR5cGUnLCB0aGlzLl9ib2R5QmxvYi50eXBlKTtcbiAgICAgICAgfSBlbHNlIGlmIChzdXBwb3J0LnNlYXJjaFBhcmFtcyAmJiBVUkxTZWFyY2hQYXJhbXMucHJvdG90eXBlLmlzUHJvdG90eXBlT2YoYm9keSkpIHtcbiAgICAgICAgICB0aGlzLmhlYWRlcnMuc2V0KCdjb250ZW50LXR5cGUnLCAnYXBwbGljYXRpb24veC13d3ctZm9ybS11cmxlbmNvZGVkO2NoYXJzZXQ9VVRGLTgnKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH07XG5cbiAgICBpZiAoc3VwcG9ydC5ibG9iKSB7XG4gICAgICB0aGlzLmJsb2IgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHJlamVjdGVkID0gY29uc3VtZWQodGhpcyk7XG4gICAgICAgIGlmIChyZWplY3RlZCkge1xuICAgICAgICAgIHJldHVybiByZWplY3RlZFxuICAgICAgICB9XG5cbiAgICAgICAgaWYgKHRoaXMuX2JvZHlCbG9iKSB7XG4gICAgICAgICAgcmV0dXJuIFByb21pc2UucmVzb2x2ZSh0aGlzLl9ib2R5QmxvYilcbiAgICAgICAgfSBlbHNlIGlmICh0aGlzLl9ib2R5QXJyYXlCdWZmZXIpIHtcbiAgICAgICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKG5ldyBCbG9iKFt0aGlzLl9ib2R5QXJyYXlCdWZmZXJdKSlcbiAgICAgICAgfSBlbHNlIGlmICh0aGlzLl9ib2R5Rm9ybURhdGEpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoJ2NvdWxkIG5vdCByZWFkIEZvcm1EYXRhIGJvZHkgYXMgYmxvYicpXG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIFByb21pc2UucmVzb2x2ZShuZXcgQmxvYihbdGhpcy5fYm9keVRleHRdKSlcbiAgICAgICAgfVxuICAgICAgfTtcblxuICAgICAgdGhpcy5hcnJheUJ1ZmZlciA9IGZ1bmN0aW9uKCkge1xuICAgICAgICBpZiAodGhpcy5fYm9keUFycmF5QnVmZmVyKSB7XG4gICAgICAgICAgcmV0dXJuIGNvbnN1bWVkKHRoaXMpIHx8IFByb21pc2UucmVzb2x2ZSh0aGlzLl9ib2R5QXJyYXlCdWZmZXIpXG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIHRoaXMuYmxvYigpLnRoZW4ocmVhZEJsb2JBc0FycmF5QnVmZmVyKVxuICAgICAgICB9XG4gICAgICB9O1xuICAgIH1cblxuICAgIHRoaXMudGV4dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgdmFyIHJlamVjdGVkID0gY29uc3VtZWQodGhpcyk7XG4gICAgICBpZiAocmVqZWN0ZWQpIHtcbiAgICAgICAgcmV0dXJuIHJlamVjdGVkXG4gICAgICB9XG5cbiAgICAgIGlmICh0aGlzLl9ib2R5QmxvYikge1xuICAgICAgICByZXR1cm4gcmVhZEJsb2JBc1RleHQodGhpcy5fYm9keUJsb2IpXG4gICAgICB9IGVsc2UgaWYgKHRoaXMuX2JvZHlBcnJheUJ1ZmZlcikge1xuICAgICAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKHJlYWRBcnJheUJ1ZmZlckFzVGV4dCh0aGlzLl9ib2R5QXJyYXlCdWZmZXIpKVxuICAgICAgfSBlbHNlIGlmICh0aGlzLl9ib2R5Rm9ybURhdGEpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdjb3VsZCBub3QgcmVhZCBGb3JtRGF0YSBib2R5IGFzIHRleHQnKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIFByb21pc2UucmVzb2x2ZSh0aGlzLl9ib2R5VGV4dClcbiAgICAgIH1cbiAgICB9O1xuXG4gICAgaWYgKHN1cHBvcnQuZm9ybURhdGEpIHtcbiAgICAgIHRoaXMuZm9ybURhdGEgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMudGV4dCgpLnRoZW4oZGVjb2RlKVxuICAgICAgfTtcbiAgICB9XG5cbiAgICB0aGlzLmpzb24gPSBmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiB0aGlzLnRleHQoKS50aGVuKEpTT04ucGFyc2UpXG4gICAgfTtcblxuICAgIHJldHVybiB0aGlzXG4gIH1cblxuICAvLyBIVFRQIG1ldGhvZHMgd2hvc2UgY2FwaXRhbGl6YXRpb24gc2hvdWxkIGJlIG5vcm1hbGl6ZWRcbiAgdmFyIG1ldGhvZHMgPSBbJ0RFTEVURScsICdHRVQnLCAnSEVBRCcsICdPUFRJT05TJywgJ1BPU1QnLCAnUFVUJ107XG5cbiAgZnVuY3Rpb24gbm9ybWFsaXplTWV0aG9kKG1ldGhvZCkge1xuICAgIHZhciB1cGNhc2VkID0gbWV0aG9kLnRvVXBwZXJDYXNlKCk7XG4gICAgcmV0dXJuIG1ldGhvZHMuaW5kZXhPZih1cGNhc2VkKSA+IC0xID8gdXBjYXNlZCA6IG1ldGhvZFxuICB9XG5cbiAgZnVuY3Rpb24gUmVxdWVzdChpbnB1dCwgb3B0aW9ucykge1xuICAgIG9wdGlvbnMgPSBvcHRpb25zIHx8IHt9O1xuICAgIHZhciBib2R5ID0gb3B0aW9ucy5ib2R5O1xuXG4gICAgaWYgKGlucHV0IGluc3RhbmNlb2YgUmVxdWVzdCkge1xuICAgICAgaWYgKGlucHV0LmJvZHlVc2VkKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ0FscmVhZHkgcmVhZCcpXG4gICAgICB9XG4gICAgICB0aGlzLnVybCA9IGlucHV0LnVybDtcbiAgICAgIHRoaXMuY3JlZGVudGlhbHMgPSBpbnB1dC5jcmVkZW50aWFscztcbiAgICAgIGlmICghb3B0aW9ucy5oZWFkZXJzKSB7XG4gICAgICAgIHRoaXMuaGVhZGVycyA9IG5ldyBIZWFkZXJzKGlucHV0LmhlYWRlcnMpO1xuICAgICAgfVxuICAgICAgdGhpcy5tZXRob2QgPSBpbnB1dC5tZXRob2Q7XG4gICAgICB0aGlzLm1vZGUgPSBpbnB1dC5tb2RlO1xuICAgICAgdGhpcy5zaWduYWwgPSBpbnB1dC5zaWduYWw7XG4gICAgICBpZiAoIWJvZHkgJiYgaW5wdXQuX2JvZHlJbml0ICE9IG51bGwpIHtcbiAgICAgICAgYm9keSA9IGlucHV0Ll9ib2R5SW5pdDtcbiAgICAgICAgaW5wdXQuYm9keVVzZWQgPSB0cnVlO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnVybCA9IFN0cmluZyhpbnB1dCk7XG4gICAgfVxuXG4gICAgdGhpcy5jcmVkZW50aWFscyA9IG9wdGlvbnMuY3JlZGVudGlhbHMgfHwgdGhpcy5jcmVkZW50aWFscyB8fCAnc2FtZS1vcmlnaW4nO1xuICAgIGlmIChvcHRpb25zLmhlYWRlcnMgfHwgIXRoaXMuaGVhZGVycykge1xuICAgICAgdGhpcy5oZWFkZXJzID0gbmV3IEhlYWRlcnMob3B0aW9ucy5oZWFkZXJzKTtcbiAgICB9XG4gICAgdGhpcy5tZXRob2QgPSBub3JtYWxpemVNZXRob2Qob3B0aW9ucy5tZXRob2QgfHwgdGhpcy5tZXRob2QgfHwgJ0dFVCcpO1xuICAgIHRoaXMubW9kZSA9IG9wdGlvbnMubW9kZSB8fCB0aGlzLm1vZGUgfHwgbnVsbDtcbiAgICB0aGlzLnNpZ25hbCA9IG9wdGlvbnMuc2lnbmFsIHx8IHRoaXMuc2lnbmFsO1xuICAgIHRoaXMucmVmZXJyZXIgPSBudWxsO1xuXG4gICAgaWYgKCh0aGlzLm1ldGhvZCA9PT0gJ0dFVCcgfHwgdGhpcy5tZXRob2QgPT09ICdIRUFEJykgJiYgYm9keSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignQm9keSBub3QgYWxsb3dlZCBmb3IgR0VUIG9yIEhFQUQgcmVxdWVzdHMnKVxuICAgIH1cbiAgICB0aGlzLl9pbml0Qm9keShib2R5KTtcbiAgfVxuXG4gIFJlcXVlc3QucHJvdG90eXBlLmNsb25lID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIG5ldyBSZXF1ZXN0KHRoaXMsIHtib2R5OiB0aGlzLl9ib2R5SW5pdH0pXG4gIH07XG5cbiAgZnVuY3Rpb24gZGVjb2RlKGJvZHkpIHtcbiAgICB2YXIgZm9ybSA9IG5ldyBGb3JtRGF0YSgpO1xuICAgIGJvZHlcbiAgICAgIC50cmltKClcbiAgICAgIC5zcGxpdCgnJicpXG4gICAgICAuZm9yRWFjaChmdW5jdGlvbihieXRlcykge1xuICAgICAgICBpZiAoYnl0ZXMpIHtcbiAgICAgICAgICB2YXIgc3BsaXQgPSBieXRlcy5zcGxpdCgnPScpO1xuICAgICAgICAgIHZhciBuYW1lID0gc3BsaXQuc2hpZnQoKS5yZXBsYWNlKC9cXCsvZywgJyAnKTtcbiAgICAgICAgICB2YXIgdmFsdWUgPSBzcGxpdC5qb2luKCc9JykucmVwbGFjZSgvXFwrL2csICcgJyk7XG4gICAgICAgICAgZm9ybS5hcHBlbmQoZGVjb2RlVVJJQ29tcG9uZW50KG5hbWUpLCBkZWNvZGVVUklDb21wb25lbnQodmFsdWUpKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgcmV0dXJuIGZvcm1cbiAgfVxuXG4gIGZ1bmN0aW9uIHBhcnNlSGVhZGVycyhyYXdIZWFkZXJzKSB7XG4gICAgdmFyIGhlYWRlcnMgPSBuZXcgSGVhZGVycygpO1xuICAgIC8vIFJlcGxhY2UgaW5zdGFuY2VzIG9mIFxcclxcbiBhbmQgXFxuIGZvbGxvd2VkIGJ5IGF0IGxlYXN0IG9uZSBzcGFjZSBvciBob3Jpem9udGFsIHRhYiB3aXRoIGEgc3BhY2VcbiAgICAvLyBodHRwczovL3Rvb2xzLmlldGYub3JnL2h0bWwvcmZjNzIzMCNzZWN0aW9uLTMuMlxuICAgIHZhciBwcmVQcm9jZXNzZWRIZWFkZXJzID0gcmF3SGVhZGVycy5yZXBsYWNlKC9cXHI/XFxuW1xcdCBdKy9nLCAnICcpO1xuICAgIHByZVByb2Nlc3NlZEhlYWRlcnMuc3BsaXQoL1xccj9cXG4vKS5mb3JFYWNoKGZ1bmN0aW9uKGxpbmUpIHtcbiAgICAgIHZhciBwYXJ0cyA9IGxpbmUuc3BsaXQoJzonKTtcbiAgICAgIHZhciBrZXkgPSBwYXJ0cy5zaGlmdCgpLnRyaW0oKTtcbiAgICAgIGlmIChrZXkpIHtcbiAgICAgICAgdmFyIHZhbHVlID0gcGFydHMuam9pbignOicpLnRyaW0oKTtcbiAgICAgICAgaGVhZGVycy5hcHBlbmQoa2V5LCB2YWx1ZSk7XG4gICAgICB9XG4gICAgfSk7XG4gICAgcmV0dXJuIGhlYWRlcnNcbiAgfVxuXG4gIEJvZHkuY2FsbChSZXF1ZXN0LnByb3RvdHlwZSk7XG5cbiAgZnVuY3Rpb24gUmVzcG9uc2UoYm9keUluaXQsIG9wdGlvbnMpIHtcbiAgICBpZiAoIW9wdGlvbnMpIHtcbiAgICAgIG9wdGlvbnMgPSB7fTtcbiAgICB9XG5cbiAgICB0aGlzLnR5cGUgPSAnZGVmYXVsdCc7XG4gICAgdGhpcy5zdGF0dXMgPSBvcHRpb25zLnN0YXR1cyA9PT0gdW5kZWZpbmVkID8gMjAwIDogb3B0aW9ucy5zdGF0dXM7XG4gICAgdGhpcy5vayA9IHRoaXMuc3RhdHVzID49IDIwMCAmJiB0aGlzLnN0YXR1cyA8IDMwMDtcbiAgICB0aGlzLnN0YXR1c1RleHQgPSAnc3RhdHVzVGV4dCcgaW4gb3B0aW9ucyA/IG9wdGlvbnMuc3RhdHVzVGV4dCA6ICdPSyc7XG4gICAgdGhpcy5oZWFkZXJzID0gbmV3IEhlYWRlcnMob3B0aW9ucy5oZWFkZXJzKTtcbiAgICB0aGlzLnVybCA9IG9wdGlvbnMudXJsIHx8ICcnO1xuICAgIHRoaXMuX2luaXRCb2R5KGJvZHlJbml0KTtcbiAgfVxuXG4gIEJvZHkuY2FsbChSZXNwb25zZS5wcm90b3R5cGUpO1xuXG4gIFJlc3BvbnNlLnByb3RvdHlwZS5jbG9uZSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBuZXcgUmVzcG9uc2UodGhpcy5fYm9keUluaXQsIHtcbiAgICAgIHN0YXR1czogdGhpcy5zdGF0dXMsXG4gICAgICBzdGF0dXNUZXh0OiB0aGlzLnN0YXR1c1RleHQsXG4gICAgICBoZWFkZXJzOiBuZXcgSGVhZGVycyh0aGlzLmhlYWRlcnMpLFxuICAgICAgdXJsOiB0aGlzLnVybFxuICAgIH0pXG4gIH07XG5cbiAgUmVzcG9uc2UuZXJyb3IgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgcmVzcG9uc2UgPSBuZXcgUmVzcG9uc2UobnVsbCwge3N0YXR1czogMCwgc3RhdHVzVGV4dDogJyd9KTtcbiAgICByZXNwb25zZS50eXBlID0gJ2Vycm9yJztcbiAgICByZXR1cm4gcmVzcG9uc2VcbiAgfTtcblxuICB2YXIgcmVkaXJlY3RTdGF0dXNlcyA9IFszMDEsIDMwMiwgMzAzLCAzMDcsIDMwOF07XG5cbiAgUmVzcG9uc2UucmVkaXJlY3QgPSBmdW5jdGlvbih1cmwsIHN0YXR1cykge1xuICAgIGlmIChyZWRpcmVjdFN0YXR1c2VzLmluZGV4T2Yoc3RhdHVzKSA9PT0gLTEpIHtcbiAgICAgIHRocm93IG5ldyBSYW5nZUVycm9yKCdJbnZhbGlkIHN0YXR1cyBjb2RlJylcbiAgICB9XG5cbiAgICByZXR1cm4gbmV3IFJlc3BvbnNlKG51bGwsIHtzdGF0dXM6IHN0YXR1cywgaGVhZGVyczoge2xvY2F0aW9uOiB1cmx9fSlcbiAgfTtcblxuICBleHBvcnRzLkRPTUV4Y2VwdGlvbiA9IHNlbGYuRE9NRXhjZXB0aW9uO1xuICB0cnkge1xuICAgIG5ldyBleHBvcnRzLkRPTUV4Y2VwdGlvbigpO1xuICB9IGNhdGNoIChlcnIpIHtcbiAgICBleHBvcnRzLkRPTUV4Y2VwdGlvbiA9IGZ1bmN0aW9uKG1lc3NhZ2UsIG5hbWUpIHtcbiAgICAgIHRoaXMubWVzc2FnZSA9IG1lc3NhZ2U7XG4gICAgICB0aGlzLm5hbWUgPSBuYW1lO1xuICAgICAgdmFyIGVycm9yID0gRXJyb3IobWVzc2FnZSk7XG4gICAgICB0aGlzLnN0YWNrID0gZXJyb3Iuc3RhY2s7XG4gICAgfTtcbiAgICBleHBvcnRzLkRPTUV4Y2VwdGlvbi5wcm90b3R5cGUgPSBPYmplY3QuY3JlYXRlKEVycm9yLnByb3RvdHlwZSk7XG4gICAgZXhwb3J0cy5ET01FeGNlcHRpb24ucHJvdG90eXBlLmNvbnN0cnVjdG9yID0gZXhwb3J0cy5ET01FeGNlcHRpb247XG4gIH1cblxuICBmdW5jdGlvbiBmZXRjaChpbnB1dCwgaW5pdCkge1xuICAgIHJldHVybiBuZXcgUHJvbWlzZShmdW5jdGlvbihyZXNvbHZlLCByZWplY3QpIHtcbiAgICAgIHZhciByZXF1ZXN0ID0gbmV3IFJlcXVlc3QoaW5wdXQsIGluaXQpO1xuXG4gICAgICBpZiAocmVxdWVzdC5zaWduYWwgJiYgcmVxdWVzdC5zaWduYWwuYWJvcnRlZCkge1xuICAgICAgICByZXR1cm4gcmVqZWN0KG5ldyBleHBvcnRzLkRPTUV4Y2VwdGlvbignQWJvcnRlZCcsICdBYm9ydEVycm9yJykpXG4gICAgICB9XG5cbiAgICAgIHZhciB4aHIgPSBuZXcgWE1MSHR0cFJlcXVlc3QoKTtcblxuICAgICAgZnVuY3Rpb24gYWJvcnRYaHIoKSB7XG4gICAgICAgIHhoci5hYm9ydCgpO1xuICAgICAgfVxuXG4gICAgICB4aHIub25sb2FkID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciBvcHRpb25zID0ge1xuICAgICAgICAgIHN0YXR1czogeGhyLnN0YXR1cyxcbiAgICAgICAgICBzdGF0dXNUZXh0OiB4aHIuc3RhdHVzVGV4dCxcbiAgICAgICAgICBoZWFkZXJzOiBwYXJzZUhlYWRlcnMoeGhyLmdldEFsbFJlc3BvbnNlSGVhZGVycygpIHx8ICcnKVxuICAgICAgICB9O1xuICAgICAgICBvcHRpb25zLnVybCA9ICdyZXNwb25zZVVSTCcgaW4geGhyID8geGhyLnJlc3BvbnNlVVJMIDogb3B0aW9ucy5oZWFkZXJzLmdldCgnWC1SZXF1ZXN0LVVSTCcpO1xuICAgICAgICB2YXIgYm9keSA9ICdyZXNwb25zZScgaW4geGhyID8geGhyLnJlc3BvbnNlIDogeGhyLnJlc3BvbnNlVGV4dDtcbiAgICAgICAgcmVzb2x2ZShuZXcgUmVzcG9uc2UoYm9keSwgb3B0aW9ucykpO1xuICAgICAgfTtcblxuICAgICAgeGhyLm9uZXJyb3IgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmVqZWN0KG5ldyBUeXBlRXJyb3IoJ05ldHdvcmsgcmVxdWVzdCBmYWlsZWQnKSk7XG4gICAgICB9O1xuXG4gICAgICB4aHIub250aW1lb3V0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJlamVjdChuZXcgVHlwZUVycm9yKCdOZXR3b3JrIHJlcXVlc3QgZmFpbGVkJykpO1xuICAgICAgfTtcblxuICAgICAgeGhyLm9uYWJvcnQgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmVqZWN0KG5ldyBleHBvcnRzLkRPTUV4Y2VwdGlvbignQWJvcnRlZCcsICdBYm9ydEVycm9yJykpO1xuICAgICAgfTtcblxuICAgICAgeGhyLm9wZW4ocmVxdWVzdC5tZXRob2QsIHJlcXVlc3QudXJsLCB0cnVlKTtcblxuICAgICAgaWYgKHJlcXVlc3QuY3JlZGVudGlhbHMgPT09ICdpbmNsdWRlJykge1xuICAgICAgICB4aHIud2l0aENyZWRlbnRpYWxzID0gdHJ1ZTtcbiAgICAgIH0gZWxzZSBpZiAocmVxdWVzdC5jcmVkZW50aWFscyA9PT0gJ29taXQnKSB7XG4gICAgICAgIHhoci53aXRoQ3JlZGVudGlhbHMgPSBmYWxzZTtcbiAgICAgIH1cblxuICAgICAgaWYgKCdyZXNwb25zZVR5cGUnIGluIHhociAmJiBzdXBwb3J0LmJsb2IpIHtcbiAgICAgICAgeGhyLnJlc3BvbnNlVHlwZSA9ICdibG9iJztcbiAgICAgIH1cblxuICAgICAgcmVxdWVzdC5oZWFkZXJzLmZvckVhY2goZnVuY3Rpb24odmFsdWUsIG5hbWUpIHtcbiAgICAgICAgeGhyLnNldFJlcXVlc3RIZWFkZXIobmFtZSwgdmFsdWUpO1xuICAgICAgfSk7XG5cbiAgICAgIGlmIChyZXF1ZXN0LnNpZ25hbCkge1xuICAgICAgICByZXF1ZXN0LnNpZ25hbC5hZGRFdmVudExpc3RlbmVyKCdhYm9ydCcsIGFib3J0WGhyKTtcblxuICAgICAgICB4aHIub25yZWFkeXN0YXRlY2hhbmdlID0gZnVuY3Rpb24oKSB7XG4gICAgICAgICAgLy8gRE9ORSAoc3VjY2VzcyBvciBmYWlsdXJlKVxuICAgICAgICAgIGlmICh4aHIucmVhZHlTdGF0ZSA9PT0gNCkge1xuICAgICAgICAgICAgcmVxdWVzdC5zaWduYWwucmVtb3ZlRXZlbnRMaXN0ZW5lcignYWJvcnQnLCBhYm9ydFhocik7XG4gICAgICAgICAgfVxuICAgICAgICB9O1xuICAgICAgfVxuXG4gICAgICB4aHIuc2VuZCh0eXBlb2YgcmVxdWVzdC5fYm9keUluaXQgPT09ICd1bmRlZmluZWQnID8gbnVsbCA6IHJlcXVlc3QuX2JvZHlJbml0KTtcbiAgICB9KVxuICB9XG5cbiAgZmV0Y2gucG9seWZpbGwgPSB0cnVlO1xuXG4gIGlmICghc2VsZi5mZXRjaCkge1xuICAgIHNlbGYuZmV0Y2ggPSBmZXRjaDtcbiAgICBzZWxmLkhlYWRlcnMgPSBIZWFkZXJzO1xuICAgIHNlbGYuUmVxdWVzdCA9IFJlcXVlc3Q7XG4gICAgc2VsZi5SZXNwb25zZSA9IFJlc3BvbnNlO1xuICB9XG5cbiAgZXhwb3J0cy5IZWFkZXJzID0gSGVhZGVycztcbiAgZXhwb3J0cy5SZXF1ZXN0ID0gUmVxdWVzdDtcbiAgZXhwb3J0cy5SZXNwb25zZSA9IFJlc3BvbnNlO1xuICBleHBvcnRzLmZldGNoID0gZmV0Y2g7XG5cbiAgT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsICdfX2VzTW9kdWxlJywgeyB2YWx1ZTogdHJ1ZSB9KTtcblxufSkpKTtcbiIsIi8qIGpzaGludCBub2RlOiB0cnVlICovXG4ndXNlIHN0cmljdCc7XG5cbi8qKlxuICAjIHdpbGRjYXJkXG5cbiAgVmVyeSBzaW1wbGUgd2lsZGNhcmQgbWF0Y2hpbmcsIHdoaWNoIGlzIGRlc2lnbmVkIHRvIHByb3ZpZGUgdGhlIHNhbWVcbiAgZnVuY3Rpb25hbGl0eSB0aGF0IGlzIGZvdW5kIGluIHRoZVxuICBbZXZlXShodHRwczovL2dpdGh1Yi5jb20vYWRvYmUtd2VicGxhdGZvcm0vZXZlKSBldmVudGluZyBsaWJyYXJ5LlxuXG4gICMjIFVzYWdlXG5cbiAgSXQgd29ya3Mgd2l0aCBzdHJpbmdzOlxuXG4gIDw8PCBleGFtcGxlcy9zdHJpbmdzLmpzXG5cbiAgQXJyYXlzOlxuXG4gIDw8PCBleGFtcGxlcy9hcnJheXMuanNcblxuICBPYmplY3RzIChtYXRjaGluZyBhZ2FpbnN0IGtleXMpOlxuXG4gIDw8PCBleGFtcGxlcy9vYmplY3RzLmpzXG5cbiAgV2hpbGUgdGhlIGxpYnJhcnkgd29ya3MgaW4gTm9kZSwgaWYgeW91IGFyZSBhcmUgbG9va2luZyBmb3IgZmlsZS1iYXNlZFxuICB3aWxkY2FyZCBtYXRjaGluZyB0aGVuIHlvdSBzaG91bGQgaGF2ZSBhIGxvb2sgYXQ6XG5cbiAgPGh0dHBzOi8vZ2l0aHViLmNvbS9pc2FhY3Mvbm9kZS1nbG9iPlxuKiovXG5cbmZ1bmN0aW9uIFdpbGRjYXJkTWF0Y2hlcih0ZXh0LCBzZXBhcmF0b3IpIHtcbiAgdGhpcy50ZXh0ID0gdGV4dCA9IHRleHQgfHwgJyc7XG4gIHRoaXMuaGFzV2lsZCA9IH50ZXh0LmluZGV4T2YoJyonKTtcbiAgdGhpcy5zZXBhcmF0b3IgPSBzZXBhcmF0b3I7XG4gIHRoaXMucGFydHMgPSB0ZXh0LnNwbGl0KHNlcGFyYXRvcik7XG59XG5cbldpbGRjYXJkTWF0Y2hlci5wcm90b3R5cGUubWF0Y2ggPSBmdW5jdGlvbihpbnB1dCkge1xuICB2YXIgbWF0Y2hlcyA9IHRydWU7XG4gIHZhciBwYXJ0cyA9IHRoaXMucGFydHM7XG4gIHZhciBpaTtcbiAgdmFyIHBhcnRzQ291bnQgPSBwYXJ0cy5sZW5ndGg7XG4gIHZhciB0ZXN0UGFydHM7XG5cbiAgaWYgKHR5cGVvZiBpbnB1dCA9PSAnc3RyaW5nJyB8fCBpbnB1dCBpbnN0YW5jZW9mIFN0cmluZykge1xuICAgIGlmICghdGhpcy5oYXNXaWxkICYmIHRoaXMudGV4dCAhPSBpbnB1dCkge1xuICAgICAgbWF0Y2hlcyA9IGZhbHNlO1xuICAgIH0gZWxzZSB7XG4gICAgICB0ZXN0UGFydHMgPSAoaW5wdXQgfHwgJycpLnNwbGl0KHRoaXMuc2VwYXJhdG9yKTtcbiAgICAgIGZvciAoaWkgPSAwOyBtYXRjaGVzICYmIGlpIDwgcGFydHNDb3VudDsgaWkrKykge1xuICAgICAgICBpZiAocGFydHNbaWldID09PSAnKicpICB7XG4gICAgICAgICAgY29udGludWU7XG4gICAgICAgIH0gZWxzZSBpZiAoaWkgPCB0ZXN0UGFydHMubGVuZ3RoKSB7XG4gICAgICAgICAgbWF0Y2hlcyA9IHBhcnRzW2lpXSA9PT0gdGVzdFBhcnRzW2lpXTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBtYXRjaGVzID0gZmFsc2U7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgLy8gSWYgbWF0Y2hlcywgdGhlbiByZXR1cm4gdGhlIGNvbXBvbmVudCBwYXJ0c1xuICAgICAgbWF0Y2hlcyA9IG1hdGNoZXMgJiYgdGVzdFBhcnRzO1xuICAgIH1cbiAgfVxuICBlbHNlIGlmICh0eXBlb2YgaW5wdXQuc3BsaWNlID09ICdmdW5jdGlvbicpIHtcbiAgICBtYXRjaGVzID0gW107XG5cbiAgICBmb3IgKGlpID0gaW5wdXQubGVuZ3RoOyBpaS0tOyApIHtcbiAgICAgIGlmICh0aGlzLm1hdGNoKGlucHV0W2lpXSkpIHtcbiAgICAgICAgbWF0Y2hlc1ttYXRjaGVzLmxlbmd0aF0gPSBpbnB1dFtpaV07XG4gICAgICB9XG4gICAgfVxuICB9XG4gIGVsc2UgaWYgKHR5cGVvZiBpbnB1dCA9PSAnb2JqZWN0Jykge1xuICAgIG1hdGNoZXMgPSB7fTtcblxuICAgIGZvciAodmFyIGtleSBpbiBpbnB1dCkge1xuICAgICAgaWYgKHRoaXMubWF0Y2goa2V5KSkge1xuICAgICAgICBtYXRjaGVzW2tleV0gPSBpbnB1dFtrZXldO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiBtYXRjaGVzO1xufTtcblxubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbih0ZXh0LCB0ZXN0LCBzZXBhcmF0b3IpIHtcbiAgdmFyIG1hdGNoZXIgPSBuZXcgV2lsZGNhcmRNYXRjaGVyKHRleHQsIHNlcGFyYXRvciB8fCAvW1xcL1xcLl0vKTtcbiAgaWYgKHR5cGVvZiB0ZXN0ICE9ICd1bmRlZmluZWQnKSB7XG4gICAgcmV0dXJuIG1hdGNoZXIubWF0Y2godGVzdCk7XG4gIH1cblxuICByZXR1cm4gbWF0Y2hlcjtcbn07XG4iLCJtb2R1bGUuZXhwb3J0cz17XG4gIFwibmFtZVwiOiBcIkB1cHB5L2NvbXBhbmlvbi1jbGllbnRcIixcbiAgXCJkZXNjcmlwdGlvblwiOiBcIkNsaWVudCBsaWJyYXJ5IGZvciBjb21tdW5pY2F0aW9uIHdpdGggQ29tcGFuaW9uLiBJbnRlbmRlZCBmb3IgdXNlIGluIFVwcHkgcGx1Z2lucy5cIixcbiAgXCJ2ZXJzaW9uXCI6IFwiMS40LjFcIixcbiAgXCJsaWNlbnNlXCI6IFwiTUlUXCIsXG4gIFwibWFpblwiOiBcImxpYi9pbmRleC5qc1wiLFxuICBcInR5cGVzXCI6IFwidHlwZXMvaW5kZXguZC50c1wiLFxuICBcImtleXdvcmRzXCI6IFtcbiAgICBcImZpbGUgdXBsb2FkZXJcIixcbiAgICBcInVwcHlcIixcbiAgICBcInVwcHktcGx1Z2luXCIsXG4gICAgXCJjb21wYW5pb25cIixcbiAgICBcInByb3ZpZGVyXCJcbiAgXSxcbiAgXCJob21lcGFnZVwiOiBcImh0dHBzOi8vdXBweS5pb1wiLFxuICBcImJ1Z3NcIjoge1xuICAgIFwidXJsXCI6IFwiaHR0cHM6Ly9naXRodWIuY29tL3RyYW5zbG9hZGl0L3VwcHkvaXNzdWVzXCJcbiAgfSxcbiAgXCJyZXBvc2l0b3J5XCI6IHtcbiAgICBcInR5cGVcIjogXCJnaXRcIixcbiAgICBcInVybFwiOiBcImdpdCtodHRwczovL2dpdGh1Yi5jb20vdHJhbnNsb2FkaXQvdXBweS5naXRcIlxuICB9LFxuICBcImRlcGVuZGVuY2llc1wiOiB7XG4gICAgXCJuYW1lc3BhY2UtZW1pdHRlclwiOiBcIl4yLjAuMVwiXG4gIH1cbn1cbiIsIid1c2Ugc3RyaWN0J1xuXG5jbGFzcyBBdXRoRXJyb3IgZXh0ZW5kcyBFcnJvciB7XG4gIGNvbnN0cnVjdG9yICgpIHtcbiAgICBzdXBlcignQXV0aG9yaXphdGlvbiByZXF1aXJlZCcpXG4gICAgdGhpcy5uYW1lID0gJ0F1dGhFcnJvcidcbiAgICB0aGlzLmlzQXV0aEVycm9yID0gdHJ1ZVxuICB9XG59XG5cbm1vZHVsZS5leHBvcnRzID0gQXV0aEVycm9yXG4iLCIndXNlIHN0cmljdCdcblxuY29uc3QgUmVxdWVzdENsaWVudCA9IHJlcXVpcmUoJy4vUmVxdWVzdENsaWVudCcpXG5jb25zdCB0b2tlblN0b3JhZ2UgPSByZXF1aXJlKCcuL3Rva2VuU3RvcmFnZScpXG5cbmNvbnN0IF9nZXROYW1lID0gKGlkKSA9PiB7XG4gIHJldHVybiBpZC5zcGxpdCgnLScpLm1hcCgocykgPT4gcy5jaGFyQXQoMCkudG9VcHBlckNhc2UoKSArIHMuc2xpY2UoMSkpLmpvaW4oJyAnKVxufVxuXG5tb2R1bGUuZXhwb3J0cyA9IGNsYXNzIFByb3ZpZGVyIGV4dGVuZHMgUmVxdWVzdENsaWVudCB7XG4gIGNvbnN0cnVjdG9yICh1cHB5LCBvcHRzKSB7XG4gICAgc3VwZXIodXBweSwgb3B0cylcbiAgICB0aGlzLnByb3ZpZGVyID0gb3B0cy5wcm92aWRlclxuICAgIHRoaXMuaWQgPSB0aGlzLnByb3ZpZGVyXG4gICAgdGhpcy5hdXRoUHJvdmlkZXIgPSBvcHRzLmF1dGhQcm92aWRlciB8fCB0aGlzLnByb3ZpZGVyXG4gICAgdGhpcy5uYW1lID0gdGhpcy5vcHRzLm5hbWUgfHwgX2dldE5hbWUodGhpcy5pZClcbiAgICB0aGlzLnBsdWdpbklkID0gdGhpcy5vcHRzLnBsdWdpbklkXG4gICAgdGhpcy50b2tlbktleSA9IGBjb21wYW5pb24tJHt0aGlzLnBsdWdpbklkfS1hdXRoLXRva2VuYFxuICB9XG5cbiAgaGVhZGVycyAoKSB7XG4gICAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgIHN1cGVyLmhlYWRlcnMoKS50aGVuKChoZWFkZXJzKSA9PiB7XG4gICAgICAgIHRoaXMuZ2V0QXV0aFRva2VuKCkudGhlbigodG9rZW4pID0+IHtcbiAgICAgICAgICByZXNvbHZlKE9iamVjdC5hc3NpZ24oe30sIGhlYWRlcnMsIHsgJ3VwcHktYXV0aC10b2tlbic6IHRva2VuIH0pKVxuICAgICAgICB9KVxuICAgICAgfSkuY2F0Y2gocmVqZWN0KVxuICAgIH0pXG4gIH1cblxuICBvblJlY2VpdmVSZXNwb25zZSAocmVzcG9uc2UpIHtcbiAgICByZXNwb25zZSA9IHN1cGVyLm9uUmVjZWl2ZVJlc3BvbnNlKHJlc3BvbnNlKVxuICAgIGNvbnN0IHBsdWdpbiA9IHRoaXMudXBweS5nZXRQbHVnaW4odGhpcy5wbHVnaW5JZClcbiAgICBjb25zdCBvbGRBdXRoZW50aWNhdGVkID0gcGx1Z2luLmdldFBsdWdpblN0YXRlKCkuYXV0aGVudGljYXRlZFxuICAgIGNvbnN0IGF1dGhlbnRpY2F0ZWQgPSBvbGRBdXRoZW50aWNhdGVkID8gcmVzcG9uc2Uuc3RhdHVzICE9PSA0MDEgOiByZXNwb25zZS5zdGF0dXMgPCA0MDBcbiAgICBwbHVnaW4uc2V0UGx1Z2luU3RhdGUoeyBhdXRoZW50aWNhdGVkIH0pXG4gICAgcmV0dXJuIHJlc3BvbnNlXG4gIH1cblxuICAvLyBAdG9kbyhpLm9sYXJld2FqdSkgY29uc2lkZXIgd2hldGhlciBvciBub3QgdGhpcyBtZXRob2Qgc2hvdWxkIGJlIGV4cG9zZWRcbiAgc2V0QXV0aFRva2VuICh0b2tlbikge1xuICAgIHJldHVybiB0aGlzLnVwcHkuZ2V0UGx1Z2luKHRoaXMucGx1Z2luSWQpLnN0b3JhZ2Uuc2V0SXRlbSh0aGlzLnRva2VuS2V5LCB0b2tlbilcbiAgfVxuXG4gIGdldEF1dGhUb2tlbiAoKSB7XG4gICAgcmV0dXJuIHRoaXMudXBweS5nZXRQbHVnaW4odGhpcy5wbHVnaW5JZCkuc3RvcmFnZS5nZXRJdGVtKHRoaXMudG9rZW5LZXkpXG4gIH1cblxuICBhdXRoVXJsICgpIHtcbiAgICByZXR1cm4gYCR7dGhpcy5ob3N0bmFtZX0vJHt0aGlzLmlkfS9jb25uZWN0YFxuICB9XG5cbiAgZmlsZVVybCAoaWQpIHtcbiAgICByZXR1cm4gYCR7dGhpcy5ob3N0bmFtZX0vJHt0aGlzLmlkfS9nZXQvJHtpZH1gXG4gIH1cblxuICBsaXN0IChkaXJlY3RvcnkpIHtcbiAgICByZXR1cm4gdGhpcy5nZXQoYCR7dGhpcy5pZH0vbGlzdC8ke2RpcmVjdG9yeSB8fCAnJ31gKVxuICB9XG5cbiAgbG9nb3V0ICgpIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgdGhpcy5nZXQoYCR7dGhpcy5pZH0vbG9nb3V0YClcbiAgICAgICAgLnRoZW4oKHJlcykgPT4ge1xuICAgICAgICAgIHRoaXMudXBweS5nZXRQbHVnaW4odGhpcy5wbHVnaW5JZCkuc3RvcmFnZS5yZW1vdmVJdGVtKHRoaXMudG9rZW5LZXkpXG4gICAgICAgICAgICAudGhlbigoKSA9PiByZXNvbHZlKHJlcykpXG4gICAgICAgICAgICAuY2F0Y2gocmVqZWN0KVxuICAgICAgICB9KS5jYXRjaChyZWplY3QpXG4gICAgfSlcbiAgfVxuXG4gIHN0YXRpYyBpbml0UGx1Z2luIChwbHVnaW4sIG9wdHMsIGRlZmF1bHRPcHRzKSB7XG4gICAgcGx1Z2luLnR5cGUgPSAnYWNxdWlyZXInXG4gICAgcGx1Z2luLmZpbGVzID0gW11cbiAgICBpZiAoZGVmYXVsdE9wdHMpIHtcbiAgICAgIHBsdWdpbi5vcHRzID0gT2JqZWN0LmFzc2lnbih7fSwgZGVmYXVsdE9wdHMsIG9wdHMpXG4gICAgfVxuXG4gICAgaWYgKG9wdHMuc2VydmVyVXJsIHx8IG9wdHMuc2VydmVyUGF0dGVybikge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdgc2VydmVyVXJsYCBhbmQgYHNlcnZlclBhdHRlcm5gIGhhdmUgYmVlbiByZW5hbWVkIHRvIGBjb21wYW5pb25VcmxgIGFuZCBgY29tcGFuaW9uQWxsb3dlZEhvc3RzYCByZXNwZWN0aXZlbHkgaW4gdGhlIDAuMzAuNSByZWxlYXNlLiBQbGVhc2UgY29uc3VsdCB0aGUgZG9jcyAoZm9yIGV4YW1wbGUsIGh0dHBzOi8vdXBweS5pby9kb2NzL2luc3RhZ3JhbS8gZm9yIHRoZSBJbnN0YWdyYW0gcGx1Z2luKSBhbmQgdXNlIHRoZSB1cGRhdGVkIG9wdGlvbnMuYCcpXG4gICAgfVxuXG4gICAgaWYgKG9wdHMuY29tcGFuaW9uQWxsb3dlZEhvc3RzKSB7XG4gICAgICBjb25zdCBwYXR0ZXJuID0gb3B0cy5jb21wYW5pb25BbGxvd2VkSG9zdHNcbiAgICAgIC8vIHZhbGlkYXRlIGNvbXBhbmlvbkFsbG93ZWRIb3N0cyBwYXJhbVxuICAgICAgaWYgKHR5cGVvZiBwYXR0ZXJuICE9PSAnc3RyaW5nJyAmJiAhQXJyYXkuaXNBcnJheShwYXR0ZXJuKSAmJiAhKHBhdHRlcm4gaW5zdGFuY2VvZiBSZWdFeHApKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoYCR7cGx1Z2luLmlkfTogdGhlIG9wdGlvbiBcImNvbXBhbmlvbkFsbG93ZWRIb3N0c1wiIG11c3QgYmUgb25lIG9mIHN0cmluZywgQXJyYXksIFJlZ0V4cGApXG4gICAgICB9XG4gICAgICBwbHVnaW4ub3B0cy5jb21wYW5pb25BbGxvd2VkSG9zdHMgPSBwYXR0ZXJuXG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIGRvZXMgbm90IHN0YXJ0IHdpdGggaHR0cHM6Ly9cbiAgICAgIGlmICgvXig/IWh0dHBzPzpcXC9cXC8pLiokL2kudGVzdChvcHRzLmNvbXBhbmlvblVybCkpIHtcbiAgICAgICAgcGx1Z2luLm9wdHMuY29tcGFuaW9uQWxsb3dlZEhvc3RzID0gYGh0dHBzOi8vJHtvcHRzLmNvbXBhbmlvblVybC5yZXBsYWNlKC9eXFwvXFwvLywgJycpfWBcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHBsdWdpbi5vcHRzLmNvbXBhbmlvbkFsbG93ZWRIb3N0cyA9IG9wdHMuY29tcGFuaW9uVXJsXG4gICAgICB9XG4gICAgfVxuXG4gICAgcGx1Z2luLnN0b3JhZ2UgPSBwbHVnaW4ub3B0cy5zdG9yYWdlIHx8IHRva2VuU3RvcmFnZVxuICB9XG59XG4iLCIndXNlIHN0cmljdCdcblxuY29uc3QgQXV0aEVycm9yID0gcmVxdWlyZSgnLi9BdXRoRXJyb3InKVxuXG4vLyBSZW1vdmUgdGhlIHRyYWlsaW5nIHNsYXNoIHNvIHdlIGNhbiBhbHdheXMgc2FmZWx5IGFwcGVuZCAveHl6LlxuZnVuY3Rpb24gc3RyaXBTbGFzaCAodXJsKSB7XG4gIHJldHVybiB1cmwucmVwbGFjZSgvXFwvJC8sICcnKVxufVxuXG5tb2R1bGUuZXhwb3J0cyA9IGNsYXNzIFJlcXVlc3RDbGllbnQge1xuICBzdGF0aWMgVkVSU0lPTiA9IHJlcXVpcmUoJy4uL3BhY2thZ2UuanNvbicpLnZlcnNpb25cblxuICBjb25zdHJ1Y3RvciAodXBweSwgb3B0cykge1xuICAgIHRoaXMudXBweSA9IHVwcHlcbiAgICB0aGlzLm9wdHMgPSBvcHRzXG4gICAgdGhpcy5vblJlY2VpdmVSZXNwb25zZSA9IHRoaXMub25SZWNlaXZlUmVzcG9uc2UuYmluZCh0aGlzKVxuICAgIHRoaXMuYWxsb3dlZEhlYWRlcnMgPSBbJ2FjY2VwdCcsICdjb250ZW50LXR5cGUnLCAndXBweS1hdXRoLXRva2VuJ11cbiAgICB0aGlzLnByZWZsaWdodERvbmUgPSBmYWxzZVxuICB9XG5cbiAgZ2V0IGhvc3RuYW1lICgpIHtcbiAgICBjb25zdCB7IGNvbXBhbmlvbiB9ID0gdGhpcy51cHB5LmdldFN0YXRlKClcbiAgICBjb25zdCBob3N0ID0gdGhpcy5vcHRzLmNvbXBhbmlvblVybFxuICAgIHJldHVybiBzdHJpcFNsYXNoKGNvbXBhbmlvbiAmJiBjb21wYW5pb25baG9zdF0gPyBjb21wYW5pb25baG9zdF0gOiBob3N0KVxuICB9XG5cbiAgZ2V0IGRlZmF1bHRIZWFkZXJzICgpIHtcbiAgICByZXR1cm4ge1xuICAgICAgQWNjZXB0OiAnYXBwbGljYXRpb24vanNvbicsXG4gICAgICAnQ29udGVudC1UeXBlJzogJ2FwcGxpY2F0aW9uL2pzb24nLFxuICAgICAgJ1VwcHktVmVyc2lvbnMnOiBgQHVwcHkvY29tcGFuaW9uLWNsaWVudD0ke1JlcXVlc3RDbGllbnQuVkVSU0lPTn1gXG4gICAgfVxuICB9XG5cbiAgaGVhZGVycyAoKSB7XG4gICAgY29uc3QgdXNlckhlYWRlcnMgPSB0aGlzLm9wdHMuY29tcGFuaW9uSGVhZGVycyB8fCB0aGlzLm9wdHMuc2VydmVySGVhZGVycyB8fCB7fVxuICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUoe1xuICAgICAgLi4udGhpcy5kZWZhdWx0SGVhZGVycyxcbiAgICAgIC4uLnVzZXJIZWFkZXJzXG4gICAgfSlcbiAgfVxuXG4gIF9nZXRQb3N0UmVzcG9uc2VGdW5jIChza2lwKSB7XG4gICAgcmV0dXJuIChyZXNwb25zZSkgPT4ge1xuICAgICAgaWYgKCFza2lwKSB7XG4gICAgICAgIHJldHVybiB0aGlzLm9uUmVjZWl2ZVJlc3BvbnNlKHJlc3BvbnNlKVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gcmVzcG9uc2VcbiAgICB9XG4gIH1cblxuICBvblJlY2VpdmVSZXNwb25zZSAocmVzcG9uc2UpIHtcbiAgICBjb25zdCBzdGF0ZSA9IHRoaXMudXBweS5nZXRTdGF0ZSgpXG4gICAgY29uc3QgY29tcGFuaW9uID0gc3RhdGUuY29tcGFuaW9uIHx8IHt9XG4gICAgY29uc3QgaG9zdCA9IHRoaXMub3B0cy5jb21wYW5pb25VcmxcbiAgICBjb25zdCBoZWFkZXJzID0gcmVzcG9uc2UuaGVhZGVyc1xuICAgIC8vIFN0b3JlIHRoZSBzZWxmLWlkZW50aWZpZWQgZG9tYWluIG5hbWUgZm9yIHRoZSBDb21wYW5pb24gaW5zdGFuY2Ugd2UganVzdCBoaXQuXG4gICAgaWYgKGhlYWRlcnMuaGFzKCdpLWFtJykgJiYgaGVhZGVycy5nZXQoJ2ktYW0nKSAhPT0gY29tcGFuaW9uW2hvc3RdKSB7XG4gICAgICB0aGlzLnVwcHkuc2V0U3RhdGUoe1xuICAgICAgICBjb21wYW5pb246IE9iamVjdC5hc3NpZ24oe30sIGNvbXBhbmlvbiwge1xuICAgICAgICAgIFtob3N0XTogaGVhZGVycy5nZXQoJ2ktYW0nKVxuICAgICAgICB9KVxuICAgICAgfSlcbiAgICB9XG4gICAgcmV0dXJuIHJlc3BvbnNlXG4gIH1cblxuICBfZ2V0VXJsICh1cmwpIHtcbiAgICBpZiAoL14oaHR0cHM/OnwpXFwvXFwvLy50ZXN0KHVybCkpIHtcbiAgICAgIHJldHVybiB1cmxcbiAgICB9XG4gICAgcmV0dXJuIGAke3RoaXMuaG9zdG5hbWV9LyR7dXJsfWBcbiAgfVxuXG4gIF9qc29uIChyZXMpIHtcbiAgICBpZiAocmVzLnN0YXR1cyA9PT0gNDAxKSB7XG4gICAgICB0aHJvdyBuZXcgQXV0aEVycm9yKClcbiAgICB9XG5cbiAgICBpZiAocmVzLnN0YXR1cyA8IDIwMCB8fCByZXMuc3RhdHVzID4gMzAwKSB7XG4gICAgICBsZXQgZXJyTXNnID0gYEZhaWxlZCByZXF1ZXN0IHdpdGggc3RhdHVzOiAke3Jlcy5zdGF0dXN9LiAke3Jlcy5zdGF0dXNUZXh0fWBcbiAgICAgIHJldHVybiByZXMuanNvbigpXG4gICAgICAgIC50aGVuKChlcnJEYXRhKSA9PiB7XG4gICAgICAgICAgZXJyTXNnID0gZXJyRGF0YS5tZXNzYWdlID8gYCR7ZXJyTXNnfSBtZXNzYWdlOiAke2VyckRhdGEubWVzc2FnZX1gIDogZXJyTXNnXG4gICAgICAgICAgZXJyTXNnID0gZXJyRGF0YS5yZXF1ZXN0SWQgPyBgJHtlcnJNc2d9IHJlcXVlc3QtSWQ6ICR7ZXJyRGF0YS5yZXF1ZXN0SWR9YCA6IGVyck1zZ1xuICAgICAgICAgIHRocm93IG5ldyBFcnJvcihlcnJNc2cpXG4gICAgICAgIH0pLmNhdGNoKCgpID0+IHsgdGhyb3cgbmV3IEVycm9yKGVyck1zZykgfSlcbiAgICB9XG4gICAgcmV0dXJuIHJlcy5qc29uKClcbiAgfVxuXG4gIHByZWZsaWdodCAocGF0aCkge1xuICAgIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG4gICAgICBpZiAodGhpcy5wcmVmbGlnaHREb25lKSB7XG4gICAgICAgIHJldHVybiByZXNvbHZlKHRoaXMuYWxsb3dlZEhlYWRlcnMuc2xpY2UoKSlcbiAgICAgIH1cblxuICAgICAgZmV0Y2godGhpcy5fZ2V0VXJsKHBhdGgpLCB7XG4gICAgICAgIG1ldGhvZDogJ09QVElPTlMnXG4gICAgICB9KVxuICAgICAgICAudGhlbigocmVzcG9uc2UpID0+IHtcbiAgICAgICAgICBpZiAocmVzcG9uc2UuaGVhZGVycy5oYXMoJ2FjY2Vzcy1jb250cm9sLWFsbG93LWhlYWRlcnMnKSkge1xuICAgICAgICAgICAgdGhpcy5hbGxvd2VkSGVhZGVycyA9IHJlc3BvbnNlLmhlYWRlcnMuZ2V0KCdhY2Nlc3MtY29udHJvbC1hbGxvdy1oZWFkZXJzJylcbiAgICAgICAgICAgICAgLnNwbGl0KCcsJykubWFwKChoZWFkZXJOYW1lKSA9PiBoZWFkZXJOYW1lLnRyaW0oKS50b0xvd2VyQ2FzZSgpKVxuICAgICAgICAgIH1cbiAgICAgICAgICB0aGlzLnByZWZsaWdodERvbmUgPSB0cnVlXG4gICAgICAgICAgcmVzb2x2ZSh0aGlzLmFsbG93ZWRIZWFkZXJzLnNsaWNlKCkpXG4gICAgICAgIH0pXG4gICAgICAgIC5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICAgICAgdGhpcy51cHB5LmxvZyhgW0NvbXBhbmlvbkNsaWVudF0gdW5hYmxlIHRvIG1ha2UgcHJlZmxpZ2h0IHJlcXVlc3QgJHtlcnJ9YCwgJ3dhcm5pbmcnKVxuICAgICAgICAgIHRoaXMucHJlZmxpZ2h0RG9uZSA9IHRydWVcbiAgICAgICAgICByZXNvbHZlKHRoaXMuYWxsb3dlZEhlYWRlcnMuc2xpY2UoKSlcbiAgICAgICAgfSlcbiAgICB9KVxuICB9XG5cbiAgcHJlZmxpZ2h0QW5kSGVhZGVycyAocGF0aCkge1xuICAgIHJldHVybiBQcm9taXNlLmFsbChbdGhpcy5wcmVmbGlnaHQocGF0aCksIHRoaXMuaGVhZGVycygpXSlcbiAgICAgIC50aGVuKChbYWxsb3dlZEhlYWRlcnMsIGhlYWRlcnNdKSA9PiB7XG4gICAgICAgIC8vIGZpbHRlciB0byBrZWVwIG9ubHkgYWxsb3dlZCBIZWFkZXJzXG4gICAgICAgIE9iamVjdC5rZXlzKGhlYWRlcnMpLmZvckVhY2goKGhlYWRlcikgPT4ge1xuICAgICAgICAgIGlmIChhbGxvd2VkSGVhZGVycy5pbmRleE9mKGhlYWRlci50b0xvd2VyQ2FzZSgpKSA9PT0gLTEpIHtcbiAgICAgICAgICAgIHRoaXMudXBweS5sb2coYFtDb21wYW5pb25DbGllbnRdIGV4Y2x1ZGluZyB1bmFsbG93ZWQgaGVhZGVyICR7aGVhZGVyfWApXG4gICAgICAgICAgICBkZWxldGUgaGVhZGVyc1toZWFkZXJdXG4gICAgICAgICAgfVxuICAgICAgICB9KVxuXG4gICAgICAgIHJldHVybiBoZWFkZXJzXG4gICAgICB9KVxuICB9XG5cbiAgZ2V0IChwYXRoLCBza2lwUG9zdFJlc3BvbnNlKSB7XG4gICAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgIHRoaXMucHJlZmxpZ2h0QW5kSGVhZGVycyhwYXRoKS50aGVuKChoZWFkZXJzKSA9PiB7XG4gICAgICAgIGZldGNoKHRoaXMuX2dldFVybChwYXRoKSwge1xuICAgICAgICAgIG1ldGhvZDogJ2dldCcsXG4gICAgICAgICAgaGVhZGVyczogaGVhZGVycyxcbiAgICAgICAgICBjcmVkZW50aWFsczogJ3NhbWUtb3JpZ2luJ1xuICAgICAgICB9KVxuICAgICAgICAgIC50aGVuKHRoaXMuX2dldFBvc3RSZXNwb25zZUZ1bmMoc2tpcFBvc3RSZXNwb25zZSkpXG4gICAgICAgICAgLnRoZW4oKHJlcykgPT4gdGhpcy5fanNvbihyZXMpLnRoZW4ocmVzb2x2ZSkpXG4gICAgICAgICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgICAgIGVyciA9IGVyci5pc0F1dGhFcnJvciA/IGVyciA6IG5ldyBFcnJvcihgQ291bGQgbm90IGdldCAke3RoaXMuX2dldFVybChwYXRoKX0uICR7ZXJyfWApXG4gICAgICAgICAgICByZWplY3QoZXJyKVxuICAgICAgICAgIH0pXG4gICAgICB9KS5jYXRjaChyZWplY3QpXG4gICAgfSlcbiAgfVxuXG4gIHBvc3QgKHBhdGgsIGRhdGEsIHNraXBQb3N0UmVzcG9uc2UpIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgdGhpcy5wcmVmbGlnaHRBbmRIZWFkZXJzKHBhdGgpLnRoZW4oKGhlYWRlcnMpID0+IHtcbiAgICAgICAgZmV0Y2godGhpcy5fZ2V0VXJsKHBhdGgpLCB7XG4gICAgICAgICAgbWV0aG9kOiAncG9zdCcsXG4gICAgICAgICAgaGVhZGVyczogaGVhZGVycyxcbiAgICAgICAgICBjcmVkZW50aWFsczogJ3NhbWUtb3JpZ2luJyxcbiAgICAgICAgICBib2R5OiBKU09OLnN0cmluZ2lmeShkYXRhKVxuICAgICAgICB9KVxuICAgICAgICAgIC50aGVuKHRoaXMuX2dldFBvc3RSZXNwb25zZUZ1bmMoc2tpcFBvc3RSZXNwb25zZSkpXG4gICAgICAgICAgLnRoZW4oKHJlcykgPT4gdGhpcy5fanNvbihyZXMpLnRoZW4ocmVzb2x2ZSkpXG4gICAgICAgICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgICAgIGVyciA9IGVyci5pc0F1dGhFcnJvciA/IGVyciA6IG5ldyBFcnJvcihgQ291bGQgbm90IHBvc3QgJHt0aGlzLl9nZXRVcmwocGF0aCl9LiAke2Vycn1gKVxuICAgICAgICAgICAgcmVqZWN0KGVycilcbiAgICAgICAgICB9KVxuICAgICAgfSkuY2F0Y2gocmVqZWN0KVxuICAgIH0pXG4gIH1cblxuICBkZWxldGUgKHBhdGgsIGRhdGEsIHNraXBQb3N0UmVzcG9uc2UpIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgdGhpcy5wcmVmbGlnaHRBbmRIZWFkZXJzKHBhdGgpLnRoZW4oKGhlYWRlcnMpID0+IHtcbiAgICAgICAgZmV0Y2goYCR7dGhpcy5ob3N0bmFtZX0vJHtwYXRofWAsIHtcbiAgICAgICAgICBtZXRob2Q6ICdkZWxldGUnLFxuICAgICAgICAgIGhlYWRlcnM6IGhlYWRlcnMsXG4gICAgICAgICAgY3JlZGVudGlhbHM6ICdzYW1lLW9yaWdpbicsXG4gICAgICAgICAgYm9keTogZGF0YSA/IEpTT04uc3RyaW5naWZ5KGRhdGEpIDogbnVsbFxuICAgICAgICB9KVxuICAgICAgICAgIC50aGVuKHRoaXMuX2dldFBvc3RSZXNwb25zZUZ1bmMoc2tpcFBvc3RSZXNwb25zZSkpXG4gICAgICAgICAgLnRoZW4oKHJlcykgPT4gdGhpcy5fanNvbihyZXMpLnRoZW4ocmVzb2x2ZSkpXG4gICAgICAgICAgLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgICAgIGVyciA9IGVyci5pc0F1dGhFcnJvciA/IGVyciA6IG5ldyBFcnJvcihgQ291bGQgbm90IGRlbGV0ZSAke3RoaXMuX2dldFVybChwYXRoKX0uICR7ZXJyfWApXG4gICAgICAgICAgICByZWplY3QoZXJyKVxuICAgICAgICAgIH0pXG4gICAgICB9KS5jYXRjaChyZWplY3QpXG4gICAgfSlcbiAgfVxufVxuIiwiY29uc3QgZWUgPSByZXF1aXJlKCduYW1lc3BhY2UtZW1pdHRlcicpXG5cbm1vZHVsZS5leHBvcnRzID0gY2xhc3MgVXBweVNvY2tldCB7XG4gIGNvbnN0cnVjdG9yIChvcHRzKSB7XG4gICAgdGhpcy5vcHRzID0gb3B0c1xuICAgIHRoaXMuX3F1ZXVlZCA9IFtdXG4gICAgdGhpcy5pc09wZW4gPSBmYWxzZVxuICAgIHRoaXMuZW1pdHRlciA9IGVlKClcblxuICAgIHRoaXMuX2hhbmRsZU1lc3NhZ2UgPSB0aGlzLl9oYW5kbGVNZXNzYWdlLmJpbmQodGhpcylcblxuICAgIHRoaXMuY2xvc2UgPSB0aGlzLmNsb3NlLmJpbmQodGhpcylcbiAgICB0aGlzLmVtaXQgPSB0aGlzLmVtaXQuYmluZCh0aGlzKVxuICAgIHRoaXMub24gPSB0aGlzLm9uLmJpbmQodGhpcylcbiAgICB0aGlzLm9uY2UgPSB0aGlzLm9uY2UuYmluZCh0aGlzKVxuICAgIHRoaXMuc2VuZCA9IHRoaXMuc2VuZC5iaW5kKHRoaXMpXG5cbiAgICBpZiAoIW9wdHMgfHwgb3B0cy5hdXRvT3BlbiAhPT0gZmFsc2UpIHtcbiAgICAgIHRoaXMub3BlbigpXG4gICAgfVxuICB9XG5cbiAgb3BlbiAoKSB7XG4gICAgdGhpcy5zb2NrZXQgPSBuZXcgV2ViU29ja2V0KHRoaXMub3B0cy50YXJnZXQpXG5cbiAgICB0aGlzLnNvY2tldC5vbm9wZW4gPSAoZSkgPT4ge1xuICAgICAgdGhpcy5pc09wZW4gPSB0cnVlXG5cbiAgICAgIHdoaWxlICh0aGlzLl9xdWV1ZWQubGVuZ3RoID4gMCAmJiB0aGlzLmlzT3Blbikge1xuICAgICAgICBjb25zdCBmaXJzdCA9IHRoaXMuX3F1ZXVlZFswXVxuICAgICAgICB0aGlzLnNlbmQoZmlyc3QuYWN0aW9uLCBmaXJzdC5wYXlsb2FkKVxuICAgICAgICB0aGlzLl9xdWV1ZWQgPSB0aGlzLl9xdWV1ZWQuc2xpY2UoMSlcbiAgICAgIH1cbiAgICB9XG5cbiAgICB0aGlzLnNvY2tldC5vbmNsb3NlID0gKGUpID0+IHtcbiAgICAgIHRoaXMuaXNPcGVuID0gZmFsc2VcbiAgICB9XG5cbiAgICB0aGlzLnNvY2tldC5vbm1lc3NhZ2UgPSB0aGlzLl9oYW5kbGVNZXNzYWdlXG4gIH1cblxuICBjbG9zZSAoKSB7XG4gICAgaWYgKHRoaXMuc29ja2V0KSB7XG4gICAgICB0aGlzLnNvY2tldC5jbG9zZSgpXG4gICAgfVxuICB9XG5cbiAgc2VuZCAoYWN0aW9uLCBwYXlsb2FkKSB7XG4gICAgLy8gYXR0YWNoIHV1aWRcblxuICAgIGlmICghdGhpcy5pc09wZW4pIHtcbiAgICAgIHRoaXMuX3F1ZXVlZC5wdXNoKHsgYWN0aW9uLCBwYXlsb2FkIH0pXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICB0aGlzLnNvY2tldC5zZW5kKEpTT04uc3RyaW5naWZ5KHtcbiAgICAgIGFjdGlvbixcbiAgICAgIHBheWxvYWRcbiAgICB9KSlcbiAgfVxuXG4gIG9uIChhY3Rpb24sIGhhbmRsZXIpIHtcbiAgICB0aGlzLmVtaXR0ZXIub24oYWN0aW9uLCBoYW5kbGVyKVxuICB9XG5cbiAgZW1pdCAoYWN0aW9uLCBwYXlsb2FkKSB7XG4gICAgdGhpcy5lbWl0dGVyLmVtaXQoYWN0aW9uLCBwYXlsb2FkKVxuICB9XG5cbiAgb25jZSAoYWN0aW9uLCBoYW5kbGVyKSB7XG4gICAgdGhpcy5lbWl0dGVyLm9uY2UoYWN0aW9uLCBoYW5kbGVyKVxuICB9XG5cbiAgX2hhbmRsZU1lc3NhZ2UgKGUpIHtcbiAgICB0cnkge1xuICAgICAgY29uc3QgbWVzc2FnZSA9IEpTT04ucGFyc2UoZS5kYXRhKVxuICAgICAgdGhpcy5lbWl0KG1lc3NhZ2UuYWN0aW9uLCBtZXNzYWdlLnBheWxvYWQpXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBjb25zb2xlLmxvZyhlcnIpXG4gICAgfVxuICB9XG59XG4iLCIndXNlIHN0cmljdCdcblxuLyoqXG4gKiBNYW5hZ2VzIGNvbW11bmljYXRpb25zIHdpdGggQ29tcGFuaW9uXG4gKi9cblxuY29uc3QgUmVxdWVzdENsaWVudCA9IHJlcXVpcmUoJy4vUmVxdWVzdENsaWVudCcpXG5jb25zdCBQcm92aWRlciA9IHJlcXVpcmUoJy4vUHJvdmlkZXInKVxuY29uc3QgU29ja2V0ID0gcmVxdWlyZSgnLi9Tb2NrZXQnKVxuXG5tb2R1bGUuZXhwb3J0cyA9IHtcbiAgUmVxdWVzdENsaWVudCxcbiAgUHJvdmlkZXIsXG4gIFNvY2tldFxufVxuIiwiJ3VzZSBzdHJpY3QnXG5cbi8qKlxuICogVGhpcyBtb2R1bGUgc2VydmVzIGFzIGFuIEFzeW5jIHdyYXBwZXIgZm9yIExvY2FsU3RvcmFnZVxuICovXG5tb2R1bGUuZXhwb3J0cy5zZXRJdGVtID0gKGtleSwgdmFsdWUpID0+IHtcbiAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlKSA9PiB7XG4gICAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oa2V5LCB2YWx1ZSlcbiAgICByZXNvbHZlKClcbiAgfSlcbn1cblxubW9kdWxlLmV4cG9ydHMuZ2V0SXRlbSA9IChrZXkpID0+IHtcbiAgcmV0dXJuIFByb21pc2UucmVzb2x2ZShsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpKVxufVxuXG5tb2R1bGUuZXhwb3J0cy5yZW1vdmVJdGVtID0gKGtleSkgPT4ge1xuICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICBsb2NhbFN0b3JhZ2UucmVtb3ZlSXRlbShrZXkpXG4gICAgcmVzb2x2ZSgpXG4gIH0pXG59XG4iLCJtb2R1bGUuZXhwb3J0cz17XG4gIFwibmFtZVwiOiBcIkB1cHB5L2NvcmVcIixcbiAgXCJkZXNjcmlwdGlvblwiOiBcIkNvcmUgbW9kdWxlIGZvciB0aGUgZXh0ZW5zaWJsZSBKYXZhU2NyaXB0IGZpbGUgdXBsb2FkIHdpZGdldCB3aXRoIHN1cHBvcnQgZm9yIGRyYWcmZHJvcCwgcmVzdW1hYmxlIHVwbG9hZHMsIHByZXZpZXdzLCByZXN0cmljdGlvbnMsIGZpbGUgcHJvY2Vzc2luZy9lbmNvZGluZywgcmVtb3RlIHByb3ZpZGVycyBsaWtlIEluc3RhZ3JhbSwgRHJvcGJveCwgR29vZ2xlIERyaXZlLCBTMyBhbmQgbW9yZSA6ZG9nOlwiLFxuICBcInZlcnNpb25cIjogXCIxLjUuMVwiLFxuICBcImxpY2Vuc2VcIjogXCJNSVRcIixcbiAgXCJtYWluXCI6IFwibGliL2luZGV4LmpzXCIsXG4gIFwic3R5bGVcIjogXCJkaXN0L3N0eWxlLm1pbi5jc3NcIixcbiAgXCJ0eXBlc1wiOiBcInR5cGVzL2luZGV4LmQudHNcIixcbiAgXCJrZXl3b3Jkc1wiOiBbXG4gICAgXCJmaWxlIHVwbG9hZGVyXCIsXG4gICAgXCJ1cHB5XCIsXG4gICAgXCJ1cHB5LXBsdWdpblwiXG4gIF0sXG4gIFwiaG9tZXBhZ2VcIjogXCJodHRwczovL3VwcHkuaW9cIixcbiAgXCJidWdzXCI6IHtcbiAgICBcInVybFwiOiBcImh0dHBzOi8vZ2l0aHViLmNvbS90cmFuc2xvYWRpdC91cHB5L2lzc3Vlc1wiXG4gIH0sXG4gIFwicmVwb3NpdG9yeVwiOiB7XG4gICAgXCJ0eXBlXCI6IFwiZ2l0XCIsXG4gICAgXCJ1cmxcIjogXCJnaXQraHR0cHM6Ly9naXRodWIuY29tL3RyYW5zbG9hZGl0L3VwcHkuZ2l0XCJcbiAgfSxcbiAgXCJkZXBlbmRlbmNpZXNcIjoge1xuICAgIFwiQHVwcHkvc3RvcmUtZGVmYXVsdFwiOiBcImZpbGU6Li4vc3RvcmUtZGVmYXVsdFwiLFxuICAgIFwiQHVwcHkvdXRpbHNcIjogXCJmaWxlOi4uL3V0aWxzXCIsXG4gICAgXCJjdWlkXCI6IFwiXjIuMS4xXCIsXG4gICAgXCJsb2Rhc2gudGhyb3R0bGVcIjogXCJeNC4xLjFcIixcbiAgICBcIm1pbWUtbWF0Y2hcIjogXCJeMS4wLjJcIixcbiAgICBcIm5hbWVzcGFjZS1lbWl0dGVyXCI6IFwiXjIuMC4xXCIsXG4gICAgXCJwcmVhY3RcIjogXCI4LjIuOVwiXG4gIH1cbn1cbiIsImNvbnN0IHByZWFjdCA9IHJlcXVpcmUoJ3ByZWFjdCcpXG5jb25zdCBmaW5kRE9NRWxlbWVudCA9IHJlcXVpcmUoJ0B1cHB5L3V0aWxzL2xpYi9maW5kRE9NRWxlbWVudCcpXG5cbi8qKlxuICogRGVmZXIgYSBmcmVxdWVudCBjYWxsIHRvIHRoZSBtaWNyb3Rhc2sgcXVldWUuXG4gKi9cbmZ1bmN0aW9uIGRlYm91bmNlIChmbikge1xuICBsZXQgY2FsbGluZyA9IG51bGxcbiAgbGV0IGxhdGVzdEFyZ3MgPSBudWxsXG4gIHJldHVybiAoLi4uYXJncykgPT4ge1xuICAgIGxhdGVzdEFyZ3MgPSBhcmdzXG4gICAgaWYgKCFjYWxsaW5nKSB7XG4gICAgICBjYWxsaW5nID0gUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICAgIGNhbGxpbmcgPSBudWxsXG4gICAgICAgIC8vIEF0IHRoaXMgcG9pbnQgYGFyZ3NgIG1heSBiZSBkaWZmZXJlbnQgZnJvbSB0aGUgbW9zdFxuICAgICAgICAvLyByZWNlbnQgc3RhdGUsIGlmIG11bHRpcGxlIGNhbGxzIGhhcHBlbmVkIHNpbmNlIHRoaXMgdGFza1xuICAgICAgICAvLyB3YXMgcXVldWVkLiBTbyB3ZSB1c2UgdGhlIGBsYXRlc3RBcmdzYCwgd2hpY2ggZGVmaW5pdGVseVxuICAgICAgICAvLyBpcyB0aGUgbW9zdCByZWNlbnQgY2FsbC5cbiAgICAgICAgcmV0dXJuIGZuKC4uLmxhdGVzdEFyZ3MpXG4gICAgICB9KVxuICAgIH1cbiAgICByZXR1cm4gY2FsbGluZ1xuICB9XG59XG5cbi8qKlxuICogQm9pbGVycGxhdGUgdGhhdCBhbGwgUGx1Z2lucyBzaGFyZSAtIGFuZCBzaG91bGQgbm90IGJlIHVzZWRcbiAqIGRpcmVjdGx5LiBJdCBhbHNvIHNob3dzIHdoaWNoIG1ldGhvZHMgZmluYWwgcGx1Z2lucyBzaG91bGQgaW1wbGVtZW50L292ZXJyaWRlLFxuICogdGhpcyBkZWNpZGluZyBvbiBzdHJ1Y3R1cmUuXG4gKlxuICogQHBhcmFtIHtvYmplY3R9IG1haW4gVXBweSBjb3JlIG9iamVjdFxuICogQHBhcmFtIHtvYmplY3R9IG9iamVjdCB3aXRoIHBsdWdpbiBvcHRpb25zXG4gKiBAcmV0dXJucyB7QXJyYXl8c3RyaW5nfSBmaWxlcyBvciBzdWNjZXNzL2ZhaWwgbWVzc2FnZVxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGNsYXNzIFBsdWdpbiB7XG4gIGNvbnN0cnVjdG9yICh1cHB5LCBvcHRzKSB7XG4gICAgdGhpcy51cHB5ID0gdXBweVxuICAgIHRoaXMub3B0cyA9IG9wdHMgfHwge31cblxuICAgIHRoaXMudXBkYXRlID0gdGhpcy51cGRhdGUuYmluZCh0aGlzKVxuICAgIHRoaXMubW91bnQgPSB0aGlzLm1vdW50LmJpbmQodGhpcylcbiAgICB0aGlzLmluc3RhbGwgPSB0aGlzLmluc3RhbGwuYmluZCh0aGlzKVxuICAgIHRoaXMudW5pbnN0YWxsID0gdGhpcy51bmluc3RhbGwuYmluZCh0aGlzKVxuICB9XG5cbiAgZ2V0UGx1Z2luU3RhdGUgKCkge1xuICAgIGNvbnN0IHsgcGx1Z2lucyB9ID0gdGhpcy51cHB5LmdldFN0YXRlKClcbiAgICByZXR1cm4gcGx1Z2luc1t0aGlzLmlkXSB8fCB7fVxuICB9XG5cbiAgc2V0UGx1Z2luU3RhdGUgKHVwZGF0ZSkge1xuICAgIGNvbnN0IHsgcGx1Z2lucyB9ID0gdGhpcy51cHB5LmdldFN0YXRlKClcblxuICAgIHRoaXMudXBweS5zZXRTdGF0ZSh7XG4gICAgICBwbHVnaW5zOiB7XG4gICAgICAgIC4uLnBsdWdpbnMsXG4gICAgICAgIFt0aGlzLmlkXToge1xuICAgICAgICAgIC4uLnBsdWdpbnNbdGhpcy5pZF0sXG4gICAgICAgICAgLi4udXBkYXRlXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KVxuICB9XG5cbiAgdXBkYXRlIChzdGF0ZSkge1xuICAgIGlmICh0eXBlb2YgdGhpcy5lbCA9PT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIGlmICh0aGlzLl91cGRhdGVVSSkge1xuICAgICAgdGhpcy5fdXBkYXRlVUkoc3RhdGUpXG4gICAgfVxuICB9XG5cbiAgLy8gQ2FsbGVkIGFmdGVyIGV2ZXJ5IHN0YXRlIHVwZGF0ZSwgYWZ0ZXIgZXZlcnl0aGluZydzIG1vdW50ZWQuIERlYm91bmNlZC5cbiAgYWZ0ZXJVcGRhdGUgKCkge1xuXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGVkIHdoZW4gcGx1Z2luIGlzIG1vdW50ZWQsIHdoZXRoZXIgaW4gRE9NIG9yIGludG8gYW5vdGhlciBwbHVnaW4uXG4gICAqIE5lZWRlZCBiZWNhdXNlIHNvbWV0aW1lcyBwbHVnaW5zIGFyZSBtb3VudGVkIHNlcGFyYXRlbHkvYWZ0ZXIgYGluc3RhbGxgLFxuICAgKiBzbyB0aGlzLmVsIGFuZCB0aGlzLnBhcmVudCBtaWdodCBub3QgYmUgYXZhaWxhYmxlIGluIGBpbnN0YWxsYC5cbiAgICogVGhpcyBpcyB0aGUgY2FzZSB3aXRoIEB1cHB5L3JlYWN0IHBsdWdpbnMsIGZvciBleGFtcGxlLlxuICAgKi9cbiAgb25Nb3VudCAoKSB7XG5cbiAgfVxuXG4gIC8qKlxuICAgKiBDaGVjayBpZiBzdXBwbGllZCBgdGFyZ2V0YCBpcyBhIERPTSBlbGVtZW50IG9yIGFuIGBvYmplY3RgLlxuICAgKiBJZiBpdOKAmXMgYW4gb2JqZWN0IOKAlCB0YXJnZXQgaXMgYSBwbHVnaW4sIGFuZCB3ZSBzZWFyY2ggYHBsdWdpbnNgXG4gICAqIGZvciBhIHBsdWdpbiB3aXRoIHNhbWUgbmFtZSBhbmQgcmV0dXJuIGl0cyB0YXJnZXQuXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfG9iamVjdH0gdGFyZ2V0XG4gICAqXG4gICAqL1xuICBtb3VudCAodGFyZ2V0LCBwbHVnaW4pIHtcbiAgICBjb25zdCBjYWxsZXJQbHVnaW5OYW1lID0gcGx1Z2luLmlkXG5cbiAgICBjb25zdCB0YXJnZXRFbGVtZW50ID0gZmluZERPTUVsZW1lbnQodGFyZ2V0KVxuXG4gICAgaWYgKHRhcmdldEVsZW1lbnQpIHtcbiAgICAgIHRoaXMuaXNUYXJnZXRET01FbCA9IHRydWVcblxuICAgICAgLy8gQVBJIGZvciBwbHVnaW5zIHRoYXQgcmVxdWlyZSBhIHN5bmNocm9ub3VzIHJlcmVuZGVyLlxuICAgICAgdGhpcy5yZXJlbmRlciA9IChzdGF0ZSkgPT4ge1xuICAgICAgICAvLyBwbHVnaW4gY291bGQgYmUgcmVtb3ZlZCwgYnV0IHRoaXMucmVyZW5kZXIgaXMgZGVib3VuY2VkIGJlbG93LFxuICAgICAgICAvLyBzbyBpdCBjb3VsZCBzdGlsbCBiZSBjYWxsZWQgZXZlbiBhZnRlciB1cHB5LnJlbW92ZVBsdWdpbiBvciB1cHB5LmNsb3NlXG4gICAgICAgIC8vIGhlbmNlIHRoZSBjaGVja1xuICAgICAgICBpZiAoIXRoaXMudXBweS5nZXRQbHVnaW4odGhpcy5pZCkpIHJldHVyblxuICAgICAgICB0aGlzLmVsID0gcHJlYWN0LnJlbmRlcih0aGlzLnJlbmRlcihzdGF0ZSksIHRhcmdldEVsZW1lbnQsIHRoaXMuZWwpXG4gICAgICAgIHRoaXMuYWZ0ZXJVcGRhdGUoKVxuICAgICAgfVxuICAgICAgdGhpcy5fdXBkYXRlVUkgPSBkZWJvdW5jZSh0aGlzLnJlcmVuZGVyKVxuXG4gICAgICB0aGlzLnVwcHkubG9nKGBJbnN0YWxsaW5nICR7Y2FsbGVyUGx1Z2luTmFtZX0gdG8gYSBET00gZWxlbWVudCAnJHt0YXJnZXR9J2ApXG5cbiAgICAgIC8vIGNsZWFyIGV2ZXJ5dGhpbmcgaW5zaWRlIHRoZSB0YXJnZXQgY29udGFpbmVyXG4gICAgICBpZiAodGhpcy5vcHRzLnJlcGxhY2VUYXJnZXRDb250ZW50KSB7XG4gICAgICAgIHRhcmdldEVsZW1lbnQuaW5uZXJIVE1MID0gJydcbiAgICAgIH1cblxuICAgICAgdGhpcy5lbCA9IHByZWFjdC5yZW5kZXIodGhpcy5yZW5kZXIodGhpcy51cHB5LmdldFN0YXRlKCkpLCB0YXJnZXRFbGVtZW50KVxuXG4gICAgICB0aGlzLm9uTW91bnQoKVxuICAgICAgcmV0dXJuIHRoaXMuZWxcbiAgICB9XG5cbiAgICBsZXQgdGFyZ2V0UGx1Z2luXG4gICAgaWYgKHR5cGVvZiB0YXJnZXQgPT09ICdvYmplY3QnICYmIHRhcmdldCBpbnN0YW5jZW9mIFBsdWdpbikge1xuICAgICAgLy8gVGFyZ2V0aW5nIGEgcGx1Z2luICppbnN0YW5jZSpcbiAgICAgIHRhcmdldFBsdWdpbiA9IHRhcmdldFxuICAgIH0gZWxzZSBpZiAodHlwZW9mIHRhcmdldCA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgLy8gVGFyZ2V0aW5nIGEgcGx1Z2luIHR5cGVcbiAgICAgIGNvbnN0IFRhcmdldCA9IHRhcmdldFxuICAgICAgLy8gRmluZCB0aGUgdGFyZ2V0IHBsdWdpbiBpbnN0YW5jZS5cbiAgICAgIHRoaXMudXBweS5pdGVyYXRlUGx1Z2lucygocGx1Z2luKSA9PiB7XG4gICAgICAgIGlmIChwbHVnaW4gaW5zdGFuY2VvZiBUYXJnZXQpIHtcbiAgICAgICAgICB0YXJnZXRQbHVnaW4gPSBwbHVnaW5cbiAgICAgICAgICByZXR1cm4gZmFsc2VcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICB9XG5cbiAgICBpZiAodGFyZ2V0UGx1Z2luKSB7XG4gICAgICB0aGlzLnVwcHkubG9nKGBJbnN0YWxsaW5nICR7Y2FsbGVyUGx1Z2luTmFtZX0gdG8gJHt0YXJnZXRQbHVnaW4uaWR9YClcbiAgICAgIHRoaXMucGFyZW50ID0gdGFyZ2V0UGx1Z2luXG4gICAgICB0aGlzLmVsID0gdGFyZ2V0UGx1Z2luLmFkZFRhcmdldChwbHVnaW4pXG5cbiAgICAgIHRoaXMub25Nb3VudCgpXG4gICAgICByZXR1cm4gdGhpcy5lbFxuICAgIH1cblxuICAgIHRoaXMudXBweS5sb2coYE5vdCBpbnN0YWxsaW5nICR7Y2FsbGVyUGx1Z2luTmFtZX1gKVxuICAgIHRocm93IG5ldyBFcnJvcihgSW52YWxpZCB0YXJnZXQgb3B0aW9uIGdpdmVuIHRvICR7Y2FsbGVyUGx1Z2luTmFtZX0uIFBsZWFzZSBtYWtlIHN1cmUgdGhhdCB0aGUgZWxlbWVudFxuICAgICAgZXhpc3RzIG9uIHRoZSBwYWdlLCBvciB0aGF0IHRoZSBwbHVnaW4geW91IGFyZSB0YXJnZXRpbmcgaGFzIGJlZW4gaW5zdGFsbGVkLiBDaGVjayB0aGF0IHRoZSA8c2NyaXB0PiB0YWcgaW5pdGlhbGl6aW5nIFVwcHlcbiAgICAgIGNvbWVzIGF0IHRoZSBib3R0b20gb2YgdGhlIHBhZ2UsIGJlZm9yZSB0aGUgY2xvc2luZyA8L2JvZHk+IHRhZyAoc2VlIGh0dHBzOi8vZ2l0aHViLmNvbS90cmFuc2xvYWRpdC91cHB5L2lzc3Vlcy8xMDQyKS5gKVxuICB9XG5cbiAgcmVuZGVyIChzdGF0ZSkge1xuICAgIHRocm93IChuZXcgRXJyb3IoJ0V4dGVuZCB0aGUgcmVuZGVyIG1ldGhvZCB0byBhZGQgeW91ciBwbHVnaW4gdG8gYSBET00gZWxlbWVudCcpKVxuICB9XG5cbiAgYWRkVGFyZ2V0IChwbHVnaW4pIHtcbiAgICB0aHJvdyAobmV3IEVycm9yKCdFeHRlbmQgdGhlIGFkZFRhcmdldCBtZXRob2QgdG8gYWRkIHlvdXIgcGx1Z2luIHRvIGFub3RoZXIgcGx1Z2luXFwncyB0YXJnZXQnKSlcbiAgfVxuXG4gIHVubW91bnQgKCkge1xuICAgIGlmICh0aGlzLmlzVGFyZ2V0RE9NRWwgJiYgdGhpcy5lbCAmJiB0aGlzLmVsLnBhcmVudE5vZGUpIHtcbiAgICAgIHRoaXMuZWwucGFyZW50Tm9kZS5yZW1vdmVDaGlsZCh0aGlzLmVsKVxuICAgIH1cbiAgfVxuXG4gIGluc3RhbGwgKCkge1xuXG4gIH1cblxuICB1bmluc3RhbGwgKCkge1xuICAgIHRoaXMudW5tb3VudCgpXG4gIH1cbn1cbiIsImNvbnN0IFRyYW5zbGF0b3IgPSByZXF1aXJlKCdAdXBweS91dGlscy9saWIvVHJhbnNsYXRvcicpXG5jb25zdCBlZSA9IHJlcXVpcmUoJ25hbWVzcGFjZS1lbWl0dGVyJylcbmNvbnN0IGN1aWQgPSByZXF1aXJlKCdjdWlkJylcbmNvbnN0IHRocm90dGxlID0gcmVxdWlyZSgnbG9kYXNoLnRocm90dGxlJylcbmNvbnN0IHByZXR0eUJ5dGVzID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL3ByZXR0eUJ5dGVzJylcbmNvbnN0IG1hdGNoID0gcmVxdWlyZSgnbWltZS1tYXRjaCcpXG5jb25zdCBEZWZhdWx0U3RvcmUgPSByZXF1aXJlKCdAdXBweS9zdG9yZS1kZWZhdWx0JylcbmNvbnN0IGdldEZpbGVUeXBlID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL2dldEZpbGVUeXBlJylcbmNvbnN0IGdldEZpbGVOYW1lQW5kRXh0ZW5zaW9uID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL2dldEZpbGVOYW1lQW5kRXh0ZW5zaW9uJylcbmNvbnN0IGdlbmVyYXRlRmlsZUlEID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL2dlbmVyYXRlRmlsZUlEJylcbmNvbnN0IHN1cHBvcnRzVXBsb2FkUHJvZ3Jlc3MgPSByZXF1aXJlKCcuL3N1cHBvcnRzVXBsb2FkUHJvZ3Jlc3MnKVxuY29uc3QgeyBudWxsTG9nZ2VyLCBkZWJ1Z0xvZ2dlciB9ID0gcmVxdWlyZSgnLi9sb2dnZXJzJylcbmNvbnN0IFBsdWdpbiA9IHJlcXVpcmUoJy4vUGx1Z2luJykgLy8gRXhwb3J0ZWQgZnJvbSBoZXJlLlxuXG5jbGFzcyBSZXN0cmljdGlvbkVycm9yIGV4dGVuZHMgRXJyb3Ige1xuICBjb25zdHJ1Y3RvciAoLi4uYXJncykge1xuICAgIHN1cGVyKC4uLmFyZ3MpXG4gICAgdGhpcy5pc1Jlc3RyaWN0aW9uID0gdHJ1ZVxuICB9XG59XG5cbi8qKlxuICogVXBweSBDb3JlIG1vZHVsZS5cbiAqIE1hbmFnZXMgcGx1Z2lucywgc3RhdGUgdXBkYXRlcywgYWN0cyBhcyBhbiBldmVudCBidXMsXG4gKiBhZGRzL3JlbW92ZXMgZmlsZXMgYW5kIG1ldGFkYXRhLlxuICovXG5jbGFzcyBVcHB5IHtcbiAgc3RhdGljIFZFUlNJT04gPSByZXF1aXJlKCcuLi9wYWNrYWdlLmpzb24nKS52ZXJzaW9uXG5cbiAgLyoqXG4gICAqIEluc3RhbnRpYXRlIFVwcHlcbiAgICpcbiAgICogQHBhcmFtIHtvYmplY3R9IG9wdHMg4oCUIFVwcHkgb3B0aW9uc1xuICAgKi9cbiAgY29uc3RydWN0b3IgKG9wdHMpIHtcbiAgICB0aGlzLmRlZmF1bHRMb2NhbGUgPSB7XG4gICAgICBzdHJpbmdzOiB7XG4gICAgICAgIHlvdUNhbk9ubHlVcGxvYWRYOiB7XG4gICAgICAgICAgMDogJ1lvdSBjYW4gb25seSB1cGxvYWQgJXtzbWFydF9jb3VudH0gZmlsZScsXG4gICAgICAgICAgMTogJ1lvdSBjYW4gb25seSB1cGxvYWQgJXtzbWFydF9jb3VudH0gZmlsZXMnLFxuICAgICAgICAgIDI6ICdZb3UgY2FuIG9ubHkgdXBsb2FkICV7c21hcnRfY291bnR9IGZpbGVzJ1xuICAgICAgICB9LFxuICAgICAgICB5b3VIYXZlVG9BdExlYXN0U2VsZWN0WDoge1xuICAgICAgICAgIDA6ICdZb3UgaGF2ZSB0byBzZWxlY3QgYXQgbGVhc3QgJXtzbWFydF9jb3VudH0gZmlsZScsXG4gICAgICAgICAgMTogJ1lvdSBoYXZlIHRvIHNlbGVjdCBhdCBsZWFzdCAle3NtYXJ0X2NvdW50fSBmaWxlcycsXG4gICAgICAgICAgMjogJ1lvdSBoYXZlIHRvIHNlbGVjdCBhdCBsZWFzdCAle3NtYXJ0X2NvdW50fSBmaWxlcydcbiAgICAgICAgfSxcbiAgICAgICAgZXhjZWVkc1NpemU6ICdUaGlzIGZpbGUgZXhjZWVkcyBtYXhpbXVtIGFsbG93ZWQgc2l6ZSBvZicsXG4gICAgICAgIHlvdUNhbk9ubHlVcGxvYWRGaWxlVHlwZXM6ICdZb3UgY2FuIG9ubHkgdXBsb2FkOiAle3R5cGVzfScsXG4gICAgICAgIGNvbXBhbmlvbkVycm9yOiAnQ29ubmVjdGlvbiB3aXRoIENvbXBhbmlvbiBmYWlsZWQnLFxuICAgICAgICBjb21wYW5pb25BdXRoRXJyb3I6ICdBdXRob3JpemF0aW9uIHJlcXVpcmVkJyxcbiAgICAgICAgY29tcGFuaW9uVW5hdXRob3JpemVIaW50OiAnVG8gdW5hdXRob3JpemUgdG8geW91ciAle3Byb3ZpZGVyfSBhY2NvdW50LCBwbGVhc2UgZ28gdG8gJXt1cmx9JyxcbiAgICAgICAgZmFpbGVkVG9VcGxvYWQ6ICdGYWlsZWQgdG8gdXBsb2FkICV7ZmlsZX0nLFxuICAgICAgICBub0ludGVybmV0Q29ubmVjdGlvbjogJ05vIEludGVybmV0IGNvbm5lY3Rpb24nLFxuICAgICAgICBjb25uZWN0ZWRUb0ludGVybmV0OiAnQ29ubmVjdGVkIHRvIHRoZSBJbnRlcm5ldCcsXG4gICAgICAgIC8vIFN0cmluZ3MgZm9yIHJlbW90ZSBwcm92aWRlcnNcbiAgICAgICAgbm9GaWxlc0ZvdW5kOiAnWW91IGhhdmUgbm8gZmlsZXMgb3IgZm9sZGVycyBoZXJlJyxcbiAgICAgICAgc2VsZWN0WDoge1xuICAgICAgICAgIDA6ICdTZWxlY3QgJXtzbWFydF9jb3VudH0nLFxuICAgICAgICAgIDE6ICdTZWxlY3QgJXtzbWFydF9jb3VudH0nLFxuICAgICAgICAgIDI6ICdTZWxlY3QgJXtzbWFydF9jb3VudH0nXG4gICAgICAgIH0sXG4gICAgICAgIHNlbGVjdEFsbEZpbGVzRnJvbUZvbGRlck5hbWVkOiAnU2VsZWN0IGFsbCBmaWxlcyBmcm9tIGZvbGRlciAle25hbWV9JyxcbiAgICAgICAgdW5zZWxlY3RBbGxGaWxlc0Zyb21Gb2xkZXJOYW1lZDogJ1Vuc2VsZWN0IGFsbCBmaWxlcyBmcm9tIGZvbGRlciAle25hbWV9JyxcbiAgICAgICAgc2VsZWN0RmlsZU5hbWVkOiAnU2VsZWN0IGZpbGUgJXtuYW1lfScsXG4gICAgICAgIHVuc2VsZWN0RmlsZU5hbWVkOiAnVW5zZWxlY3QgZmlsZSAle25hbWV9JyxcbiAgICAgICAgb3BlbkZvbGRlck5hbWVkOiAnT3BlbiBmb2xkZXIgJXtuYW1lfScsXG4gICAgICAgIGNhbmNlbDogJ0NhbmNlbCcsXG4gICAgICAgIGxvZ091dDogJ0xvZyBvdXQnLFxuICAgICAgICBmaWx0ZXI6ICdGaWx0ZXInLFxuICAgICAgICByZXNldEZpbHRlcjogJ1Jlc2V0IGZpbHRlcicsXG4gICAgICAgIGxvYWRpbmc6ICdMb2FkaW5nLi4uJyxcbiAgICAgICAgYXV0aGVudGljYXRlV2l0aFRpdGxlOiAnUGxlYXNlIGF1dGhlbnRpY2F0ZSB3aXRoICV7cGx1Z2luTmFtZX0gdG8gc2VsZWN0IGZpbGVzJyxcbiAgICAgICAgYXV0aGVudGljYXRlV2l0aDogJ0Nvbm5lY3QgdG8gJXtwbHVnaW5OYW1lfScsXG4gICAgICAgIGVtcHR5Rm9sZGVyQWRkZWQ6ICdObyBmaWxlcyB3ZXJlIGFkZGVkIGZyb20gZW1wdHkgZm9sZGVyJyxcbiAgICAgICAgZm9sZGVyQWRkZWQ6IHtcbiAgICAgICAgICAwOiAnQWRkZWQgJXtzbWFydF9jb3VudH0gZmlsZSBmcm9tICV7Zm9sZGVyfScsXG4gICAgICAgICAgMTogJ0FkZGVkICV7c21hcnRfY291bnR9IGZpbGVzIGZyb20gJXtmb2xkZXJ9JyxcbiAgICAgICAgICAyOiAnQWRkZWQgJXtzbWFydF9jb3VudH0gZmlsZXMgZnJvbSAle2ZvbGRlcn0nXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBzZXQgZGVmYXVsdCBvcHRpb25zXG4gICAgY29uc3QgZGVmYXVsdE9wdGlvbnMgPSB7XG4gICAgICBpZDogJ3VwcHknLFxuICAgICAgYXV0b1Byb2NlZWQ6IGZhbHNlLFxuICAgICAgYWxsb3dNdWx0aXBsZVVwbG9hZHM6IHRydWUsXG4gICAgICBkZWJ1ZzogZmFsc2UsXG4gICAgICByZXN0cmljdGlvbnM6IHtcbiAgICAgICAgbWF4RmlsZVNpemU6IG51bGwsXG4gICAgICAgIG1heE51bWJlck9mRmlsZXM6IG51bGwsXG4gICAgICAgIG1pbk51bWJlck9mRmlsZXM6IG51bGwsXG4gICAgICAgIGFsbG93ZWRGaWxlVHlwZXM6IG51bGxcbiAgICAgIH0sXG4gICAgICBtZXRhOiB7fSxcbiAgICAgIG9uQmVmb3JlRmlsZUFkZGVkOiAoY3VycmVudEZpbGUsIGZpbGVzKSA9PiBjdXJyZW50RmlsZSxcbiAgICAgIG9uQmVmb3JlVXBsb2FkOiAoZmlsZXMpID0+IGZpbGVzLFxuICAgICAgc3RvcmU6IERlZmF1bHRTdG9yZSgpLFxuICAgICAgbG9nZ2VyOiBudWxsTG9nZ2VyXG4gICAgfVxuXG4gICAgLy8gTWVyZ2UgZGVmYXVsdCBvcHRpb25zIHdpdGggdGhlIG9uZXMgc2V0IGJ5IHVzZXJcbiAgICB0aGlzLm9wdHMgPSBPYmplY3QuYXNzaWduKHt9LCBkZWZhdWx0T3B0aW9ucywgb3B0cylcbiAgICB0aGlzLm9wdHMucmVzdHJpY3Rpb25zID0gT2JqZWN0LmFzc2lnbih7fSwgZGVmYXVsdE9wdGlvbnMucmVzdHJpY3Rpb25zLCB0aGlzLm9wdHMucmVzdHJpY3Rpb25zKVxuXG4gICAgLy8gU3VwcG9ydCBkZWJ1ZzogdHJ1ZSBmb3IgYmFja3dhcmRzLWNvbXBhdGFiaWxpdHksIHVubGVzcyBsb2dnZXIgaXMgc2V0IGluIG9wdHNcbiAgICAvLyBvcHRzIGluc3RlYWQgb2YgdGhpcy5vcHRzIHRvIGF2b2lkIGNvbXBhcmluZyBvYmplY3RzIOKAlCB3ZSBzZXQgbG9nZ2VyOiBudWxsTG9nZ2VyIGluIGRlZmF1bHRPcHRpb25zXG4gICAgaWYgKG9wdHMgJiYgb3B0cy5sb2dnZXIgJiYgb3B0cy5kZWJ1Zykge1xuICAgICAgdGhpcy5sb2coJ1lvdSBhcmUgdXNpbmcgYSBjdXN0b20gYGxvZ2dlcmAsIGJ1dCBhbHNvIHNldCBgZGVidWc6IHRydWVgLCB3aGljaCB1c2VzIGJ1aWx0LWluIGxvZ2dlciB0byBvdXRwdXQgbG9ncyB0byBjb25zb2xlLiBJZ25vcmluZyBgZGVidWc6IHRydWVgIGFuZCB1c2luZyB5b3VyIGN1c3RvbSBgbG9nZ2VyYC4nLCAnd2FybmluZycpXG4gICAgfSBlbHNlIGlmIChvcHRzICYmIG9wdHMuZGVidWcpIHtcbiAgICAgIHRoaXMub3B0cy5sb2dnZXIgPSBkZWJ1Z0xvZ2dlclxuICAgIH1cblxuICAgIHRoaXMubG9nKGBVc2luZyBDb3JlIHYke3RoaXMuY29uc3RydWN0b3IuVkVSU0lPTn1gKVxuXG4gICAgaWYgKHRoaXMub3B0cy5yZXN0cmljdGlvbnMuYWxsb3dlZEZpbGVUeXBlcyAmJlxuICAgICAgICB0aGlzLm9wdHMucmVzdHJpY3Rpb25zLmFsbG93ZWRGaWxlVHlwZXMgIT09IG51bGwgJiZcbiAgICAgICAgIUFycmF5LmlzQXJyYXkodGhpcy5vcHRzLnJlc3RyaWN0aW9ucy5hbGxvd2VkRmlsZVR5cGVzKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignYHJlc3RyaWN0aW9ucy5hbGxvd2VkRmlsZVR5cGVzYCBtdXN0IGJlIGFuIGFycmF5JylcbiAgICB9XG5cbiAgICAvLyBpMThuXG4gICAgdGhpcy50cmFuc2xhdG9yID0gbmV3IFRyYW5zbGF0b3IoW3RoaXMuZGVmYXVsdExvY2FsZSwgdGhpcy5vcHRzLmxvY2FsZV0pXG4gICAgdGhpcy5sb2NhbGUgPSB0aGlzLnRyYW5zbGF0b3IubG9jYWxlXG4gICAgdGhpcy5pMThuID0gdGhpcy50cmFuc2xhdG9yLnRyYW5zbGF0ZS5iaW5kKHRoaXMudHJhbnNsYXRvcilcbiAgICB0aGlzLmkxOG5BcnJheSA9IHRoaXMudHJhbnNsYXRvci50cmFuc2xhdGVBcnJheS5iaW5kKHRoaXMudHJhbnNsYXRvcilcblxuICAgIC8vIENvbnRhaW5lciBmb3IgZGlmZmVyZW50IHR5cGVzIG9mIHBsdWdpbnNcbiAgICB0aGlzLnBsdWdpbnMgPSB7fVxuXG4gICAgdGhpcy5nZXRTdGF0ZSA9IHRoaXMuZ2V0U3RhdGUuYmluZCh0aGlzKVxuICAgIHRoaXMuZ2V0UGx1Z2luID0gdGhpcy5nZXRQbHVnaW4uYmluZCh0aGlzKVxuICAgIHRoaXMuc2V0RmlsZU1ldGEgPSB0aGlzLnNldEZpbGVNZXRhLmJpbmQodGhpcylcbiAgICB0aGlzLnNldEZpbGVTdGF0ZSA9IHRoaXMuc2V0RmlsZVN0YXRlLmJpbmQodGhpcylcbiAgICB0aGlzLmxvZyA9IHRoaXMubG9nLmJpbmQodGhpcylcbiAgICB0aGlzLmluZm8gPSB0aGlzLmluZm8uYmluZCh0aGlzKVxuICAgIHRoaXMuaGlkZUluZm8gPSB0aGlzLmhpZGVJbmZvLmJpbmQodGhpcylcbiAgICB0aGlzLmFkZEZpbGUgPSB0aGlzLmFkZEZpbGUuYmluZCh0aGlzKVxuICAgIHRoaXMucmVtb3ZlRmlsZSA9IHRoaXMucmVtb3ZlRmlsZS5iaW5kKHRoaXMpXG4gICAgdGhpcy5wYXVzZVJlc3VtZSA9IHRoaXMucGF1c2VSZXN1bWUuYmluZCh0aGlzKVxuXG4gICAgLy8gX19fV2h5IHRocm90dGxlIGF0IDUwMG1zP1xuICAgIC8vICAgIC0gV2UgbXVzdCB0aHJvdHRsZSBhdCA+MjUwbXMgZm9yIHN1cGVyZm9jdXMgaW4gRGFzaGJvYXJkIHRvIHdvcmsgd2VsbCAoYmVjYXVzZSBhbmltYXRpb24gdGFrZXMgMC4yNXMsIGFuZCB3ZSB3YW50IHRvIHdhaXQgZm9yIGFsbCBhbmltYXRpb25zIHRvIGJlIG92ZXIgYmVmb3JlIHJlZm9jdXNpbmcpLlxuICAgIC8vICAgIFtQcmFjdGljYWwgQ2hlY2tdOiBpZiB0aG90dGxlIGlzIGF0IDEwMG1zLCB0aGVuIGlmIHlvdSBhcmUgdXBsb2FkaW5nIGEgZmlsZSwgYW5kIGNsaWNrICdBREQgTU9SRSBGSUxFUycsIC0gZm9jdXMgd29uJ3QgYWN0aXZhdGUgaW4gRmlyZWZveC5cbiAgICAvLyAgICAtIFdlIG11c3QgdGhyb3R0bGUgYXQgYXJvdW5kID41MDBtcyB0byBhdm9pZCBwZXJmb3JtYW5jZSBsYWdzLlxuICAgIC8vICAgIFtQcmFjdGljYWwgQ2hlY2tdIEZpcmVmb3gsIHRyeSB0byB1cGxvYWQgYSBiaWcgZmlsZSBmb3IgYSBwcm9sb25nZWQgcGVyaW9kIG9mIHRpbWUuIExhcHRvcCB3aWxsIHN0YXJ0IHRvIGhlYXQgdXAuXG4gICAgdGhpcy5fY2FsY3VsYXRlUHJvZ3Jlc3MgPSB0aHJvdHRsZSh0aGlzLl9jYWxjdWxhdGVQcm9ncmVzcy5iaW5kKHRoaXMpLCA1MDAsIHsgbGVhZGluZzogdHJ1ZSwgdHJhaWxpbmc6IHRydWUgfSlcblxuICAgIHRoaXMudXBkYXRlT25saW5lU3RhdHVzID0gdGhpcy51cGRhdGVPbmxpbmVTdGF0dXMuYmluZCh0aGlzKVxuICAgIHRoaXMucmVzZXRQcm9ncmVzcyA9IHRoaXMucmVzZXRQcm9ncmVzcy5iaW5kKHRoaXMpXG5cbiAgICB0aGlzLnBhdXNlQWxsID0gdGhpcy5wYXVzZUFsbC5iaW5kKHRoaXMpXG4gICAgdGhpcy5yZXN1bWVBbGwgPSB0aGlzLnJlc3VtZUFsbC5iaW5kKHRoaXMpXG4gICAgdGhpcy5yZXRyeUFsbCA9IHRoaXMucmV0cnlBbGwuYmluZCh0aGlzKVxuICAgIHRoaXMuY2FuY2VsQWxsID0gdGhpcy5jYW5jZWxBbGwuYmluZCh0aGlzKVxuICAgIHRoaXMucmV0cnlVcGxvYWQgPSB0aGlzLnJldHJ5VXBsb2FkLmJpbmQodGhpcylcbiAgICB0aGlzLnVwbG9hZCA9IHRoaXMudXBsb2FkLmJpbmQodGhpcylcblxuICAgIHRoaXMuZW1pdHRlciA9IGVlKClcbiAgICB0aGlzLm9uID0gdGhpcy5vbi5iaW5kKHRoaXMpXG4gICAgdGhpcy5vZmYgPSB0aGlzLm9mZi5iaW5kKHRoaXMpXG4gICAgdGhpcy5vbmNlID0gdGhpcy5lbWl0dGVyLm9uY2UuYmluZCh0aGlzLmVtaXR0ZXIpXG4gICAgdGhpcy5lbWl0ID0gdGhpcy5lbWl0dGVyLmVtaXQuYmluZCh0aGlzLmVtaXR0ZXIpXG5cbiAgICB0aGlzLnByZVByb2Nlc3NvcnMgPSBbXVxuICAgIHRoaXMudXBsb2FkZXJzID0gW11cbiAgICB0aGlzLnBvc3RQcm9jZXNzb3JzID0gW11cblxuICAgIHRoaXMuc3RvcmUgPSB0aGlzLm9wdHMuc3RvcmVcbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIHBsdWdpbnM6IHt9LFxuICAgICAgZmlsZXM6IHt9LFxuICAgICAgY3VycmVudFVwbG9hZHM6IHt9LFxuICAgICAgYWxsb3dOZXdVcGxvYWQ6IHRydWUsXG4gICAgICBjYXBhYmlsaXRpZXM6IHtcbiAgICAgICAgdXBsb2FkUHJvZ3Jlc3M6IHN1cHBvcnRzVXBsb2FkUHJvZ3Jlc3MoKSxcbiAgICAgICAgaW5kaXZpZHVhbENhbmNlbGxhdGlvbjogdHJ1ZSxcbiAgICAgICAgcmVzdW1hYmxlVXBsb2FkczogZmFsc2VcbiAgICAgIH0sXG4gICAgICB0b3RhbFByb2dyZXNzOiAwLFxuICAgICAgbWV0YTogeyAuLi50aGlzLm9wdHMubWV0YSB9LFxuICAgICAgaW5mbzoge1xuICAgICAgICBpc0hpZGRlbjogdHJ1ZSxcbiAgICAgICAgdHlwZTogJ2luZm8nLFxuICAgICAgICBtZXNzYWdlOiAnJ1xuICAgICAgfVxuICAgIH0pXG5cbiAgICB0aGlzLl9zdG9yZVVuc3Vic2NyaWJlID0gdGhpcy5zdG9yZS5zdWJzY3JpYmUoKHByZXZTdGF0ZSwgbmV4dFN0YXRlLCBwYXRjaCkgPT4ge1xuICAgICAgdGhpcy5lbWl0KCdzdGF0ZS11cGRhdGUnLCBwcmV2U3RhdGUsIG5leHRTdGF0ZSwgcGF0Y2gpXG4gICAgICB0aGlzLnVwZGF0ZUFsbChuZXh0U3RhdGUpXG4gICAgfSlcblxuICAgIC8vIEV4cG9zaW5nIHVwcHkgb2JqZWN0IG9uIHdpbmRvdyBmb3IgZGVidWdnaW5nIGFuZCB0ZXN0aW5nXG4gICAgaWYgKHRoaXMub3B0cy5kZWJ1ZyAmJiB0eXBlb2Ygd2luZG93ICE9PSAndW5kZWZpbmVkJykge1xuICAgICAgd2luZG93W3RoaXMub3B0cy5pZF0gPSB0aGlzXG4gICAgfVxuXG4gICAgdGhpcy5fYWRkTGlzdGVuZXJzKClcbiAgfVxuXG4gIG9uIChldmVudCwgY2FsbGJhY2spIHtcbiAgICB0aGlzLmVtaXR0ZXIub24oZXZlbnQsIGNhbGxiYWNrKVxuICAgIHJldHVybiB0aGlzXG4gIH1cblxuICBvZmYgKGV2ZW50LCBjYWxsYmFjaykge1xuICAgIHRoaXMuZW1pdHRlci5vZmYoZXZlbnQsIGNhbGxiYWNrKVxuICAgIHJldHVybiB0aGlzXG4gIH1cblxuICAvKipcbiAgICogSXRlcmF0ZSBvbiBhbGwgcGx1Z2lucyBhbmQgcnVuIGB1cGRhdGVgIG9uIHRoZW0uXG4gICAqIENhbGxlZCBlYWNoIHRpbWUgc3RhdGUgY2hhbmdlcy5cbiAgICpcbiAgICovXG4gIHVwZGF0ZUFsbCAoc3RhdGUpIHtcbiAgICB0aGlzLml0ZXJhdGVQbHVnaW5zKHBsdWdpbiA9PiB7XG4gICAgICBwbHVnaW4udXBkYXRlKHN0YXRlKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogVXBkYXRlcyBzdGF0ZSB3aXRoIGEgcGF0Y2hcbiAgICpcbiAgICogQHBhcmFtIHtvYmplY3R9IHBhdGNoIHtmb286ICdiYXInfVxuICAgKi9cbiAgc2V0U3RhdGUgKHBhdGNoKSB7XG4gICAgdGhpcy5zdG9yZS5zZXRTdGF0ZShwYXRjaClcbiAgfVxuXG4gIC8qKlxuICAgKiBSZXR1cm5zIGN1cnJlbnQgc3RhdGUuXG4gICAqXG4gICAqIEByZXR1cm5zIHtvYmplY3R9XG4gICAqL1xuICBnZXRTdGF0ZSAoKSB7XG4gICAgcmV0dXJuIHRoaXMuc3RvcmUuZ2V0U3RhdGUoKVxuICB9XG5cbiAgLyoqXG4gICAqIEJhY2sgY29tcGF0IGZvciB3aGVuIHVwcHkuc3RhdGUgaXMgdXNlZCBpbnN0ZWFkIG9mIHVwcHkuZ2V0U3RhdGUoKS5cbiAgICovXG4gIGdldCBzdGF0ZSAoKSB7XG4gICAgcmV0dXJuIHRoaXMuZ2V0U3RhdGUoKVxuICB9XG5cbiAgLyoqXG4gICAqIFNob3J0aGFuZCB0byBzZXQgc3RhdGUgZm9yIGEgc3BlY2lmaWMgZmlsZS5cbiAgICovXG4gIHNldEZpbGVTdGF0ZSAoZmlsZUlELCBzdGF0ZSkge1xuICAgIGlmICghdGhpcy5nZXRTdGF0ZSgpLmZpbGVzW2ZpbGVJRF0pIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihgQ2Fu4oCZdCBzZXQgc3RhdGUgZm9yICR7ZmlsZUlEfSAodGhlIGZpbGUgY291bGQgaGF2ZSBiZWVuIHJlbW92ZWQpYClcbiAgICB9XG5cbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGZpbGVzOiBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldFN0YXRlKCkuZmlsZXMsIHtcbiAgICAgICAgW2ZpbGVJRF06IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuZ2V0U3RhdGUoKS5maWxlc1tmaWxlSURdLCBzdGF0ZSlcbiAgICAgIH0pXG4gICAgfSlcbiAgfVxuXG4gIHJlc2V0UHJvZ3Jlc3MgKCkge1xuICAgIGNvbnN0IGRlZmF1bHRQcm9ncmVzcyA9IHtcbiAgICAgIHBlcmNlbnRhZ2U6IDAsXG4gICAgICBieXRlc1VwbG9hZGVkOiAwLFxuICAgICAgdXBsb2FkQ29tcGxldGU6IGZhbHNlLFxuICAgICAgdXBsb2FkU3RhcnRlZDogbnVsbFxuICAgIH1cbiAgICBjb25zdCBmaWxlcyA9IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuZ2V0U3RhdGUoKS5maWxlcylcbiAgICBjb25zdCB1cGRhdGVkRmlsZXMgPSB7fVxuICAgIE9iamVjdC5rZXlzKGZpbGVzKS5mb3JFYWNoKGZpbGVJRCA9PiB7XG4gICAgICBjb25zdCB1cGRhdGVkRmlsZSA9IE9iamVjdC5hc3NpZ24oe30sIGZpbGVzW2ZpbGVJRF0pXG4gICAgICB1cGRhdGVkRmlsZS5wcm9ncmVzcyA9IE9iamVjdC5hc3NpZ24oe30sIHVwZGF0ZWRGaWxlLnByb2dyZXNzLCBkZWZhdWx0UHJvZ3Jlc3MpXG4gICAgICB1cGRhdGVkRmlsZXNbZmlsZUlEXSA9IHVwZGF0ZWRGaWxlXG4gICAgfSlcblxuICAgIHRoaXMuc2V0U3RhdGUoe1xuICAgICAgZmlsZXM6IHVwZGF0ZWRGaWxlcyxcbiAgICAgIHRvdGFsUHJvZ3Jlc3M6IDBcbiAgICB9KVxuXG4gICAgLy8gVE9ETyBEb2N1bWVudCBvbiB0aGUgd2Vic2l0ZVxuICAgIHRoaXMuZW1pdCgncmVzZXQtcHJvZ3Jlc3MnKVxuICB9XG5cbiAgYWRkUHJlUHJvY2Vzc29yIChmbikge1xuICAgIHRoaXMucHJlUHJvY2Vzc29ycy5wdXNoKGZuKVxuICB9XG5cbiAgcmVtb3ZlUHJlUHJvY2Vzc29yIChmbikge1xuICAgIGNvbnN0IGkgPSB0aGlzLnByZVByb2Nlc3NvcnMuaW5kZXhPZihmbilcbiAgICBpZiAoaSAhPT0gLTEpIHtcbiAgICAgIHRoaXMucHJlUHJvY2Vzc29ycy5zcGxpY2UoaSwgMSlcbiAgICB9XG4gIH1cblxuICBhZGRQb3N0UHJvY2Vzc29yIChmbikge1xuICAgIHRoaXMucG9zdFByb2Nlc3NvcnMucHVzaChmbilcbiAgfVxuXG4gIHJlbW92ZVBvc3RQcm9jZXNzb3IgKGZuKSB7XG4gICAgY29uc3QgaSA9IHRoaXMucG9zdFByb2Nlc3NvcnMuaW5kZXhPZihmbilcbiAgICBpZiAoaSAhPT0gLTEpIHtcbiAgICAgIHRoaXMucG9zdFByb2Nlc3NvcnMuc3BsaWNlKGksIDEpXG4gICAgfVxuICB9XG5cbiAgYWRkVXBsb2FkZXIgKGZuKSB7XG4gICAgdGhpcy51cGxvYWRlcnMucHVzaChmbilcbiAgfVxuXG4gIHJlbW92ZVVwbG9hZGVyIChmbikge1xuICAgIGNvbnN0IGkgPSB0aGlzLnVwbG9hZGVycy5pbmRleE9mKGZuKVxuICAgIGlmIChpICE9PSAtMSkge1xuICAgICAgdGhpcy51cGxvYWRlcnMuc3BsaWNlKGksIDEpXG4gICAgfVxuICB9XG5cbiAgc2V0TWV0YSAoZGF0YSkge1xuICAgIGNvbnN0IHVwZGF0ZWRNZXRhID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLm1ldGEsIGRhdGEpXG4gICAgY29uc3QgdXBkYXRlZEZpbGVzID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLmZpbGVzKVxuXG4gICAgT2JqZWN0LmtleXModXBkYXRlZEZpbGVzKS5mb3JFYWNoKChmaWxlSUQpID0+IHtcbiAgICAgIHVwZGF0ZWRGaWxlc1tmaWxlSURdID0gT2JqZWN0LmFzc2lnbih7fSwgdXBkYXRlZEZpbGVzW2ZpbGVJRF0sIHtcbiAgICAgICAgbWV0YTogT2JqZWN0LmFzc2lnbih7fSwgdXBkYXRlZEZpbGVzW2ZpbGVJRF0ubWV0YSwgZGF0YSlcbiAgICAgIH0pXG4gICAgfSlcblxuICAgIHRoaXMubG9nKCdBZGRpbmcgbWV0YWRhdGE6JylcbiAgICB0aGlzLmxvZyhkYXRhKVxuXG4gICAgdGhpcy5zZXRTdGF0ZSh7XG4gICAgICBtZXRhOiB1cGRhdGVkTWV0YSxcbiAgICAgIGZpbGVzOiB1cGRhdGVkRmlsZXNcbiAgICB9KVxuICB9XG5cbiAgc2V0RmlsZU1ldGEgKGZpbGVJRCwgZGF0YSkge1xuICAgIGNvbnN0IHVwZGF0ZWRGaWxlcyA9IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuZ2V0U3RhdGUoKS5maWxlcylcbiAgICBpZiAoIXVwZGF0ZWRGaWxlc1tmaWxlSURdKSB7XG4gICAgICB0aGlzLmxvZygnV2FzIHRyeWluZyB0byBzZXQgbWV0YWRhdGEgZm9yIGEgZmlsZSB0aGF0IGhhcyBiZWVuIHJlbW92ZWQ6ICcsIGZpbGVJRClcbiAgICAgIHJldHVyblxuICAgIH1cbiAgICBjb25zdCBuZXdNZXRhID0gT2JqZWN0LmFzc2lnbih7fSwgdXBkYXRlZEZpbGVzW2ZpbGVJRF0ubWV0YSwgZGF0YSlcbiAgICB1cGRhdGVkRmlsZXNbZmlsZUlEXSA9IE9iamVjdC5hc3NpZ24oe30sIHVwZGF0ZWRGaWxlc1tmaWxlSURdLCB7XG4gICAgICBtZXRhOiBuZXdNZXRhXG4gICAgfSlcbiAgICB0aGlzLnNldFN0YXRlKHsgZmlsZXM6IHVwZGF0ZWRGaWxlcyB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEdldCBhIGZpbGUgb2JqZWN0LlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZmlsZUlEIFRoZSBJRCBvZiB0aGUgZmlsZSBvYmplY3QgdG8gcmV0dXJuLlxuICAgKi9cbiAgZ2V0RmlsZSAoZmlsZUlEKSB7XG4gICAgcmV0dXJuIHRoaXMuZ2V0U3RhdGUoKS5maWxlc1tmaWxlSURdXG4gIH1cblxuICAvKipcbiAgICogR2V0IGFsbCBmaWxlcyBpbiBhbiBhcnJheS5cbiAgICovXG4gIGdldEZpbGVzICgpIHtcbiAgICBjb25zdCB7IGZpbGVzIH0gPSB0aGlzLmdldFN0YXRlKClcbiAgICByZXR1cm4gT2JqZWN0LmtleXMoZmlsZXMpLm1hcCgoZmlsZUlEKSA9PiBmaWxlc1tmaWxlSURdKVxuICB9XG5cbiAgLyoqXG4gICAqIENoZWNrIGlmIG1pbk51bWJlck9mRmlsZXMgcmVzdHJpY3Rpb24gaXMgcmVhY2hlZCBiZWZvcmUgdXBsb2FkaW5nLlxuICAgKlxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgX2NoZWNrTWluTnVtYmVyT2ZGaWxlcyAoZmlsZXMpIHtcbiAgICBjb25zdCB7IG1pbk51bWJlck9mRmlsZXMgfSA9IHRoaXMub3B0cy5yZXN0cmljdGlvbnNcbiAgICBpZiAoT2JqZWN0LmtleXMoZmlsZXMpLmxlbmd0aCA8IG1pbk51bWJlck9mRmlsZXMpIHtcbiAgICAgIHRocm93IG5ldyBSZXN0cmljdGlvbkVycm9yKGAke3RoaXMuaTE4bigneW91SGF2ZVRvQXRMZWFzdFNlbGVjdFgnLCB7IHNtYXJ0X2NvdW50OiBtaW5OdW1iZXJPZkZpbGVzIH0pfWApXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIENoZWNrIGlmIGZpbGUgcGFzc2VzIGEgc2V0IG9mIHJlc3RyaWN0aW9ucyBzZXQgaW4gb3B0aW9uczogbWF4RmlsZVNpemUsXG4gICAqIG1heE51bWJlck9mRmlsZXMgYW5kIGFsbG93ZWRGaWxlVHlwZXMuXG4gICAqXG4gICAqIEBwYXJhbSB7b2JqZWN0fSBmaWxlIG9iamVjdCB0byBjaGVja1xuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgX2NoZWNrUmVzdHJpY3Rpb25zIChmaWxlKSB7XG4gICAgY29uc3QgeyBtYXhGaWxlU2l6ZSwgbWF4TnVtYmVyT2ZGaWxlcywgYWxsb3dlZEZpbGVUeXBlcyB9ID0gdGhpcy5vcHRzLnJlc3RyaWN0aW9uc1xuXG4gICAgaWYgKG1heE51bWJlck9mRmlsZXMpIHtcbiAgICAgIGlmIChPYmplY3Qua2V5cyh0aGlzLmdldFN0YXRlKCkuZmlsZXMpLmxlbmd0aCArIDEgPiBtYXhOdW1iZXJPZkZpbGVzKSB7XG4gICAgICAgIHRocm93IG5ldyBSZXN0cmljdGlvbkVycm9yKGAke3RoaXMuaTE4bigneW91Q2FuT25seVVwbG9hZFgnLCB7IHNtYXJ0X2NvdW50OiBtYXhOdW1iZXJPZkZpbGVzIH0pfWApXG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGFsbG93ZWRGaWxlVHlwZXMpIHtcbiAgICAgIGNvbnN0IGlzQ29ycmVjdEZpbGVUeXBlID0gYWxsb3dlZEZpbGVUeXBlcy5zb21lKCh0eXBlKSA9PiB7XG4gICAgICAgIC8vIGlzIHRoaXMgaXMgYSBtaW1lLXR5cGVcbiAgICAgICAgaWYgKHR5cGUuaW5kZXhPZignLycpID4gLTEpIHtcbiAgICAgICAgICBpZiAoIWZpbGUudHlwZSkgcmV0dXJuIGZhbHNlXG4gICAgICAgICAgcmV0dXJuIG1hdGNoKGZpbGUudHlwZSwgdHlwZSlcbiAgICAgICAgfVxuXG4gICAgICAgIC8vIG90aGVyd2lzZSB0aGlzIGlzIGxpa2VseSBhbiBleHRlbnNpb25cbiAgICAgICAgaWYgKHR5cGVbMF0gPT09ICcuJykge1xuICAgICAgICAgIHJldHVybiBmaWxlLmV4dGVuc2lvbi50b0xvd2VyQ2FzZSgpID09PSB0eXBlLnN1YnN0cigxKS50b0xvd2VyQ2FzZSgpXG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGZhbHNlXG4gICAgICB9KVxuXG4gICAgICBpZiAoIWlzQ29ycmVjdEZpbGVUeXBlKSB7XG4gICAgICAgIGNvbnN0IGFsbG93ZWRGaWxlVHlwZXNTdHJpbmcgPSBhbGxvd2VkRmlsZVR5cGVzLmpvaW4oJywgJylcbiAgICAgICAgdGhyb3cgbmV3IFJlc3RyaWN0aW9uRXJyb3IodGhpcy5pMThuKCd5b3VDYW5Pbmx5VXBsb2FkRmlsZVR5cGVzJywgeyB0eXBlczogYWxsb3dlZEZpbGVUeXBlc1N0cmluZyB9KSlcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBXZSBjYW4ndCBjaGVjayBtYXhGaWxlU2l6ZSBpZiB0aGUgc2l6ZSBpcyB1bmtub3duLlxuICAgIGlmIChtYXhGaWxlU2l6ZSAmJiBmaWxlLmRhdGEuc2l6ZSAhPSBudWxsKSB7XG4gICAgICBpZiAoZmlsZS5kYXRhLnNpemUgPiBtYXhGaWxlU2l6ZSkge1xuICAgICAgICB0aHJvdyBuZXcgUmVzdHJpY3Rpb25FcnJvcihgJHt0aGlzLmkxOG4oJ2V4Y2VlZHNTaXplJyl9ICR7cHJldHR5Qnl0ZXMobWF4RmlsZVNpemUpfWApXG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgX3Nob3dPckxvZ0Vycm9yQW5kVGhyb3cgKGVyciwgeyBzaG93SW5mb3JtZXIgPSB0cnVlLCBmaWxlID0gbnVsbCB9ID0ge30pIHtcbiAgICBjb25zdCBtZXNzYWdlID0gdHlwZW9mIGVyciA9PT0gJ29iamVjdCcgPyBlcnIubWVzc2FnZSA6IGVyclxuICAgIGNvbnN0IGRldGFpbHMgPSAodHlwZW9mIGVyciA9PT0gJ29iamVjdCcgJiYgZXJyLmRldGFpbHMpID8gZXJyLmRldGFpbHMgOiAnJ1xuXG4gICAgLy8gUmVzdHJpY3Rpb24gZXJyb3JzIHNob3VsZCBiZSBsb2dnZWQsIGJ1dCBub3QgYXMgZXJyb3JzLFxuICAgIC8vIGFzIHRoZXkgYXJlIGV4cGVjdGVkIGFuZCBzaG93biBpbiB0aGUgVUkuXG4gICAgaWYgKGVyci5pc1Jlc3RyaWN0aW9uKSB7XG4gICAgICB0aGlzLmxvZyhgJHttZXNzYWdlfSAke2RldGFpbHN9YClcbiAgICAgIHRoaXMuZW1pdCgncmVzdHJpY3Rpb24tZmFpbGVkJywgZmlsZSwgZXJyKVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmxvZyhgJHttZXNzYWdlfSAke2RldGFpbHN9YCwgJ2Vycm9yJylcbiAgICB9XG5cbiAgICAvLyBTb21ldGltZXMgaW5mb3JtZXIgaGFzIHRvIGJlIHNob3duIG1hbnVhbGx5IGJ5IHRoZSBkZXZlbG9wZXIsXG4gICAgLy8gZm9yIGV4YW1wbGUsIGluIGBvbkJlZm9yZUZpbGVBZGRlZGAuXG4gICAgaWYgKHNob3dJbmZvcm1lcikge1xuICAgICAgdGhpcy5pbmZvKHsgbWVzc2FnZTogbWVzc2FnZSwgZGV0YWlsczogZGV0YWlscyB9LCAnZXJyb3InLCA1MDAwKVxuICAgIH1cblxuICAgIHRocm93ICh0eXBlb2YgZXJyID09PSAnb2JqZWN0JyA/IGVyciA6IG5ldyBFcnJvcihlcnIpKVxuICB9XG5cbiAgLyoqXG4gICAqIEFkZCBhIG5ldyBmaWxlIHRvIGBzdGF0ZS5maWxlc2AuIFRoaXMgd2lsbCBydW4gYG9uQmVmb3JlRmlsZUFkZGVkYCxcbiAgICogdHJ5IHRvIGd1ZXNzIGZpbGUgdHlwZSBpbiBhIGNsZXZlciB3YXksIGNoZWNrIGZpbGUgYWdhaW5zdCByZXN0cmljdGlvbnMsXG4gICAqIGFuZCBzdGFydCBhbiB1cGxvYWQgaWYgYGF1dG9Qcm9jZWVkID09PSB0cnVlYC5cbiAgICpcbiAgICogQHBhcmFtIHtvYmplY3R9IGZpbGUgb2JqZWN0IHRvIGFkZFxuICAgKiBAcmV0dXJucyB7c3RyaW5nfSBpZCBmb3IgdGhlIGFkZGVkIGZpbGVcbiAgICovXG4gIGFkZEZpbGUgKGZpbGUpIHtcbiAgICBjb25zdCB7IGZpbGVzLCBhbGxvd05ld1VwbG9hZCB9ID0gdGhpcy5nZXRTdGF0ZSgpXG5cbiAgICBpZiAoYWxsb3dOZXdVcGxvYWQgPT09IGZhbHNlKSB7XG4gICAgICB0aGlzLl9zaG93T3JMb2dFcnJvckFuZFRocm93KG5ldyBSZXN0cmljdGlvbkVycm9yKCdDYW5ub3QgYWRkIG5ldyBmaWxlczogYWxyZWFkeSB1cGxvYWRpbmcuJyksIHsgZmlsZSB9KVxuICAgIH1cblxuICAgIGNvbnN0IGZpbGVUeXBlID0gZ2V0RmlsZVR5cGUoZmlsZSlcbiAgICBmaWxlLnR5cGUgPSBmaWxlVHlwZVxuXG4gICAgY29uc3Qgb25CZWZvcmVGaWxlQWRkZWRSZXN1bHQgPSB0aGlzLm9wdHMub25CZWZvcmVGaWxlQWRkZWQoZmlsZSwgZmlsZXMpXG5cbiAgICBpZiAob25CZWZvcmVGaWxlQWRkZWRSZXN1bHQgPT09IGZhbHNlKSB7XG4gICAgICAvLyBEb27igJl0IHNob3cgVUkgaW5mbyBmb3IgdGhpcyBlcnJvciwgYXMgaXQgc2hvdWxkIGJlIGRvbmUgYnkgdGhlIGRldmVsb3BlclxuICAgICAgdGhpcy5fc2hvd09yTG9nRXJyb3JBbmRUaHJvdyhuZXcgUmVzdHJpY3Rpb25FcnJvcignQ2Fubm90IGFkZCB0aGUgZmlsZSBiZWNhdXNlIG9uQmVmb3JlRmlsZUFkZGVkIHJldHVybmVkIGZhbHNlLicpLCB7IHNob3dJbmZvcm1lcjogZmFsc2UsIGZpbGUgfSlcbiAgICB9XG5cbiAgICBpZiAodHlwZW9mIG9uQmVmb3JlRmlsZUFkZGVkUmVzdWx0ID09PSAnb2JqZWN0JyAmJiBvbkJlZm9yZUZpbGVBZGRlZFJlc3VsdCkge1xuICAgICAgZmlsZSA9IG9uQmVmb3JlRmlsZUFkZGVkUmVzdWx0XG4gICAgfVxuXG4gICAgbGV0IGZpbGVOYW1lXG4gICAgaWYgKGZpbGUubmFtZSkge1xuICAgICAgZmlsZU5hbWUgPSBmaWxlLm5hbWVcbiAgICB9IGVsc2UgaWYgKGZpbGVUeXBlLnNwbGl0KCcvJylbMF0gPT09ICdpbWFnZScpIHtcbiAgICAgIGZpbGVOYW1lID0gZmlsZVR5cGUuc3BsaXQoJy8nKVswXSArICcuJyArIGZpbGVUeXBlLnNwbGl0KCcvJylbMV1cbiAgICB9IGVsc2Uge1xuICAgICAgZmlsZU5hbWUgPSAnbm9uYW1lJ1xuICAgIH1cbiAgICBjb25zdCBmaWxlRXh0ZW5zaW9uID0gZ2V0RmlsZU5hbWVBbmRFeHRlbnNpb24oZmlsZU5hbWUpLmV4dGVuc2lvblxuICAgIGNvbnN0IGlzUmVtb3RlID0gZmlsZS5pc1JlbW90ZSB8fCBmYWxzZVxuXG4gICAgY29uc3QgZmlsZUlEID0gZ2VuZXJhdGVGaWxlSUQoZmlsZSlcblxuICAgIGlmIChmaWxlc1tmaWxlSURdKSB7XG4gICAgICB0aGlzLl9zaG93T3JMb2dFcnJvckFuZFRocm93KG5ldyBSZXN0cmljdGlvbkVycm9yKGBDYW5ub3QgYWRkIHRoZSBkdXBsaWNhdGUgZmlsZSAnJHtmaWxlTmFtZX0nLCBpdCBhbHJlYWR5IGV4aXN0cy5gKSwgeyBmaWxlIH0pXG4gICAgfVxuXG4gICAgY29uc3QgbWV0YSA9IGZpbGUubWV0YSB8fCB7fVxuICAgIG1ldGEubmFtZSA9IGZpbGVOYW1lXG4gICAgbWV0YS50eXBlID0gZmlsZVR5cGVcblxuICAgIC8vIGBudWxsYCBtZWFucyB0aGUgc2l6ZSBpcyB1bmtub3duLlxuICAgIGNvbnN0IHNpemUgPSBpc0Zpbml0ZShmaWxlLmRhdGEuc2l6ZSkgPyBmaWxlLmRhdGEuc2l6ZSA6IG51bGxcbiAgICBjb25zdCBuZXdGaWxlID0ge1xuICAgICAgc291cmNlOiBmaWxlLnNvdXJjZSB8fCAnJyxcbiAgICAgIGlkOiBmaWxlSUQsXG4gICAgICBuYW1lOiBmaWxlTmFtZSxcbiAgICAgIGV4dGVuc2lvbjogZmlsZUV4dGVuc2lvbiB8fCAnJyxcbiAgICAgIG1ldGE6IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuZ2V0U3RhdGUoKS5tZXRhLCBtZXRhKSxcbiAgICAgIHR5cGU6IGZpbGVUeXBlLFxuICAgICAgZGF0YTogZmlsZS5kYXRhLFxuICAgICAgcHJvZ3Jlc3M6IHtcbiAgICAgICAgcGVyY2VudGFnZTogMCxcbiAgICAgICAgYnl0ZXNVcGxvYWRlZDogMCxcbiAgICAgICAgYnl0ZXNUb3RhbDogc2l6ZSxcbiAgICAgICAgdXBsb2FkQ29tcGxldGU6IGZhbHNlLFxuICAgICAgICB1cGxvYWRTdGFydGVkOiBudWxsXG4gICAgICB9LFxuICAgICAgc2l6ZTogc2l6ZSxcbiAgICAgIGlzUmVtb3RlOiBpc1JlbW90ZSxcbiAgICAgIHJlbW90ZTogZmlsZS5yZW1vdGUgfHwgJycsXG4gICAgICBwcmV2aWV3OiBmaWxlLnByZXZpZXdcbiAgICB9XG5cbiAgICB0cnkge1xuICAgICAgdGhpcy5fY2hlY2tSZXN0cmljdGlvbnMobmV3RmlsZSlcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIHRoaXMuX3Nob3dPckxvZ0Vycm9yQW5kVGhyb3coZXJyLCB7IGZpbGU6IG5ld0ZpbGUgfSlcbiAgICB9XG5cbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGZpbGVzOiBPYmplY3QuYXNzaWduKHt9LCBmaWxlcywge1xuICAgICAgICBbZmlsZUlEXTogbmV3RmlsZVxuICAgICAgfSlcbiAgICB9KVxuXG4gICAgdGhpcy5lbWl0KCdmaWxlLWFkZGVkJywgbmV3RmlsZSlcbiAgICB0aGlzLmxvZyhgQWRkZWQgZmlsZTogJHtmaWxlTmFtZX0sICR7ZmlsZUlEfSwgbWltZSB0eXBlOiAke2ZpbGVUeXBlfWApXG5cbiAgICBpZiAodGhpcy5vcHRzLmF1dG9Qcm9jZWVkICYmICF0aGlzLnNjaGVkdWxlZEF1dG9Qcm9jZWVkKSB7XG4gICAgICB0aGlzLnNjaGVkdWxlZEF1dG9Qcm9jZWVkID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuc2NoZWR1bGVkQXV0b1Byb2NlZWQgPSBudWxsXG4gICAgICAgIHRoaXMudXBsb2FkKCkuY2F0Y2goKGVycikgPT4ge1xuICAgICAgICAgIGlmICghZXJyLmlzUmVzdHJpY3Rpb24pIHtcbiAgICAgICAgICAgIHRoaXMubG9nKGVyci5zdGFjayB8fCBlcnIubWVzc2FnZSB8fCBlcnIpXG4gICAgICAgICAgfVxuICAgICAgICB9KVxuICAgICAgfSwgNClcbiAgICB9XG5cbiAgICByZXR1cm4gZmlsZUlEXG4gIH1cblxuICByZW1vdmVGaWxlIChmaWxlSUQpIHtcbiAgICBjb25zdCB7IGZpbGVzLCBjdXJyZW50VXBsb2FkcyB9ID0gdGhpcy5nZXRTdGF0ZSgpXG4gICAgY29uc3QgdXBkYXRlZEZpbGVzID0gT2JqZWN0LmFzc2lnbih7fSwgZmlsZXMpXG4gICAgY29uc3QgcmVtb3ZlZEZpbGUgPSB1cGRhdGVkRmlsZXNbZmlsZUlEXVxuICAgIGRlbGV0ZSB1cGRhdGVkRmlsZXNbZmlsZUlEXVxuXG4gICAgLy8gUmVtb3ZlIHRoaXMgZmlsZSBmcm9tIGl0cyBgY3VycmVudFVwbG9hZGAuXG4gICAgY29uc3QgdXBkYXRlZFVwbG9hZHMgPSBPYmplY3QuYXNzaWduKHt9LCBjdXJyZW50VXBsb2FkcylcbiAgICBjb25zdCByZW1vdmVVcGxvYWRzID0gW11cbiAgICBPYmplY3Qua2V5cyh1cGRhdGVkVXBsb2FkcykuZm9yRWFjaCgodXBsb2FkSUQpID0+IHtcbiAgICAgIGNvbnN0IG5ld0ZpbGVJRHMgPSBjdXJyZW50VXBsb2Fkc1t1cGxvYWRJRF0uZmlsZUlEcy5maWx0ZXIoKHVwbG9hZEZpbGVJRCkgPT4gdXBsb2FkRmlsZUlEICE9PSBmaWxlSUQpXG4gICAgICAvLyBSZW1vdmUgdGhlIHVwbG9hZCBpZiBubyBmaWxlcyBhcmUgYXNzb2NpYXRlZCB3aXRoIGl0IGFueW1vcmUuXG4gICAgICBpZiAobmV3RmlsZUlEcy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgcmVtb3ZlVXBsb2Fkcy5wdXNoKHVwbG9hZElEKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cblxuICAgICAgdXBkYXRlZFVwbG9hZHNbdXBsb2FkSURdID0gT2JqZWN0LmFzc2lnbih7fSwgY3VycmVudFVwbG9hZHNbdXBsb2FkSURdLCB7XG4gICAgICAgIGZpbGVJRHM6IG5ld0ZpbGVJRHNcbiAgICAgIH0pXG4gICAgfSlcblxuICAgIHRoaXMuc2V0U3RhdGUoe1xuICAgICAgY3VycmVudFVwbG9hZHM6IHVwZGF0ZWRVcGxvYWRzLFxuICAgICAgZmlsZXM6IHVwZGF0ZWRGaWxlcyxcbiAgICAgIC4uLihcbiAgICAgICAgLy8gSWYgdGhpcyBpcyB0aGUgbGFzdCBmaWxlIHdlIGp1c3QgcmVtb3ZlZCAtIGFsbG93IG5ldyB1cGxvYWRzIVxuICAgICAgICBPYmplY3Qua2V5cyh1cGRhdGVkRmlsZXMpLmxlbmd0aCA9PT0gMCAmJlxuICAgICAgICB7IGFsbG93TmV3VXBsb2FkOiB0cnVlIH1cbiAgICAgIClcbiAgICB9KVxuXG4gICAgcmVtb3ZlVXBsb2Fkcy5mb3JFYWNoKCh1cGxvYWRJRCkgPT4ge1xuICAgICAgdGhpcy5fcmVtb3ZlVXBsb2FkKHVwbG9hZElEKVxuICAgIH0pXG5cbiAgICB0aGlzLl9jYWxjdWxhdGVUb3RhbFByb2dyZXNzKClcbiAgICB0aGlzLmVtaXQoJ2ZpbGUtcmVtb3ZlZCcsIHJlbW92ZWRGaWxlKVxuICAgIHRoaXMubG9nKGBGaWxlIHJlbW92ZWQ6ICR7cmVtb3ZlZEZpbGUuaWR9YClcbiAgfVxuXG4gIHBhdXNlUmVzdW1lIChmaWxlSUQpIHtcbiAgICBpZiAoIXRoaXMuZ2V0U3RhdGUoKS5jYXBhYmlsaXRpZXMucmVzdW1hYmxlVXBsb2FkcyB8fFxuICAgICAgICAgdGhpcy5nZXRGaWxlKGZpbGVJRCkudXBsb2FkQ29tcGxldGUpIHtcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIGNvbnN0IHdhc1BhdXNlZCA9IHRoaXMuZ2V0RmlsZShmaWxlSUQpLmlzUGF1c2VkIHx8IGZhbHNlXG4gICAgY29uc3QgaXNQYXVzZWQgPSAhd2FzUGF1c2VkXG5cbiAgICB0aGlzLnNldEZpbGVTdGF0ZShmaWxlSUQsIHtcbiAgICAgIGlzUGF1c2VkOiBpc1BhdXNlZFxuICAgIH0pXG5cbiAgICB0aGlzLmVtaXQoJ3VwbG9hZC1wYXVzZScsIGZpbGVJRCwgaXNQYXVzZWQpXG5cbiAgICByZXR1cm4gaXNQYXVzZWRcbiAgfVxuXG4gIHBhdXNlQWxsICgpIHtcbiAgICBjb25zdCB1cGRhdGVkRmlsZXMgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldFN0YXRlKCkuZmlsZXMpXG4gICAgY29uc3QgaW5Qcm9ncmVzc1VwZGF0ZWRGaWxlcyA9IE9iamVjdC5rZXlzKHVwZGF0ZWRGaWxlcykuZmlsdGVyKChmaWxlKSA9PiB7XG4gICAgICByZXR1cm4gIXVwZGF0ZWRGaWxlc1tmaWxlXS5wcm9ncmVzcy51cGxvYWRDb21wbGV0ZSAmJlxuICAgICAgICAgICAgIHVwZGF0ZWRGaWxlc1tmaWxlXS5wcm9ncmVzcy51cGxvYWRTdGFydGVkXG4gICAgfSlcblxuICAgIGluUHJvZ3Jlc3NVcGRhdGVkRmlsZXMuZm9yRWFjaCgoZmlsZSkgPT4ge1xuICAgICAgY29uc3QgdXBkYXRlZEZpbGUgPSBPYmplY3QuYXNzaWduKHt9LCB1cGRhdGVkRmlsZXNbZmlsZV0sIHtcbiAgICAgICAgaXNQYXVzZWQ6IHRydWVcbiAgICAgIH0pXG4gICAgICB1cGRhdGVkRmlsZXNbZmlsZV0gPSB1cGRhdGVkRmlsZVxuICAgIH0pXG4gICAgdGhpcy5zZXRTdGF0ZSh7IGZpbGVzOiB1cGRhdGVkRmlsZXMgfSlcblxuICAgIHRoaXMuZW1pdCgncGF1c2UtYWxsJylcbiAgfVxuXG4gIHJlc3VtZUFsbCAoKSB7XG4gICAgY29uc3QgdXBkYXRlZEZpbGVzID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLmZpbGVzKVxuICAgIGNvbnN0IGluUHJvZ3Jlc3NVcGRhdGVkRmlsZXMgPSBPYmplY3Qua2V5cyh1cGRhdGVkRmlsZXMpLmZpbHRlcigoZmlsZSkgPT4ge1xuICAgICAgcmV0dXJuICF1cGRhdGVkRmlsZXNbZmlsZV0ucHJvZ3Jlc3MudXBsb2FkQ29tcGxldGUgJiZcbiAgICAgICAgICAgICB1cGRhdGVkRmlsZXNbZmlsZV0ucHJvZ3Jlc3MudXBsb2FkU3RhcnRlZFxuICAgIH0pXG5cbiAgICBpblByb2dyZXNzVXBkYXRlZEZpbGVzLmZvckVhY2goKGZpbGUpID0+IHtcbiAgICAgIGNvbnN0IHVwZGF0ZWRGaWxlID0gT2JqZWN0LmFzc2lnbih7fSwgdXBkYXRlZEZpbGVzW2ZpbGVdLCB7XG4gICAgICAgIGlzUGF1c2VkOiBmYWxzZSxcbiAgICAgICAgZXJyb3I6IG51bGxcbiAgICAgIH0pXG4gICAgICB1cGRhdGVkRmlsZXNbZmlsZV0gPSB1cGRhdGVkRmlsZVxuICAgIH0pXG4gICAgdGhpcy5zZXRTdGF0ZSh7IGZpbGVzOiB1cGRhdGVkRmlsZXMgfSlcblxuICAgIHRoaXMuZW1pdCgncmVzdW1lLWFsbCcpXG4gIH1cblxuICByZXRyeUFsbCAoKSB7XG4gICAgY29uc3QgdXBkYXRlZEZpbGVzID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLmZpbGVzKVxuICAgIGNvbnN0IGZpbGVzVG9SZXRyeSA9IE9iamVjdC5rZXlzKHVwZGF0ZWRGaWxlcykuZmlsdGVyKGZpbGUgPT4ge1xuICAgICAgcmV0dXJuIHVwZGF0ZWRGaWxlc1tmaWxlXS5lcnJvclxuICAgIH0pXG5cbiAgICBmaWxlc1RvUmV0cnkuZm9yRWFjaCgoZmlsZSkgPT4ge1xuICAgICAgY29uc3QgdXBkYXRlZEZpbGUgPSBPYmplY3QuYXNzaWduKHt9LCB1cGRhdGVkRmlsZXNbZmlsZV0sIHtcbiAgICAgICAgaXNQYXVzZWQ6IGZhbHNlLFxuICAgICAgICBlcnJvcjogbnVsbFxuICAgICAgfSlcbiAgICAgIHVwZGF0ZWRGaWxlc1tmaWxlXSA9IHVwZGF0ZWRGaWxlXG4gICAgfSlcbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGZpbGVzOiB1cGRhdGVkRmlsZXMsXG4gICAgICBlcnJvcjogbnVsbFxuICAgIH0pXG5cbiAgICB0aGlzLmVtaXQoJ3JldHJ5LWFsbCcsIGZpbGVzVG9SZXRyeSlcblxuICAgIGNvbnN0IHVwbG9hZElEID0gdGhpcy5fY3JlYXRlVXBsb2FkKGZpbGVzVG9SZXRyeSlcbiAgICByZXR1cm4gdGhpcy5fcnVuVXBsb2FkKHVwbG9hZElEKVxuICB9XG5cbiAgY2FuY2VsQWxsICgpIHtcbiAgICB0aGlzLmVtaXQoJ2NhbmNlbC1hbGwnKVxuXG4gICAgY29uc3QgZmlsZXMgPSBPYmplY3Qua2V5cyh0aGlzLmdldFN0YXRlKCkuZmlsZXMpXG4gICAgZmlsZXMuZm9yRWFjaCgoZmlsZUlEKSA9PiB7XG4gICAgICB0aGlzLnJlbW92ZUZpbGUoZmlsZUlEKVxuICAgIH0pXG5cbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIHRvdGFsUHJvZ3Jlc3M6IDAsXG4gICAgICBlcnJvcjogbnVsbFxuICAgIH0pXG4gIH1cblxuICByZXRyeVVwbG9hZCAoZmlsZUlEKSB7XG4gICAgdGhpcy5zZXRGaWxlU3RhdGUoZmlsZUlELCB7XG4gICAgICBlcnJvcjogbnVsbCxcbiAgICAgIGlzUGF1c2VkOiBmYWxzZVxuICAgIH0pXG5cbiAgICB0aGlzLmVtaXQoJ3VwbG9hZC1yZXRyeScsIGZpbGVJRClcblxuICAgIGNvbnN0IHVwbG9hZElEID0gdGhpcy5fY3JlYXRlVXBsb2FkKFtmaWxlSURdKVxuICAgIHJldHVybiB0aGlzLl9ydW5VcGxvYWQodXBsb2FkSUQpXG4gIH1cblxuICByZXNldCAoKSB7XG4gICAgdGhpcy5jYW5jZWxBbGwoKVxuICB9XG5cbiAgX2NhbGN1bGF0ZVByb2dyZXNzIChmaWxlLCBkYXRhKSB7XG4gICAgaWYgKCF0aGlzLmdldEZpbGUoZmlsZS5pZCkpIHtcbiAgICAgIHRoaXMubG9nKGBOb3Qgc2V0dGluZyBwcm9ncmVzcyBmb3IgYSBmaWxlIHRoYXQgaGFzIGJlZW4gcmVtb3ZlZDogJHtmaWxlLmlkfWApXG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICAvLyBieXRlc1RvdGFsIG1heSBiZSBudWxsIG9yIHplcm87IGluIHRoYXQgY2FzZSB3ZSBjYW4ndCBkaXZpZGUgYnkgaXRcbiAgICBjb25zdCBjYW5IYXZlUGVyY2VudGFnZSA9IGlzRmluaXRlKGRhdGEuYnl0ZXNUb3RhbCkgJiYgZGF0YS5ieXRlc1RvdGFsID4gMFxuICAgIHRoaXMuc2V0RmlsZVN0YXRlKGZpbGUuaWQsIHtcbiAgICAgIHByb2dyZXNzOiBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldEZpbGUoZmlsZS5pZCkucHJvZ3Jlc3MsIHtcbiAgICAgICAgYnl0ZXNVcGxvYWRlZDogZGF0YS5ieXRlc1VwbG9hZGVkLFxuICAgICAgICBieXRlc1RvdGFsOiBkYXRhLmJ5dGVzVG90YWwsXG4gICAgICAgIHBlcmNlbnRhZ2U6IGNhbkhhdmVQZXJjZW50YWdlXG4gICAgICAgICAgLy8gVE9ETyhnb3RvLWJ1cy1zdG9wKSBmbG9vcmluZyB0aGlzIHNob3VsZCBwcm9iYWJseSBiZSB0aGUgY2hvaWNlIG9mIHRoZSBVST9cbiAgICAgICAgICAvLyB3ZSBnZXQgbW9yZSBhY2N1cmF0ZSBjYWxjdWxhdGlvbnMgaWYgd2UgZG9uJ3Qgcm91bmQgdGhpcyBhdCBhbGwuXG4gICAgICAgICAgPyBNYXRoLnJvdW5kKGRhdGEuYnl0ZXNVcGxvYWRlZCAvIGRhdGEuYnl0ZXNUb3RhbCAqIDEwMClcbiAgICAgICAgICA6IDBcbiAgICAgIH0pXG4gICAgfSlcblxuICAgIHRoaXMuX2NhbGN1bGF0ZVRvdGFsUHJvZ3Jlc3MoKVxuICB9XG5cbiAgX2NhbGN1bGF0ZVRvdGFsUHJvZ3Jlc3MgKCkge1xuICAgIC8vIGNhbGN1bGF0ZSB0b3RhbCBwcm9ncmVzcywgdXNpbmcgdGhlIG51bWJlciBvZiBmaWxlcyBjdXJyZW50bHkgdXBsb2FkaW5nLFxuICAgIC8vIG11bHRpcGxpZWQgYnkgMTAwIGFuZCB0aGUgc3VtbSBvZiBpbmRpdmlkdWFsIHByb2dyZXNzIG9mIGVhY2ggZmlsZVxuICAgIGNvbnN0IGZpbGVzID0gdGhpcy5nZXRGaWxlcygpXG5cbiAgICBjb25zdCBpblByb2dyZXNzID0gZmlsZXMuZmlsdGVyKChmaWxlKSA9PiB7XG4gICAgICByZXR1cm4gZmlsZS5wcm9ncmVzcy51cGxvYWRTdGFydGVkXG4gICAgfSlcblxuICAgIGlmIChpblByb2dyZXNzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgdGhpcy5lbWl0KCdwcm9ncmVzcycsIDApXG4gICAgICB0aGlzLnNldFN0YXRlKHsgdG90YWxQcm9ncmVzczogMCB9KVxuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgY29uc3Qgc2l6ZWRGaWxlcyA9IGluUHJvZ3Jlc3MuZmlsdGVyKChmaWxlKSA9PiBmaWxlLnByb2dyZXNzLmJ5dGVzVG90YWwgIT0gbnVsbClcbiAgICBjb25zdCB1bnNpemVkRmlsZXMgPSBpblByb2dyZXNzLmZpbHRlcigoZmlsZSkgPT4gZmlsZS5wcm9ncmVzcy5ieXRlc1RvdGFsID09IG51bGwpXG5cbiAgICBpZiAoc2l6ZWRGaWxlcy5sZW5ndGggPT09IDApIHtcbiAgICAgIGNvbnN0IHByb2dyZXNzTWF4ID0gaW5Qcm9ncmVzcy5sZW5ndGggKiAxMDBcbiAgICAgIGNvbnN0IGN1cnJlbnRQcm9ncmVzcyA9IHVuc2l6ZWRGaWxlcy5yZWR1Y2UoKGFjYywgZmlsZSkgPT4ge1xuICAgICAgICByZXR1cm4gYWNjICsgZmlsZS5wcm9ncmVzcy5wZXJjZW50YWdlXG4gICAgICB9LCAwKVxuICAgICAgY29uc3QgdG90YWxQcm9ncmVzcyA9IE1hdGgucm91bmQoY3VycmVudFByb2dyZXNzIC8gcHJvZ3Jlc3NNYXggKiAxMDApXG4gICAgICB0aGlzLnNldFN0YXRlKHsgdG90YWxQcm9ncmVzcyB9KVxuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgbGV0IHRvdGFsU2l6ZSA9IHNpemVkRmlsZXMucmVkdWNlKChhY2MsIGZpbGUpID0+IHtcbiAgICAgIHJldHVybiBhY2MgKyBmaWxlLnByb2dyZXNzLmJ5dGVzVG90YWxcbiAgICB9LCAwKVxuICAgIGNvbnN0IGF2ZXJhZ2VTaXplID0gdG90YWxTaXplIC8gc2l6ZWRGaWxlcy5sZW5ndGhcbiAgICB0b3RhbFNpemUgKz0gYXZlcmFnZVNpemUgKiB1bnNpemVkRmlsZXMubGVuZ3RoXG5cbiAgICBsZXQgdXBsb2FkZWRTaXplID0gMFxuICAgIHNpemVkRmlsZXMuZm9yRWFjaCgoZmlsZSkgPT4ge1xuICAgICAgdXBsb2FkZWRTaXplICs9IGZpbGUucHJvZ3Jlc3MuYnl0ZXNVcGxvYWRlZFxuICAgIH0pXG4gICAgdW5zaXplZEZpbGVzLmZvckVhY2goKGZpbGUpID0+IHtcbiAgICAgIHVwbG9hZGVkU2l6ZSArPSBhdmVyYWdlU2l6ZSAqIChmaWxlLnByb2dyZXNzLnBlcmNlbnRhZ2UgfHwgMCkgLyAxMDBcbiAgICB9KVxuXG4gICAgbGV0IHRvdGFsUHJvZ3Jlc3MgPSB0b3RhbFNpemUgPT09IDBcbiAgICAgID8gMFxuICAgICAgOiBNYXRoLnJvdW5kKHVwbG9hZGVkU2l6ZSAvIHRvdGFsU2l6ZSAqIDEwMClcblxuICAgIC8vIGhvdCBmaXgsIGJlY2F1c2U6XG4gICAgLy8gdXBsb2FkZWRTaXplIGVuZGVkIHVwIGxhcmdlciB0aGFuIHRvdGFsU2l6ZSwgcmVzdWx0aW5nIGluIDEzMjUlIHRvdGFsXG4gICAgaWYgKHRvdGFsUHJvZ3Jlc3MgPiAxMDApIHtcbiAgICAgIHRvdGFsUHJvZ3Jlc3MgPSAxMDBcbiAgICB9XG5cbiAgICB0aGlzLnNldFN0YXRlKHsgdG90YWxQcm9ncmVzcyB9KVxuICAgIHRoaXMuZW1pdCgncHJvZ3Jlc3MnLCB0b3RhbFByb2dyZXNzKVxuICB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBsaXN0ZW5lcnMgZm9yIGFsbCBnbG9iYWwgYWN0aW9ucywgbGlrZTpcbiAgICogYGVycm9yYCwgYGZpbGUtcmVtb3ZlZGAsIGB1cGxvYWQtcHJvZ3Jlc3NgXG4gICAqL1xuICBfYWRkTGlzdGVuZXJzICgpIHtcbiAgICB0aGlzLm9uKCdlcnJvcicsIChlcnJvcikgPT4ge1xuICAgICAgdGhpcy5zZXRTdGF0ZSh7IGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdVbmtub3duIGVycm9yJyB9KVxuICAgIH0pXG5cbiAgICB0aGlzLm9uKCd1cGxvYWQtZXJyb3InLCAoZmlsZSwgZXJyb3IsIHJlc3BvbnNlKSA9PiB7XG4gICAgICB0aGlzLnNldEZpbGVTdGF0ZShmaWxlLmlkLCB7XG4gICAgICAgIGVycm9yOiBlcnJvci5tZXNzYWdlIHx8ICdVbmtub3duIGVycm9yJyxcbiAgICAgICAgcmVzcG9uc2VcbiAgICAgIH0pXG5cbiAgICAgIHRoaXMuc2V0U3RhdGUoeyBlcnJvcjogZXJyb3IubWVzc2FnZSB9KVxuXG4gICAgICBsZXQgbWVzc2FnZSA9IHRoaXMuaTE4bignZmFpbGVkVG9VcGxvYWQnLCB7IGZpbGU6IGZpbGUubmFtZSB9KVxuICAgICAgaWYgKHR5cGVvZiBlcnJvciA9PT0gJ29iamVjdCcgJiYgZXJyb3IubWVzc2FnZSkge1xuICAgICAgICBtZXNzYWdlID0geyBtZXNzYWdlOiBtZXNzYWdlLCBkZXRhaWxzOiBlcnJvci5tZXNzYWdlIH1cbiAgICAgIH1cbiAgICAgIHRoaXMuaW5mbyhtZXNzYWdlLCAnZXJyb3InLCA1MDAwKVxuICAgIH0pXG5cbiAgICB0aGlzLm9uKCd1cGxvYWQnLCAoKSA9PiB7XG4gICAgICB0aGlzLnNldFN0YXRlKHsgZXJyb3I6IG51bGwgfSlcbiAgICB9KVxuXG4gICAgdGhpcy5vbigndXBsb2FkLXN0YXJ0ZWQnLCAoZmlsZSwgdXBsb2FkKSA9PiB7XG4gICAgICBpZiAoIXRoaXMuZ2V0RmlsZShmaWxlLmlkKSkge1xuICAgICAgICB0aGlzLmxvZyhgTm90IHNldHRpbmcgcHJvZ3Jlc3MgZm9yIGEgZmlsZSB0aGF0IGhhcyBiZWVuIHJlbW92ZWQ6ICR7ZmlsZS5pZH1gKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIHRoaXMuc2V0RmlsZVN0YXRlKGZpbGUuaWQsIHtcbiAgICAgICAgcHJvZ3Jlc3M6IHtcbiAgICAgICAgICB1cGxvYWRTdGFydGVkOiBEYXRlLm5vdygpLFxuICAgICAgICAgIHVwbG9hZENvbXBsZXRlOiBmYWxzZSxcbiAgICAgICAgICBwZXJjZW50YWdlOiAwLFxuICAgICAgICAgIGJ5dGVzVXBsb2FkZWQ6IDAsXG4gICAgICAgICAgYnl0ZXNUb3RhbDogZmlsZS5zaXplXG4gICAgICAgIH1cbiAgICAgIH0pXG4gICAgfSlcblxuICAgIHRoaXMub24oJ3VwbG9hZC1wcm9ncmVzcycsIHRoaXMuX2NhbGN1bGF0ZVByb2dyZXNzKVxuXG4gICAgdGhpcy5vbigndXBsb2FkLXN1Y2Nlc3MnLCAoZmlsZSwgdXBsb2FkUmVzcCkgPT4ge1xuICAgICAgaWYgKCF0aGlzLmdldEZpbGUoZmlsZS5pZCkpIHtcbiAgICAgICAgdGhpcy5sb2coYE5vdCBzZXR0aW5nIHByb2dyZXNzIGZvciBhIGZpbGUgdGhhdCBoYXMgYmVlbiByZW1vdmVkOiAke2ZpbGUuaWR9YClcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGN1cnJlbnRQcm9ncmVzcyA9IHRoaXMuZ2V0RmlsZShmaWxlLmlkKS5wcm9ncmVzc1xuICAgICAgdGhpcy5zZXRGaWxlU3RhdGUoZmlsZS5pZCwge1xuICAgICAgICBwcm9ncmVzczogT2JqZWN0LmFzc2lnbih7fSwgY3VycmVudFByb2dyZXNzLCB7XG4gICAgICAgICAgdXBsb2FkQ29tcGxldGU6IHRydWUsXG4gICAgICAgICAgcGVyY2VudGFnZTogMTAwLFxuICAgICAgICAgIGJ5dGVzVXBsb2FkZWQ6IGN1cnJlbnRQcm9ncmVzcy5ieXRlc1RvdGFsXG4gICAgICAgIH0pLFxuICAgICAgICByZXNwb25zZTogdXBsb2FkUmVzcCxcbiAgICAgICAgdXBsb2FkVVJMOiB1cGxvYWRSZXNwLnVwbG9hZFVSTCxcbiAgICAgICAgaXNQYXVzZWQ6IGZhbHNlXG4gICAgICB9KVxuXG4gICAgICB0aGlzLl9jYWxjdWxhdGVUb3RhbFByb2dyZXNzKClcbiAgICB9KVxuXG4gICAgdGhpcy5vbigncHJlcHJvY2Vzcy1wcm9ncmVzcycsIChmaWxlLCBwcm9ncmVzcykgPT4ge1xuICAgICAgaWYgKCF0aGlzLmdldEZpbGUoZmlsZS5pZCkpIHtcbiAgICAgICAgdGhpcy5sb2coYE5vdCBzZXR0aW5nIHByb2dyZXNzIGZvciBhIGZpbGUgdGhhdCBoYXMgYmVlbiByZW1vdmVkOiAke2ZpbGUuaWR9YClcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG4gICAgICB0aGlzLnNldEZpbGVTdGF0ZShmaWxlLmlkLCB7XG4gICAgICAgIHByb2dyZXNzOiBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldEZpbGUoZmlsZS5pZCkucHJvZ3Jlc3MsIHtcbiAgICAgICAgICBwcmVwcm9jZXNzOiBwcm9ncmVzc1xuICAgICAgICB9KVxuICAgICAgfSlcbiAgICB9KVxuXG4gICAgdGhpcy5vbigncHJlcHJvY2Vzcy1jb21wbGV0ZScsIChmaWxlKSA9PiB7XG4gICAgICBpZiAoIXRoaXMuZ2V0RmlsZShmaWxlLmlkKSkge1xuICAgICAgICB0aGlzLmxvZyhgTm90IHNldHRpbmcgcHJvZ3Jlc3MgZm9yIGEgZmlsZSB0aGF0IGhhcyBiZWVuIHJlbW92ZWQ6ICR7ZmlsZS5pZH1gKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIGNvbnN0IGZpbGVzID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLmZpbGVzKVxuICAgICAgZmlsZXNbZmlsZS5pZF0gPSBPYmplY3QuYXNzaWduKHt9LCBmaWxlc1tmaWxlLmlkXSwge1xuICAgICAgICBwcm9ncmVzczogT2JqZWN0LmFzc2lnbih7fSwgZmlsZXNbZmlsZS5pZF0ucHJvZ3Jlc3MpXG4gICAgICB9KVxuICAgICAgZGVsZXRlIGZpbGVzW2ZpbGUuaWRdLnByb2dyZXNzLnByZXByb2Nlc3NcblxuICAgICAgdGhpcy5zZXRTdGF0ZSh7IGZpbGVzOiBmaWxlcyB9KVxuICAgIH0pXG5cbiAgICB0aGlzLm9uKCdwb3N0cHJvY2Vzcy1wcm9ncmVzcycsIChmaWxlLCBwcm9ncmVzcykgPT4ge1xuICAgICAgaWYgKCF0aGlzLmdldEZpbGUoZmlsZS5pZCkpIHtcbiAgICAgICAgdGhpcy5sb2coYE5vdCBzZXR0aW5nIHByb2dyZXNzIGZvciBhIGZpbGUgdGhhdCBoYXMgYmVlbiByZW1vdmVkOiAke2ZpbGUuaWR9YClcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG4gICAgICB0aGlzLnNldEZpbGVTdGF0ZShmaWxlLmlkLCB7XG4gICAgICAgIHByb2dyZXNzOiBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldFN0YXRlKCkuZmlsZXNbZmlsZS5pZF0ucHJvZ3Jlc3MsIHtcbiAgICAgICAgICBwb3N0cHJvY2VzczogcHJvZ3Jlc3NcbiAgICAgICAgfSlcbiAgICAgIH0pXG4gICAgfSlcblxuICAgIHRoaXMub24oJ3Bvc3Rwcm9jZXNzLWNvbXBsZXRlJywgKGZpbGUpID0+IHtcbiAgICAgIGlmICghdGhpcy5nZXRGaWxlKGZpbGUuaWQpKSB7XG4gICAgICAgIHRoaXMubG9nKGBOb3Qgc2V0dGluZyBwcm9ncmVzcyBmb3IgYSBmaWxlIHRoYXQgaGFzIGJlZW4gcmVtb3ZlZDogJHtmaWxlLmlkfWApXG4gICAgICAgIHJldHVyblxuICAgICAgfVxuICAgICAgY29uc3QgZmlsZXMgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLmdldFN0YXRlKCkuZmlsZXMpXG4gICAgICBmaWxlc1tmaWxlLmlkXSA9IE9iamVjdC5hc3NpZ24oe30sIGZpbGVzW2ZpbGUuaWRdLCB7XG4gICAgICAgIHByb2dyZXNzOiBPYmplY3QuYXNzaWduKHt9LCBmaWxlc1tmaWxlLmlkXS5wcm9ncmVzcylcbiAgICAgIH0pXG4gICAgICBkZWxldGUgZmlsZXNbZmlsZS5pZF0ucHJvZ3Jlc3MucG9zdHByb2Nlc3NcbiAgICAgIC8vIFRPRE8gc2hvdWxkIHdlIHNldCBzb21lIGtpbmQgb2YgYGZ1bGx5Q29tcGxldGVgIHByb3BlcnR5IG9uIHRoZSBmaWxlIG9iamVjdFxuICAgICAgLy8gc28gaXQncyBlYXNpZXIgdG8gc2VlIHRoYXQgdGhlIGZpbGUgaXMgdXBsb2Fk4oCmZnVsbHkgY29tcGxldGXigKZyYXRoZXIgdGhhblxuICAgICAgLy8gd2hhdCB3ZSBoYXZlIHRvIGRvIG5vdyAoYHVwbG9hZENvbXBsZXRlICYmICFwb3N0cHJvY2Vzc2ApXG5cbiAgICAgIHRoaXMuc2V0U3RhdGUoeyBmaWxlczogZmlsZXMgfSlcbiAgICB9KVxuXG4gICAgdGhpcy5vbigncmVzdG9yZWQnLCAoKSA9PiB7XG4gICAgICAvLyBGaWxlcyBtYXkgaGF2ZSBjaGFuZ2VkLS1lbnN1cmUgcHJvZ3Jlc3MgaXMgc3RpbGwgYWNjdXJhdGUuXG4gICAgICB0aGlzLl9jYWxjdWxhdGVUb3RhbFByb2dyZXNzKClcbiAgICB9KVxuXG4gICAgLy8gc2hvdyBpbmZvcm1lciBpZiBvZmZsaW5lXG4gICAgaWYgKHR5cGVvZiB3aW5kb3cgIT09ICd1bmRlZmluZWQnICYmIHdpbmRvdy5hZGRFdmVudExpc3RlbmVyKSB7XG4gICAgICB3aW5kb3cuYWRkRXZlbnRMaXN0ZW5lcignb25saW5lJywgKCkgPT4gdGhpcy51cGRhdGVPbmxpbmVTdGF0dXMoKSlcbiAgICAgIHdpbmRvdy5hZGRFdmVudExpc3RlbmVyKCdvZmZsaW5lJywgKCkgPT4gdGhpcy51cGRhdGVPbmxpbmVTdGF0dXMoKSlcbiAgICAgIHNldFRpbWVvdXQoKCkgPT4gdGhpcy51cGRhdGVPbmxpbmVTdGF0dXMoKSwgMzAwMClcbiAgICB9XG4gIH1cblxuICB1cGRhdGVPbmxpbmVTdGF0dXMgKCkge1xuICAgIGNvbnN0IG9ubGluZSA9XG4gICAgICB0eXBlb2Ygd2luZG93Lm5hdmlnYXRvci5vbkxpbmUgIT09ICd1bmRlZmluZWQnXG4gICAgICAgID8gd2luZG93Lm5hdmlnYXRvci5vbkxpbmVcbiAgICAgICAgOiB0cnVlXG4gICAgaWYgKCFvbmxpbmUpIHtcbiAgICAgIHRoaXMuZW1pdCgnaXMtb2ZmbGluZScpXG4gICAgICB0aGlzLmluZm8odGhpcy5pMThuKCdub0ludGVybmV0Q29ubmVjdGlvbicpLCAnZXJyb3InLCAwKVxuICAgICAgdGhpcy53YXNPZmZsaW5lID0gdHJ1ZVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmVtaXQoJ2lzLW9ubGluZScpXG4gICAgICBpZiAodGhpcy53YXNPZmZsaW5lKSB7XG4gICAgICAgIHRoaXMuZW1pdCgnYmFjay1vbmxpbmUnKVxuICAgICAgICB0aGlzLmluZm8odGhpcy5pMThuKCdjb25uZWN0ZWRUb0ludGVybmV0JyksICdzdWNjZXNzJywgMzAwMClcbiAgICAgICAgdGhpcy53YXNPZmZsaW5lID0gZmFsc2VcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBnZXRJRCAoKSB7XG4gICAgcmV0dXJuIHRoaXMub3B0cy5pZFxuICB9XG5cbiAgLyoqXG4gICAqIFJlZ2lzdGVycyBhIHBsdWdpbiB3aXRoIENvcmUuXG4gICAqXG4gICAqIEBwYXJhbSB7b2JqZWN0fSBQbHVnaW4gb2JqZWN0XG4gICAqIEBwYXJhbSB7b2JqZWN0fSBbb3B0c10gb2JqZWN0IHdpdGggb3B0aW9ucyB0byBiZSBwYXNzZWQgdG8gUGx1Z2luXG4gICAqIEByZXR1cm5zIHtvYmplY3R9IHNlbGYgZm9yIGNoYWluaW5nXG4gICAqL1xuICB1c2UgKFBsdWdpbiwgb3B0cykge1xuICAgIGlmICh0eXBlb2YgUGx1Z2luICE9PSAnZnVuY3Rpb24nKSB7XG4gICAgICBjb25zdCBtc2cgPSBgRXhwZWN0ZWQgYSBwbHVnaW4gY2xhc3MsIGJ1dCBnb3QgJHtQbHVnaW4gPT09IG51bGwgPyAnbnVsbCcgOiB0eXBlb2YgUGx1Z2lufS5gICtcbiAgICAgICAgJyBQbGVhc2UgdmVyaWZ5IHRoYXQgdGhlIHBsdWdpbiB3YXMgaW1wb3J0ZWQgYW5kIHNwZWxsZWQgY29ycmVjdGx5LidcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IobXNnKVxuICAgIH1cblxuICAgIC8vIEluc3RhbnRpYXRlXG4gICAgY29uc3QgcGx1Z2luID0gbmV3IFBsdWdpbih0aGlzLCBvcHRzKVxuICAgIGNvbnN0IHBsdWdpbklkID0gcGx1Z2luLmlkXG4gICAgdGhpcy5wbHVnaW5zW3BsdWdpbi50eXBlXSA9IHRoaXMucGx1Z2luc1twbHVnaW4udHlwZV0gfHwgW11cblxuICAgIGlmICghcGx1Z2luSWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignWW91ciBwbHVnaW4gbXVzdCBoYXZlIGFuIGlkJylcbiAgICB9XG5cbiAgICBpZiAoIXBsdWdpbi50eXBlKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1lvdXIgcGx1Z2luIG11c3QgaGF2ZSBhIHR5cGUnKVxuICAgIH1cblxuICAgIGNvbnN0IGV4aXN0c1BsdWdpbkFscmVhZHkgPSB0aGlzLmdldFBsdWdpbihwbHVnaW5JZClcbiAgICBpZiAoZXhpc3RzUGx1Z2luQWxyZWFkeSkge1xuICAgICAgY29uc3QgbXNnID0gYEFscmVhZHkgZm91bmQgYSBwbHVnaW4gbmFtZWQgJyR7ZXhpc3RzUGx1Z2luQWxyZWFkeS5pZH0nLiBgICtcbiAgICAgICAgYFRyaWVkIHRvIHVzZTogJyR7cGx1Z2luSWR9Jy5cXG5gICtcbiAgICAgICAgJ1VwcHkgcGx1Z2lucyBtdXN0IGhhdmUgdW5pcXVlIGBpZGAgb3B0aW9ucy4gU2VlIGh0dHBzOi8vdXBweS5pby9kb2NzL3BsdWdpbnMvI2lkLidcbiAgICAgIHRocm93IG5ldyBFcnJvcihtc2cpXG4gICAgfVxuXG4gICAgaWYgKFBsdWdpbi5WRVJTSU9OKSB7XG4gICAgICB0aGlzLmxvZyhgVXNpbmcgJHtwbHVnaW5JZH0gdiR7UGx1Z2luLlZFUlNJT059YClcbiAgICB9XG5cbiAgICB0aGlzLnBsdWdpbnNbcGx1Z2luLnR5cGVdLnB1c2gocGx1Z2luKVxuICAgIHBsdWdpbi5pbnN0YWxsKClcblxuICAgIHJldHVybiB0aGlzXG4gIH1cblxuICAvKipcbiAgICogRmluZCBvbmUgUGx1Z2luIGJ5IG5hbWUuXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBpZCBwbHVnaW4gaWRcbiAgICogQHJldHVybnMge29iamVjdHxib29sZWFufVxuICAgKi9cbiAgZ2V0UGx1Z2luIChpZCkge1xuICAgIGxldCBmb3VuZFBsdWdpbiA9IG51bGxcbiAgICB0aGlzLml0ZXJhdGVQbHVnaW5zKChwbHVnaW4pID0+IHtcbiAgICAgIGlmIChwbHVnaW4uaWQgPT09IGlkKSB7XG4gICAgICAgIGZvdW5kUGx1Z2luID0gcGx1Z2luXG4gICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIGZvdW5kUGx1Z2luXG4gIH1cblxuICAvKipcbiAgICogSXRlcmF0ZSB0aHJvdWdoIGFsbCBgdXNlYGQgcGx1Z2lucy5cbiAgICpcbiAgICogQHBhcmFtIHtGdW5jdGlvbn0gbWV0aG9kIHRoYXQgd2lsbCBiZSBydW4gb24gZWFjaCBwbHVnaW5cbiAgICovXG4gIGl0ZXJhdGVQbHVnaW5zIChtZXRob2QpIHtcbiAgICBPYmplY3Qua2V5cyh0aGlzLnBsdWdpbnMpLmZvckVhY2gocGx1Z2luVHlwZSA9PiB7XG4gICAgICB0aGlzLnBsdWdpbnNbcGx1Z2luVHlwZV0uZm9yRWFjaChtZXRob2QpXG4gICAgfSlcbiAgfVxuXG4gIC8qKlxuICAgKiBVbmluc3RhbGwgYW5kIHJlbW92ZSBhIHBsdWdpbi5cbiAgICpcbiAgICogQHBhcmFtIHtvYmplY3R9IGluc3RhbmNlIFRoZSBwbHVnaW4gaW5zdGFuY2UgdG8gcmVtb3ZlLlxuICAgKi9cbiAgcmVtb3ZlUGx1Z2luIChpbnN0YW5jZSkge1xuICAgIHRoaXMubG9nKGBSZW1vdmluZyBwbHVnaW4gJHtpbnN0YW5jZS5pZH1gKVxuICAgIHRoaXMuZW1pdCgncGx1Z2luLXJlbW92ZScsIGluc3RhbmNlKVxuXG4gICAgaWYgKGluc3RhbmNlLnVuaW5zdGFsbCkge1xuICAgICAgaW5zdGFuY2UudW5pbnN0YWxsKClcbiAgICB9XG5cbiAgICBjb25zdCBsaXN0ID0gdGhpcy5wbHVnaW5zW2luc3RhbmNlLnR5cGVdLnNsaWNlKClcbiAgICBjb25zdCBpbmRleCA9IGxpc3QuaW5kZXhPZihpbnN0YW5jZSlcbiAgICBpZiAoaW5kZXggIT09IC0xKSB7XG4gICAgICBsaXN0LnNwbGljZShpbmRleCwgMSlcbiAgICAgIHRoaXMucGx1Z2luc1tpbnN0YW5jZS50eXBlXSA9IGxpc3RcbiAgICB9XG5cbiAgICBjb25zdCB1cGRhdGVkU3RhdGUgPSB0aGlzLmdldFN0YXRlKClcbiAgICBkZWxldGUgdXBkYXRlZFN0YXRlLnBsdWdpbnNbaW5zdGFuY2UuaWRdXG4gICAgdGhpcy5zZXRTdGF0ZSh1cGRhdGVkU3RhdGUpXG4gIH1cblxuICAvKipcbiAgICogVW5pbnN0YWxsIGFsbCBwbHVnaW5zIGFuZCBjbG9zZSBkb3duIHRoaXMgVXBweSBpbnN0YW5jZS5cbiAgICovXG4gIGNsb3NlICgpIHtcbiAgICB0aGlzLmxvZyhgQ2xvc2luZyBVcHB5IGluc3RhbmNlICR7dGhpcy5vcHRzLmlkfTogcmVtb3ZpbmcgYWxsIGZpbGVzIGFuZCB1bmluc3RhbGxpbmcgcGx1Z2luc2ApXG5cbiAgICB0aGlzLnJlc2V0KClcblxuICAgIHRoaXMuX3N0b3JlVW5zdWJzY3JpYmUoKVxuXG4gICAgdGhpcy5pdGVyYXRlUGx1Z2lucygocGx1Z2luKSA9PiB7XG4gICAgICB0aGlzLnJlbW92ZVBsdWdpbihwbHVnaW4pXG4gICAgfSlcbiAgfVxuXG4gIC8qKlxuICAgKiBTZXQgaW5mbyBtZXNzYWdlIGluIGBzdGF0ZS5pbmZvYCwgc28gdGhhdCBVSSBwbHVnaW5zIGxpa2UgYEluZm9ybWVyYFxuICAgKiBjYW4gZGlzcGxheSB0aGUgbWVzc2FnZS5cbiAgICpcbiAgICogQHBhcmFtIHtzdHJpbmcgfCBvYmplY3R9IG1lc3NhZ2UgTWVzc2FnZSB0byBiZSBkaXNwbGF5ZWQgYnkgdGhlIGluZm9ybWVyXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBbdHlwZV1cbiAgICogQHBhcmFtIHtudW1iZXJ9IFtkdXJhdGlvbl1cbiAgICovXG5cbiAgaW5mbyAobWVzc2FnZSwgdHlwZSA9ICdpbmZvJywgZHVyYXRpb24gPSAzMDAwKSB7XG4gICAgY29uc3QgaXNDb21wbGV4TWVzc2FnZSA9IHR5cGVvZiBtZXNzYWdlID09PSAnb2JqZWN0J1xuXG4gICAgdGhpcy5zZXRTdGF0ZSh7XG4gICAgICBpbmZvOiB7XG4gICAgICAgIGlzSGlkZGVuOiBmYWxzZSxcbiAgICAgICAgdHlwZTogdHlwZSxcbiAgICAgICAgbWVzc2FnZTogaXNDb21wbGV4TWVzc2FnZSA/IG1lc3NhZ2UubWVzc2FnZSA6IG1lc3NhZ2UsXG4gICAgICAgIGRldGFpbHM6IGlzQ29tcGxleE1lc3NhZ2UgPyBtZXNzYWdlLmRldGFpbHMgOiBudWxsXG4gICAgICB9XG4gICAgfSlcblxuICAgIHRoaXMuZW1pdCgnaW5mby12aXNpYmxlJylcblxuICAgIGNsZWFyVGltZW91dCh0aGlzLmluZm9UaW1lb3V0SUQpXG4gICAgaWYgKGR1cmF0aW9uID09PSAwKSB7XG4gICAgICB0aGlzLmluZm9UaW1lb3V0SUQgPSB1bmRlZmluZWRcbiAgICAgIHJldHVyblxuICAgIH1cblxuICAgIC8vIGhpZGUgdGhlIGluZm9ybWVyIGFmdGVyIGBkdXJhdGlvbmAgbWlsbGlzZWNvbmRzXG4gICAgdGhpcy5pbmZvVGltZW91dElEID0gc2V0VGltZW91dCh0aGlzLmhpZGVJbmZvLCBkdXJhdGlvbilcbiAgfVxuXG4gIGhpZGVJbmZvICgpIHtcbiAgICBjb25zdCBuZXdJbmZvID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5nZXRTdGF0ZSgpLmluZm8sIHtcbiAgICAgIGlzSGlkZGVuOiB0cnVlXG4gICAgfSlcbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGluZm86IG5ld0luZm9cbiAgICB9KVxuICAgIHRoaXMuZW1pdCgnaW5mby1oaWRkZW4nKVxuICB9XG5cbiAgLyoqXG4gICAqIFBhc3NlcyBtZXNzYWdlcyB0byBhIGZ1bmN0aW9uLCBwcm92aWRlZCBpbiBgb3B0cy5sb2dnZXJgLlxuICAgKiBJZiBgb3B0cy5sb2dnZXI6IFVwcHkuZGVidWdMb2dnZXJgIG9yIGBvcHRzLmRlYnVnOiB0cnVlYCwgbG9ncyB0byB0aGUgYnJvd3NlciBjb25zb2xlLlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ3xvYmplY3R9IG1lc3NhZ2UgdG8gbG9nXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBbdHlwZV0gb3B0aW9uYWwgYGVycm9yYCBvciBgd2FybmluZ2BcbiAgICovXG4gIGxvZyAobWVzc2FnZSwgdHlwZSkge1xuICAgIGNvbnN0IHsgbG9nZ2VyIH0gPSB0aGlzLm9wdHNcbiAgICBzd2l0Y2ggKHR5cGUpIHtcbiAgICAgIGNhc2UgJ2Vycm9yJzogbG9nZ2VyLmVycm9yKG1lc3NhZ2UpOyBicmVha1xuICAgICAgY2FzZSAnd2FybmluZyc6IGxvZ2dlci53YXJuKG1lc3NhZ2UpOyBicmVha1xuICAgICAgZGVmYXVsdDogbG9nZ2VyLmRlYnVnKG1lc3NhZ2UpOyBicmVha1xuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBPYnNvbGV0ZSwgZXZlbnQgbGlzdGVuZXJzIGFyZSBub3cgYWRkZWQgaW4gdGhlIGNvbnN0cnVjdG9yLlxuICAgKi9cbiAgcnVuICgpIHtcbiAgICB0aGlzLmxvZygnQ2FsbGluZyBydW4oKSBpcyBubyBsb25nZXIgbmVjZXNzYXJ5LicsICd3YXJuaW5nJylcbiAgICByZXR1cm4gdGhpc1xuICB9XG5cbiAgLyoqXG4gICAqIFJlc3RvcmUgYW4gdXBsb2FkIGJ5IGl0cyBJRC5cbiAgICovXG4gIHJlc3RvcmUgKHVwbG9hZElEKSB7XG4gICAgdGhpcy5sb2coYENvcmU6IGF0dGVtcHRpbmcgdG8gcmVzdG9yZSB1cGxvYWQgXCIke3VwbG9hZElEfVwiYClcblxuICAgIGlmICghdGhpcy5nZXRTdGF0ZSgpLmN1cnJlbnRVcGxvYWRzW3VwbG9hZElEXSkge1xuICAgICAgdGhpcy5fcmVtb3ZlVXBsb2FkKHVwbG9hZElEKVxuICAgICAgcmV0dXJuIFByb21pc2UucmVqZWN0KG5ldyBFcnJvcignTm9uZXhpc3RlbnQgdXBsb2FkJykpXG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMuX3J1blVwbG9hZCh1cGxvYWRJRClcbiAgfVxuXG4gIC8qKlxuICAgKiBDcmVhdGUgYW4gdXBsb2FkIGZvciBhIGJ1bmNoIG9mIGZpbGVzLlxuICAgKlxuICAgKiBAcGFyYW0ge0FycmF5PHN0cmluZz59IGZpbGVJRHMgRmlsZSBJRHMgdG8gaW5jbHVkZSBpbiB0aGlzIHVwbG9hZC5cbiAgICogQHJldHVybnMge3N0cmluZ30gSUQgb2YgdGhpcyB1cGxvYWQuXG4gICAqL1xuICBfY3JlYXRlVXBsb2FkIChmaWxlSURzKSB7XG4gICAgY29uc3QgeyBhbGxvd05ld1VwbG9hZCwgY3VycmVudFVwbG9hZHMgfSA9IHRoaXMuZ2V0U3RhdGUoKVxuICAgIGlmICghYWxsb3dOZXdVcGxvYWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignQ2Fubm90IGNyZWF0ZSBhIG5ldyB1cGxvYWQ6IGFscmVhZHkgdXBsb2FkaW5nLicpXG4gICAgfVxuXG4gICAgY29uc3QgdXBsb2FkSUQgPSBjdWlkKClcblxuICAgIHRoaXMuZW1pdCgndXBsb2FkJywge1xuICAgICAgaWQ6IHVwbG9hZElELFxuICAgICAgZmlsZUlEczogZmlsZUlEc1xuICAgIH0pXG5cbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGFsbG93TmV3VXBsb2FkOiB0aGlzLm9wdHMuYWxsb3dNdWx0aXBsZVVwbG9hZHMgIT09IGZhbHNlLFxuXG4gICAgICBjdXJyZW50VXBsb2Fkczoge1xuICAgICAgICAuLi5jdXJyZW50VXBsb2FkcyxcbiAgICAgICAgW3VwbG9hZElEXToge1xuICAgICAgICAgIGZpbGVJRHM6IGZpbGVJRHMsXG4gICAgICAgICAgc3RlcDogMCxcbiAgICAgICAgICByZXN1bHQ6IHt9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9KVxuXG4gICAgcmV0dXJuIHVwbG9hZElEXG4gIH1cblxuICBfZ2V0VXBsb2FkICh1cGxvYWRJRCkge1xuICAgIGNvbnN0IHsgY3VycmVudFVwbG9hZHMgfSA9IHRoaXMuZ2V0U3RhdGUoKVxuXG4gICAgcmV0dXJuIGN1cnJlbnRVcGxvYWRzW3VwbG9hZElEXVxuICB9XG5cbiAgLyoqXG4gICAqIEFkZCBkYXRhIHRvIGFuIHVwbG9hZCdzIHJlc3VsdCBvYmplY3QuXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSB1cGxvYWRJRCBUaGUgSUQgb2YgdGhlIHVwbG9hZC5cbiAgICogQHBhcmFtIHtvYmplY3R9IGRhdGEgRGF0YSBwcm9wZXJ0aWVzIHRvIGFkZCB0byB0aGUgcmVzdWx0IG9iamVjdC5cbiAgICovXG4gIGFkZFJlc3VsdERhdGEgKHVwbG9hZElELCBkYXRhKSB7XG4gICAgaWYgKCF0aGlzLl9nZXRVcGxvYWQodXBsb2FkSUQpKSB7XG4gICAgICB0aGlzLmxvZyhgTm90IHNldHRpbmcgcmVzdWx0IGZvciBhbiB1cGxvYWQgdGhhdCBoYXMgYmVlbiByZW1vdmVkOiAke3VwbG9hZElEfWApXG4gICAgICByZXR1cm5cbiAgICB9XG4gICAgY29uc3QgY3VycmVudFVwbG9hZHMgPSB0aGlzLmdldFN0YXRlKCkuY3VycmVudFVwbG9hZHNcbiAgICBjb25zdCBjdXJyZW50VXBsb2FkID0gT2JqZWN0LmFzc2lnbih7fSwgY3VycmVudFVwbG9hZHNbdXBsb2FkSURdLCB7XG4gICAgICByZXN1bHQ6IE9iamVjdC5hc3NpZ24oe30sIGN1cnJlbnRVcGxvYWRzW3VwbG9hZElEXS5yZXN1bHQsIGRhdGEpXG4gICAgfSlcbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGN1cnJlbnRVcGxvYWRzOiBPYmplY3QuYXNzaWduKHt9LCBjdXJyZW50VXBsb2Fkcywge1xuICAgICAgICBbdXBsb2FkSURdOiBjdXJyZW50VXBsb2FkXG4gICAgICB9KVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogUmVtb3ZlIGFuIHVwbG9hZCwgZWcuIGlmIGl0IGhhcyBiZWVuIGNhbmNlbGVkIG9yIGNvbXBsZXRlZC5cbiAgICpcbiAgICogQHBhcmFtIHtzdHJpbmd9IHVwbG9hZElEIFRoZSBJRCBvZiB0aGUgdXBsb2FkLlxuICAgKi9cbiAgX3JlbW92ZVVwbG9hZCAodXBsb2FkSUQpIHtcbiAgICBjb25zdCBjdXJyZW50VXBsb2FkcyA9IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuZ2V0U3RhdGUoKS5jdXJyZW50VXBsb2FkcylcbiAgICBkZWxldGUgY3VycmVudFVwbG9hZHNbdXBsb2FkSURdXG5cbiAgICB0aGlzLnNldFN0YXRlKHtcbiAgICAgIGN1cnJlbnRVcGxvYWRzOiBjdXJyZW50VXBsb2Fkc1xuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogUnVuIGFuIHVwbG9hZC4gVGhpcyBwaWNrcyB1cCB3aGVyZSBpdCBsZWZ0IG9mZiBpbiBjYXNlIHRoZSB1cGxvYWQgaXMgYmVpbmcgcmVzdG9yZWQuXG4gICAqXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBfcnVuVXBsb2FkICh1cGxvYWRJRCkge1xuICAgIGNvbnN0IHVwbG9hZERhdGEgPSB0aGlzLmdldFN0YXRlKCkuY3VycmVudFVwbG9hZHNbdXBsb2FkSURdXG4gICAgY29uc3QgcmVzdG9yZVN0ZXAgPSB1cGxvYWREYXRhLnN0ZXBcblxuICAgIGNvbnN0IHN0ZXBzID0gW1xuICAgICAgLi4udGhpcy5wcmVQcm9jZXNzb3JzLFxuICAgICAgLi4udGhpcy51cGxvYWRlcnMsXG4gICAgICAuLi50aGlzLnBvc3RQcm9jZXNzb3JzXG4gICAgXVxuICAgIGxldCBsYXN0U3RlcCA9IFByb21pc2UucmVzb2x2ZSgpXG4gICAgc3RlcHMuZm9yRWFjaCgoZm4sIHN0ZXApID0+IHtcbiAgICAgIC8vIFNraXAgdGhpcyBzdGVwIGlmIHdlIGFyZSByZXN0b3JpbmcgYW5kIGhhdmUgYWxyZWFkeSBjb21wbGV0ZWQgdGhpcyBzdGVwIGJlZm9yZS5cbiAgICAgIGlmIChzdGVwIDwgcmVzdG9yZVN0ZXApIHtcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG5cbiAgICAgIGxhc3RTdGVwID0gbGFzdFN0ZXAudGhlbigoKSA9PiB7XG4gICAgICAgIGNvbnN0IHsgY3VycmVudFVwbG9hZHMgfSA9IHRoaXMuZ2V0U3RhdGUoKVxuICAgICAgICBjb25zdCBjdXJyZW50VXBsb2FkID0gY3VycmVudFVwbG9hZHNbdXBsb2FkSURdXG4gICAgICAgIGlmICghY3VycmVudFVwbG9hZCkge1xuICAgICAgICAgIHJldHVyblxuICAgICAgICB9XG5cbiAgICAgICAgY29uc3QgdXBkYXRlZFVwbG9hZCA9IE9iamVjdC5hc3NpZ24oe30sIGN1cnJlbnRVcGxvYWQsIHtcbiAgICAgICAgICBzdGVwOiBzdGVwXG4gICAgICAgIH0pXG4gICAgICAgIHRoaXMuc2V0U3RhdGUoe1xuICAgICAgICAgIGN1cnJlbnRVcGxvYWRzOiBPYmplY3QuYXNzaWduKHt9LCBjdXJyZW50VXBsb2Fkcywge1xuICAgICAgICAgICAgW3VwbG9hZElEXTogdXBkYXRlZFVwbG9hZFxuICAgICAgICAgIH0pXG4gICAgICAgIH0pXG5cbiAgICAgICAgLy8gVE9ETyBnaXZlIHRoaXMgdGhlIGB1cGRhdGVkVXBsb2FkYCBvYmplY3QgYXMgaXRzIG9ubHkgcGFyYW1ldGVyIG1heWJlP1xuICAgICAgICAvLyBPdGhlcndpc2Ugd2hlbiBtb3JlIG1ldGFkYXRhIG1heSBiZSBhZGRlZCB0byB0aGUgdXBsb2FkIHRoaXMgd291bGQga2VlcCBnZXR0aW5nIG1vcmUgcGFyYW1ldGVyc1xuICAgICAgICByZXR1cm4gZm4odXBkYXRlZFVwbG9hZC5maWxlSURzLCB1cGxvYWRJRClcbiAgICAgIH0pLnRoZW4oKHJlc3VsdCkgPT4ge1xuICAgICAgICByZXR1cm4gbnVsbFxuICAgICAgfSlcbiAgICB9KVxuXG4gICAgLy8gTm90IHJldHVybmluZyB0aGUgYGNhdGNoYGVkIHByb21pc2UsIGJlY2F1c2Ugd2Ugc3RpbGwgd2FudCB0byByZXR1cm4gYSByZWplY3RlZFxuICAgIC8vIHByb21pc2UgZnJvbSB0aGlzIG1ldGhvZCBpZiB0aGUgdXBsb2FkIGZhaWxlZC5cbiAgICBsYXN0U3RlcC5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICB0aGlzLmVtaXQoJ2Vycm9yJywgZXJyLCB1cGxvYWRJRClcbiAgICAgIHRoaXMuX3JlbW92ZVVwbG9hZCh1cGxvYWRJRClcbiAgICB9KVxuXG4gICAgcmV0dXJuIGxhc3RTdGVwLnRoZW4oKCkgPT4ge1xuICAgICAgLy8gU2V0IHJlc3VsdCBkYXRhLlxuICAgICAgY29uc3QgeyBjdXJyZW50VXBsb2FkcyB9ID0gdGhpcy5nZXRTdGF0ZSgpXG4gICAgICBjb25zdCBjdXJyZW50VXBsb2FkID0gY3VycmVudFVwbG9hZHNbdXBsb2FkSURdXG4gICAgICBpZiAoIWN1cnJlbnRVcGxvYWQpIHtcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG5cbiAgICAgIGNvbnN0IGZpbGVzID0gY3VycmVudFVwbG9hZC5maWxlSURzXG4gICAgICAgIC5tYXAoKGZpbGVJRCkgPT4gdGhpcy5nZXRGaWxlKGZpbGVJRCkpXG4gICAgICBjb25zdCBzdWNjZXNzZnVsID0gZmlsZXMuZmlsdGVyKChmaWxlKSA9PiAhZmlsZS5lcnJvcilcbiAgICAgIGNvbnN0IGZhaWxlZCA9IGZpbGVzLmZpbHRlcigoZmlsZSkgPT4gZmlsZS5lcnJvcilcbiAgICAgIHRoaXMuYWRkUmVzdWx0RGF0YSh1cGxvYWRJRCwgeyBzdWNjZXNzZnVsLCBmYWlsZWQsIHVwbG9hZElEIH0pXG4gICAgfSkudGhlbigoKSA9PiB7XG4gICAgICAvLyBFbWl0IGNvbXBsZXRpb24gZXZlbnRzLlxuICAgICAgLy8gVGhpcyBpcyBpbiBhIHNlcGFyYXRlIGZ1bmN0aW9uIHNvIHRoYXQgdGhlIGBjdXJyZW50VXBsb2Fkc2AgdmFyaWFibGVcbiAgICAgIC8vIGFsd2F5cyByZWZlcnMgdG8gdGhlIGxhdGVzdCBzdGF0ZS4gSW4gdGhlIGhhbmRsZXIgcmlnaHQgYWJvdmUgaXQgcmVmZXJzXG4gICAgICAvLyB0byBhbiBvdXRkYXRlZCBvYmplY3Qgd2l0aG91dCB0aGUgYC5yZXN1bHRgIHByb3BlcnR5LlxuICAgICAgY29uc3QgeyBjdXJyZW50VXBsb2FkcyB9ID0gdGhpcy5nZXRTdGF0ZSgpXG4gICAgICBpZiAoIWN1cnJlbnRVcGxvYWRzW3VwbG9hZElEXSkge1xuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIGNvbnN0IGN1cnJlbnRVcGxvYWQgPSBjdXJyZW50VXBsb2Fkc1t1cGxvYWRJRF1cbiAgICAgIGNvbnN0IHJlc3VsdCA9IGN1cnJlbnRVcGxvYWQucmVzdWx0XG4gICAgICB0aGlzLmVtaXQoJ2NvbXBsZXRlJywgcmVzdWx0KVxuXG4gICAgICB0aGlzLl9yZW1vdmVVcGxvYWQodXBsb2FkSUQpXG5cbiAgICAgIHJldHVybiByZXN1bHRcbiAgICB9KS50aGVuKChyZXN1bHQpID0+IHtcbiAgICAgIGlmIChyZXN1bHQgPT0gbnVsbCkge1xuICAgICAgICB0aGlzLmxvZyhgTm90IHNldHRpbmcgcmVzdWx0IGZvciBhbiB1cGxvYWQgdGhhdCBoYXMgYmVlbiByZW1vdmVkOiAke3VwbG9hZElEfWApXG4gICAgICB9XG4gICAgICByZXR1cm4gcmVzdWx0XG4gICAgfSlcbiAgfVxuXG4gIC8qKlxuICAgKiBTdGFydCBhbiB1cGxvYWQgZm9yIGFsbCB0aGUgZmlsZXMgdGhhdCBhcmUgbm90IGN1cnJlbnRseSBiZWluZyB1cGxvYWRlZC5cbiAgICpcbiAgICogQHJldHVybnMge1Byb21pc2V9XG4gICAqL1xuICB1cGxvYWQgKCkge1xuICAgIGlmICghdGhpcy5wbHVnaW5zLnVwbG9hZGVyKSB7XG4gICAgICB0aGlzLmxvZygnTm8gdXBsb2FkZXIgdHlwZSBwbHVnaW5zIGFyZSB1c2VkJywgJ3dhcm5pbmcnKVxuICAgIH1cblxuICAgIGxldCBmaWxlcyA9IHRoaXMuZ2V0U3RhdGUoKS5maWxlc1xuXG4gICAgY29uc3Qgb25CZWZvcmVVcGxvYWRSZXN1bHQgPSB0aGlzLm9wdHMub25CZWZvcmVVcGxvYWQoZmlsZXMpXG5cbiAgICBpZiAob25CZWZvcmVVcGxvYWRSZXN1bHQgPT09IGZhbHNlKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZWplY3QobmV3IEVycm9yKCdOb3Qgc3RhcnRpbmcgdGhlIHVwbG9hZCBiZWNhdXNlIG9uQmVmb3JlVXBsb2FkIHJldHVybmVkIGZhbHNlJykpXG4gICAgfVxuXG4gICAgaWYgKG9uQmVmb3JlVXBsb2FkUmVzdWx0ICYmIHR5cGVvZiBvbkJlZm9yZVVwbG9hZFJlc3VsdCA9PT0gJ29iamVjdCcpIHtcbiAgICAgIGZpbGVzID0gb25CZWZvcmVVcGxvYWRSZXN1bHRcbiAgICB9XG5cbiAgICByZXR1cm4gUHJvbWlzZS5yZXNvbHZlKClcbiAgICAgIC50aGVuKCgpID0+IHRoaXMuX2NoZWNrTWluTnVtYmVyT2ZGaWxlcyhmaWxlcykpXG4gICAgICAudGhlbigoKSA9PiB7XG4gICAgICAgIGNvbnN0IHsgY3VycmVudFVwbG9hZHMgfSA9IHRoaXMuZ2V0U3RhdGUoKVxuICAgICAgICAvLyBnZXQgYSBsaXN0IG9mIGZpbGVzIHRoYXQgYXJlIGN1cnJlbnRseSBhc3NpZ25lZCB0byB1cGxvYWRzXG4gICAgICAgIGNvbnN0IGN1cnJlbnRseVVwbG9hZGluZ0ZpbGVzID0gT2JqZWN0LmtleXMoY3VycmVudFVwbG9hZHMpLnJlZHVjZSgocHJldiwgY3VycikgPT4gcHJldi5jb25jYXQoY3VycmVudFVwbG9hZHNbY3Vycl0uZmlsZUlEcyksIFtdKVxuXG4gICAgICAgIGNvbnN0IHdhaXRpbmdGaWxlSURzID0gW11cbiAgICAgICAgT2JqZWN0LmtleXMoZmlsZXMpLmZvckVhY2goKGZpbGVJRCkgPT4ge1xuICAgICAgICAgIGNvbnN0IGZpbGUgPSB0aGlzLmdldEZpbGUoZmlsZUlEKVxuICAgICAgICAgIC8vIGlmIHRoZSBmaWxlIGhhc24ndCBzdGFydGVkIHVwbG9hZGluZyBhbmQgaGFzbid0IGFscmVhZHkgYmVlbiBhc3NpZ25lZCB0byBhbiB1cGxvYWQuLlxuICAgICAgICAgIGlmICgoIWZpbGUucHJvZ3Jlc3MudXBsb2FkU3RhcnRlZCkgJiYgKGN1cnJlbnRseVVwbG9hZGluZ0ZpbGVzLmluZGV4T2YoZmlsZUlEKSA9PT0gLTEpKSB7XG4gICAgICAgICAgICB3YWl0aW5nRmlsZUlEcy5wdXNoKGZpbGUuaWQpXG4gICAgICAgICAgfVxuICAgICAgICB9KVxuXG4gICAgICAgIGNvbnN0IHVwbG9hZElEID0gdGhpcy5fY3JlYXRlVXBsb2FkKHdhaXRpbmdGaWxlSURzKVxuICAgICAgICByZXR1cm4gdGhpcy5fcnVuVXBsb2FkKHVwbG9hZElEKVxuICAgICAgfSlcbiAgICAgIC5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICAgIHRoaXMuX3Nob3dPckxvZ0Vycm9yQW5kVGhyb3coZXJyKVxuICAgICAgfSlcbiAgfVxufVxuXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIChvcHRzKSB7XG4gIHJldHVybiBuZXcgVXBweShvcHRzKVxufVxuXG4vLyBFeHBvc2UgY2xhc3MgY29uc3RydWN0b3IuXG5tb2R1bGUuZXhwb3J0cy5VcHB5ID0gVXBweVxubW9kdWxlLmV4cG9ydHMuUGx1Z2luID0gUGx1Z2luXG5tb2R1bGUuZXhwb3J0cy5kZWJ1Z0xvZ2dlciA9IGRlYnVnTG9nZ2VyXG4iLCJjb25zdCBnZXRUaW1lU3RhbXAgPSByZXF1aXJlKCdAdXBweS91dGlscy9saWIvZ2V0VGltZVN0YW1wJylcblxuLy8gU3dhbGxvdyBsb2dzLCBkZWZhdWx0IGlmIGxvZ2dlciBpcyBub3Qgc2V0IG9yIGRlYnVnOiBmYWxzZVxuY29uc3QgbnVsbExvZ2dlciA9IHtcbiAgZGVidWc6ICguLi5hcmdzKSA9PiB7fSxcbiAgd2FybjogKC4uLmFyZ3MpID0+IHt9LFxuICBlcnJvcjogKC4uLmFyZ3MpID0+IHt9XG59XG5cbi8vIFByaW50IGxvZ3MgdG8gY29uc29sZSB3aXRoIG5hbWVzcGFjZSArIHRpbWVzdGFtcCxcbi8vIHNldCBieSBsb2dnZXI6IFVwcHkuZGVidWdMb2dnZXIgb3IgZGVidWc6IHRydWVcbmNvbnN0IGRlYnVnTG9nZ2VyID0ge1xuICBkZWJ1ZzogKC4uLmFyZ3MpID0+IHtcbiAgICAvLyBJRSAxMCBkb2VzbuKAmXQgc3VwcG9ydCBjb25zb2xlLmRlYnVnXG4gICAgY29uc3QgZGVidWcgPSBjb25zb2xlLmRlYnVnIHx8IGNvbnNvbGUubG9nXG4gICAgZGVidWcuY2FsbChjb25zb2xlLCBgW1VwcHldIFske2dldFRpbWVTdGFtcCgpfV1gLCAuLi5hcmdzKVxuICB9LFxuICB3YXJuOiAoLi4uYXJncykgPT4gY29uc29sZS53YXJuKGBbVXBweV0gWyR7Z2V0VGltZVN0YW1wKCl9XWAsIC4uLmFyZ3MpLFxuICBlcnJvcjogKC4uLmFyZ3MpID0+IGNvbnNvbGUuZXJyb3IoYFtVcHB5XSBbJHtnZXRUaW1lU3RhbXAoKX1dYCwgLi4uYXJncylcbn1cblxubW9kdWxlLmV4cG9ydHMgPSB7XG4gIG51bGxMb2dnZXIsXG4gIGRlYnVnTG9nZ2VyXG59XG4iLCIvLyBFZGdlIDE1LnggZG9lcyBub3QgZmlyZSAncHJvZ3Jlc3MnIGV2ZW50cyBvbiB1cGxvYWRzLlxuLy8gU2VlIGh0dHBzOi8vZ2l0aHViLmNvbS90cmFuc2xvYWRpdC91cHB5L2lzc3Vlcy85NDVcbi8vIEFuZCBodHRwczovL2RldmVsb3Blci5taWNyb3NvZnQuY29tL2VuLXVzL21pY3Jvc29mdC1lZGdlL3BsYXRmb3JtL2lzc3Vlcy8xMjIyNDUxMC9cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gc3VwcG9ydHNVcGxvYWRQcm9ncmVzcyAodXNlckFnZW50KSB7XG4gIC8vIEFsbG93IHBhc3NpbmcgaW4gdXNlckFnZW50IGZvciB0ZXN0c1xuICBpZiAodXNlckFnZW50ID09IG51bGwpIHtcbiAgICB1c2VyQWdlbnQgPSB0eXBlb2YgbmF2aWdhdG9yICE9PSAndW5kZWZpbmVkJyA/IG5hdmlnYXRvci51c2VyQWdlbnQgOiBudWxsXG4gIH1cbiAgLy8gQXNzdW1lIGl0IHdvcmtzIGJlY2F1c2UgYmFzaWNhbGx5IGV2ZXJ5dGhpbmcgc3VwcG9ydHMgcHJvZ3Jlc3MgZXZlbnRzLlxuICBpZiAoIXVzZXJBZ2VudCkgcmV0dXJuIHRydWVcblxuICBjb25zdCBtID0gL0VkZ2VcXC8oXFxkK1xcLlxcZCspLy5leGVjKHVzZXJBZ2VudClcbiAgaWYgKCFtKSByZXR1cm4gdHJ1ZVxuXG4gIGNvbnN0IGVkZ2VWZXJzaW9uID0gbVsxXVxuICBsZXQgW21ham9yLCBtaW5vcl0gPSBlZGdlVmVyc2lvbi5zcGxpdCgnLicpXG4gIG1ham9yID0gcGFyc2VJbnQobWFqb3IsIDEwKVxuICBtaW5vciA9IHBhcnNlSW50KG1pbm9yLCAxMClcblxuICAvLyBXb3JrZWQgYmVmb3JlOlxuICAvLyBFZGdlIDQwLjE1MDYzLjAuMFxuICAvLyBNaWNyb3NvZnQgRWRnZUhUTUwgMTUuMTUwNjNcbiAgaWYgKG1ham9yIDwgMTUgfHwgKG1ham9yID09PSAxNSAmJiBtaW5vciA8IDE1MDYzKSkge1xuICAgIHJldHVybiB0cnVlXG4gIH1cblxuICAvLyBGaXhlZCBpbjpcbiAgLy8gTWljcm9zb2Z0IEVkZ2VIVE1MIDE4LjE4MjE4XG4gIGlmIChtYWpvciA+IDE4IHx8IChtYWpvciA9PT0gMTggJiYgbWlub3IgPj0gMTgyMTgpKSB7XG4gICAgcmV0dXJuIHRydWVcbiAgfVxuXG4gIC8vIG90aGVyIHZlcnNpb25zIGRvbid0IHdvcmsuXG4gIHJldHVybiBmYWxzZVxufVxuIiwibW9kdWxlLmV4cG9ydHM9e1xuICBcIm5hbWVcIjogXCJAdXBweS9kcmFnLWRyb3BcIixcbiAgXCJkZXNjcmlwdGlvblwiOiBcIkRyb3BwYWJsZSB6b25lIFVJIGZvciBVcHB5LiBEcmFnIGFuZCBkcm9wIGZpbGVzIGludG8gaXQgdG8gdXBsb2FkLlwiLFxuICBcInZlcnNpb25cIjogXCIxLjMuMVwiLFxuICBcImxpY2Vuc2VcIjogXCJNSVRcIixcbiAgXCJtYWluXCI6IFwibGliL2luZGV4LmpzXCIsXG4gIFwic3R5bGVcIjogXCJkaXN0L3N0eWxlLm1pbi5jc3NcIixcbiAgXCJ0eXBlc1wiOiBcInR5cGVzL2luZGV4LmQudHNcIixcbiAgXCJrZXl3b3Jkc1wiOiBbXG4gICAgXCJmaWxlIHVwbG9hZGVyXCIsXG4gICAgXCJ1cHB5XCIsXG4gICAgXCJ1cHB5LXBsdWdpblwiLFxuICAgIFwiZHJhZy1kcm9wXCIsXG4gICAgXCJkcmFnXCIsXG4gICAgXCJkcm9wXCIsXG4gICAgXCJkcm9wem9uZVwiLFxuICAgIFwidXBsb2FkXCJcbiAgXSxcbiAgXCJob21lcGFnZVwiOiBcImh0dHBzOi8vdXBweS5pb1wiLFxuICBcImJ1Z3NcIjoge1xuICAgIFwidXJsXCI6IFwiaHR0cHM6Ly9naXRodWIuY29tL3RyYW5zbG9hZGl0L3VwcHkvaXNzdWVzXCJcbiAgfSxcbiAgXCJyZXBvc2l0b3J5XCI6IHtcbiAgICBcInR5cGVcIjogXCJnaXRcIixcbiAgICBcInVybFwiOiBcImdpdCtodHRwczovL2dpdGh1Yi5jb20vdHJhbnNsb2FkaXQvdXBweS5naXRcIlxuICB9LFxuICBcImRlcGVuZGVuY2llc1wiOiB7XG4gICAgXCJAdXBweS91dGlsc1wiOiBcImZpbGU6Li4vdXRpbHNcIixcbiAgICBcInByZWFjdFwiOiBcIjguMi45XCJcbiAgfSxcbiAgXCJwZWVyRGVwZW5kZW5jaWVzXCI6IHtcbiAgICBcIkB1cHB5L2NvcmVcIjogXCJeMS4wLjBcIlxuICB9XG59XG4iLCJjb25zdCB7IFBsdWdpbiB9ID0gcmVxdWlyZSgnQHVwcHkvY29yZScpXG5jb25zdCBUcmFuc2xhdG9yID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL1RyYW5zbGF0b3InKVxuY29uc3QgdG9BcnJheSA9IHJlcXVpcmUoJ0B1cHB5L3V0aWxzL2xpYi90b0FycmF5JylcbmNvbnN0IGlzRHJhZ0Ryb3BTdXBwb3J0ZWQgPSByZXF1aXJlKCdAdXBweS91dGlscy9saWIvaXNEcmFnRHJvcFN1cHBvcnRlZCcpXG5jb25zdCBnZXREcm9wcGVkRmlsZXMgPSByZXF1aXJlKCdAdXBweS91dGlscy9saWIvZ2V0RHJvcHBlZEZpbGVzJylcbmNvbnN0IHsgaCB9ID0gcmVxdWlyZSgncHJlYWN0JylcblxuLyoqXG4gKiBEcmFnICYgRHJvcCBwbHVnaW5cbiAqXG4gKi9cbm1vZHVsZS5leHBvcnRzID0gY2xhc3MgRHJhZ0Ryb3AgZXh0ZW5kcyBQbHVnaW4ge1xuICBzdGF0aWMgVkVSU0lPTiA9IHJlcXVpcmUoJy4uL3BhY2thZ2UuanNvbicpLnZlcnNpb25cblxuICBjb25zdHJ1Y3RvciAodXBweSwgb3B0cykge1xuICAgIHN1cGVyKHVwcHksIG9wdHMpXG4gICAgdGhpcy50eXBlID0gJ2FjcXVpcmVyJ1xuICAgIHRoaXMuaWQgPSB0aGlzLm9wdHMuaWQgfHwgJ0RyYWdEcm9wJ1xuICAgIHRoaXMudGl0bGUgPSAnRHJhZyAmIERyb3AnXG5cbiAgICB0aGlzLmRlZmF1bHRMb2NhbGUgPSB7XG4gICAgICBzdHJpbmdzOiB7XG4gICAgICAgIGRyb3BIZXJlT3I6ICdEcm9wIGZpbGVzIGhlcmUgb3IgJXticm93c2V9JyxcbiAgICAgICAgYnJvd3NlOiAnYnJvd3NlJ1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIERlZmF1bHQgb3B0aW9uc1xuICAgIGNvbnN0IGRlZmF1bHRPcHRzID0ge1xuICAgICAgdGFyZ2V0OiBudWxsLFxuICAgICAgaW5wdXROYW1lOiAnZmlsZXNbXScsXG4gICAgICB3aWR0aDogJzEwMCUnLFxuICAgICAgaGVpZ2h0OiAnMTAwJScsXG4gICAgICBub3RlOiBudWxsXG4gICAgfVxuXG4gICAgLy8gTWVyZ2UgZGVmYXVsdCBvcHRpb25zIHdpdGggdGhlIG9uZXMgc2V0IGJ5IHVzZXJcbiAgICB0aGlzLm9wdHMgPSBPYmplY3QuYXNzaWduKHt9LCBkZWZhdWx0T3B0cywgb3B0cylcblxuICAgIC8vIENoZWNrIGZvciBicm93c2VyIGRyYWdEcm9wIHN1cHBvcnRcbiAgICB0aGlzLmlzRHJhZ0Ryb3BTdXBwb3J0ZWQgPSBpc0RyYWdEcm9wU3VwcG9ydGVkKClcbiAgICB0aGlzLnJlbW92ZURyYWdPdmVyQ2xhc3NUaW1lb3V0ID0gbnVsbFxuXG4gICAgLy8gaTE4blxuICAgIHRoaXMudHJhbnNsYXRvciA9IG5ldyBUcmFuc2xhdG9yKFt0aGlzLmRlZmF1bHRMb2NhbGUsIHRoaXMudXBweS5sb2NhbGUsIHRoaXMub3B0cy5sb2NhbGVdKVxuICAgIHRoaXMuaTE4biA9IHRoaXMudHJhbnNsYXRvci50cmFuc2xhdGUuYmluZCh0aGlzLnRyYW5zbGF0b3IpXG4gICAgdGhpcy5pMThuQXJyYXkgPSB0aGlzLnRyYW5zbGF0b3IudHJhbnNsYXRlQXJyYXkuYmluZCh0aGlzLnRyYW5zbGF0b3IpXG5cbiAgICAvLyBCaW5kIGB0aGlzYCB0byBjbGFzcyBtZXRob2RzXG4gICAgdGhpcy5oYW5kbGVJbnB1dENoYW5nZSA9IHRoaXMuaGFuZGxlSW5wdXRDaGFuZ2UuYmluZCh0aGlzKVxuICAgIHRoaXMuaGFuZGxlRHJhZ092ZXIgPSB0aGlzLmhhbmRsZURyYWdPdmVyLmJpbmQodGhpcylcbiAgICB0aGlzLmhhbmRsZURyYWdMZWF2ZSA9IHRoaXMuaGFuZGxlRHJhZ0xlYXZlLmJpbmQodGhpcylcbiAgICB0aGlzLmhhbmRsZURyb3AgPSB0aGlzLmhhbmRsZURyb3AuYmluZCh0aGlzKVxuICAgIHRoaXMuYWRkRmlsZSA9IHRoaXMuYWRkRmlsZS5iaW5kKHRoaXMpXG4gICAgdGhpcy5yZW5kZXIgPSB0aGlzLnJlbmRlci5iaW5kKHRoaXMpXG4gIH1cblxuICBhZGRGaWxlIChmaWxlKSB7XG4gICAgdHJ5IHtcbiAgICAgIHRoaXMudXBweS5hZGRGaWxlKHtcbiAgICAgICAgc291cmNlOiB0aGlzLmlkLFxuICAgICAgICBuYW1lOiBmaWxlLm5hbWUsXG4gICAgICAgIHR5cGU6IGZpbGUudHlwZSxcbiAgICAgICAgZGF0YTogZmlsZSxcbiAgICAgICAgbWV0YToge1xuICAgICAgICAgIHJlbGF0aXZlUGF0aDogZmlsZS5yZWxhdGl2ZVBhdGggfHwgbnVsbFxuICAgICAgICB9XG4gICAgICB9KVxuICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgaWYgKCFlcnIuaXNSZXN0cmljdGlvbikge1xuICAgICAgICB0aGlzLnVwcHkubG9nKGVycilcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBoYW5kbGVJbnB1dENoYW5nZSAoZXZlbnQpIHtcbiAgICB0aGlzLnVwcHkubG9nKCdbRHJhZ0Ryb3BdIEZpbGVzIHNlbGVjdGVkIHRocm91Z2ggaW5wdXQnKVxuICAgIGNvbnN0IGZpbGVzID0gdG9BcnJheShldmVudC50YXJnZXQuZmlsZXMpXG4gICAgZmlsZXMuZm9yRWFjaCh0aGlzLmFkZEZpbGUpXG5cbiAgICAvLyBXZSBjbGVhciB0aGUgaW5wdXQgYWZ0ZXIgYSBmaWxlIGlzIHNlbGVjdGVkLCBiZWNhdXNlIG90aGVyd2lzZVxuICAgIC8vIGNoYW5nZSBldmVudCBpcyBub3QgZmlyZWQgaW4gQ2hyb21lIGFuZCBTYWZhcmkgd2hlbiBhIGZpbGVcbiAgICAvLyB3aXRoIHRoZSBzYW1lIG5hbWUgaXMgc2VsZWN0ZWQuXG4gICAgLy8gX19fV2h5IG5vdCB1c2UgdmFsdWU9XCJcIiBvbiA8aW5wdXQvPiBpbnN0ZWFkP1xuICAgIC8vICAgIEJlY2F1c2UgaWYgd2UgdXNlIHRoYXQgbWV0aG9kIG9mIGNsZWFyaW5nIHRoZSBpbnB1dCxcbiAgICAvLyAgICBDaHJvbWUgd2lsbCBub3QgdHJpZ2dlciBjaGFuZ2UgaWYgd2UgZHJvcCB0aGUgc2FtZSBmaWxlIHR3aWNlIChJc3N1ZSAjNzY4KS5cbiAgICBldmVudC50YXJnZXQudmFsdWUgPSBudWxsXG4gIH1cblxuICBoYW5kbGVEcm9wIChldmVudCwgZHJvcENhdGVnb3J5KSB7XG4gICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxuICAgIGV2ZW50LnN0b3BQcm9wYWdhdGlvbigpXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMucmVtb3ZlRHJhZ092ZXJDbGFzc1RpbWVvdXQpXG4gICAgLy8gMS4gQWRkIGEgc21hbGwgKCspIGljb24gb24gZHJvcFxuICAgIGV2ZW50LmRhdGFUcmFuc2Zlci5kcm9wRWZmZWN0ID0gJ2NvcHknXG5cbiAgICAvLyAyLiBSZW1vdmUgZHJhZ292ZXIgY2xhc3NcbiAgICB0aGlzLnNldFBsdWdpblN0YXRlKHsgaXNEcmFnZ2luZ092ZXI6IGZhbHNlIH0pXG5cbiAgICAvLyAzLiBBZGQgYWxsIGRyb3BwZWQgZmlsZXNcbiAgICB0aGlzLnVwcHkubG9nKCdbRHJhZ0Ryb3BdIEZpbGVzIHdlcmUgZHJvcHBlZCcpXG4gICAgY29uc3QgbG9nRHJvcEVycm9yID0gKGVycm9yKSA9PiB7XG4gICAgICB0aGlzLnVwcHkubG9nKGVycm9yLCAnZXJyb3InKVxuICAgIH1cbiAgICBnZXREcm9wcGVkRmlsZXMoZXZlbnQuZGF0YVRyYW5zZmVyLCB7IGxvZ0Ryb3BFcnJvciB9KVxuICAgICAgLnRoZW4oKGZpbGVzKSA9PiB7XG4gICAgICAgIGZpbGVzLmZvckVhY2godGhpcy5hZGRGaWxlKVxuICAgICAgfSlcbiAgfVxuXG4gIGhhbmRsZURyYWdPdmVyIChldmVudCkge1xuICAgIGV2ZW50LnByZXZlbnREZWZhdWx0KClcbiAgICBldmVudC5zdG9wUHJvcGFnYXRpb24oKVxuXG4gICAgY2xlYXJUaW1lb3V0KHRoaXMucmVtb3ZlRHJhZ092ZXJDbGFzc1RpbWVvdXQpXG4gICAgdGhpcy5zZXRQbHVnaW5TdGF0ZSh7IGlzRHJhZ2dpbmdPdmVyOiB0cnVlIH0pXG4gIH1cblxuICBoYW5kbGVEcmFnTGVhdmUgKGV2ZW50KSB7XG4gICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxuICAgIGV2ZW50LnN0b3BQcm9wYWdhdGlvbigpXG5cbiAgICBjbGVhclRpbWVvdXQodGhpcy5yZW1vdmVEcmFnT3ZlckNsYXNzVGltZW91dClcbiAgICAvLyBUaW1lb3V0IGFnYWluc3QgZmxpY2tlcmluZywgdGhpcyBzb2x1dGlvbiBpcyB0YWtlbiBmcm9tIGRyYWctZHJvcCBsaWJyYXJ5LiBTb2x1dGlvbiB3aXRoICdwb2ludGVyLWV2ZW50czogbm9uZScgZGlkbid0IHdvcmsgYWNyb3NzIGJyb3dzZXJzLlxuICAgIHRoaXMucmVtb3ZlRHJhZ092ZXJDbGFzc1RpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMuc2V0UGx1Z2luU3RhdGUoeyBpc0RyYWdnaW5nT3ZlcjogZmFsc2UgfSlcbiAgICB9LCA1MClcbiAgfVxuXG4gIHJlbmRlckhpZGRlbkZpbGVJbnB1dCAoKSB7XG4gICAgY29uc3QgcmVzdHJpY3Rpb25zID0gdGhpcy51cHB5Lm9wdHMucmVzdHJpY3Rpb25zXG4gICAgcmV0dXJuIChcbiAgICAgIDxpbnB1dFxuICAgICAgICBjbGFzcz1cInVwcHktRHJhZ0Ryb3AtaW5wdXRcIlxuICAgICAgICB0eXBlPVwiZmlsZVwiXG4gICAgICAgIHRhYmluZGV4PXstMX1cbiAgICAgICAgZm9jdXNhYmxlPVwiZmFsc2VcIlxuICAgICAgICByZWY9eyhyZWYpID0+IHsgdGhpcy5maWxlSW5wdXRSZWYgPSByZWYgfX1cbiAgICAgICAgbmFtZT17dGhpcy5vcHRzLmlucHV0TmFtZX1cbiAgICAgICAgbXVsdGlwbGU9e3Jlc3RyaWN0aW9ucy5tYXhOdW1iZXJPZkZpbGVzICE9PSAxfVxuICAgICAgICBhY2NlcHQ9e3Jlc3RyaWN0aW9ucy5hbGxvd2VkRmlsZVR5cGVzfVxuICAgICAgICBvbmNoYW5nZT17dGhpcy5oYW5kbGVJbnB1dENoYW5nZX1cbiAgICAgIC8+XG4gICAgKVxuICB9XG5cbiAgcmVuZGVyQXJyb3dTdmcgKCkge1xuICAgIHJldHVybiAoXG4gICAgICA8c3ZnIGFyaWEtaGlkZGVuPVwidHJ1ZVwiIGZvY3VzYWJsZT1cImZhbHNlXCIgY2xhc3M9XCJVcHB5SWNvbiB1cHB5LURyYWdEcm9wLWFycm93XCIgd2lkdGg9XCIxNlwiIGhlaWdodD1cIjE2XCIgdmlld0JveD1cIjAgMCAxNiAxNlwiPlxuICAgICAgICA8cGF0aCBkPVwiTTExIDEwVjBINXYxMEgybDYgNiA2LTZoLTN6bTAgMFwiIGZpbGwtcnVsZT1cImV2ZW5vZGRcIiAvPlxuICAgICAgPC9zdmc+XG4gICAgKVxuICB9XG5cbiAgcmVuZGVyTGFiZWwgKCkge1xuICAgIHJldHVybiAoXG4gICAgICA8ZGl2IGNsYXNzPVwidXBweS1EcmFnRHJvcC1sYWJlbFwiPlxuICAgICAgICB7dGhpcy5pMThuQXJyYXkoJ2Ryb3BIZXJlT3InLCB7XG4gICAgICAgICAgYnJvd3NlOiA8c3BhbiBjbGFzcz1cInVwcHktRHJhZ0Ryb3AtYnJvd3NlXCI+e3RoaXMuaTE4bignYnJvd3NlJyl9PC9zcGFuPlxuICAgICAgICB9KX1cbiAgICAgIDwvZGl2PlxuICAgIClcbiAgfVxuXG4gIHJlbmRlck5vdGUgKCkge1xuICAgIHJldHVybiAoXG4gICAgICA8c3BhbiBjbGFzcz1cInVwcHktRHJhZ0Ryb3Atbm90ZVwiPnt0aGlzLm9wdHMubm90ZX08L3NwYW4+XG4gICAgKVxuICB9XG5cbiAgcmVuZGVyIChzdGF0ZSkge1xuICAgIGNvbnN0IGRyYWdEcm9wQ2xhc3MgPSBgXG4gICAgICB1cHB5LVJvb3RcbiAgICAgIHVwcHktdS1yZXNldFxuICAgICAgdXBweS1EcmFnRHJvcC1jb250YWluZXJcbiAgICAgICR7dGhpcy5pc0RyYWdEcm9wU3VwcG9ydGVkID8gJ3VwcHktRHJhZ0Ryb3AtLWlzLWRyYWdkcm9wLXN1cHBvcnRlZCcgOiAnJ31cbiAgICAgICR7dGhpcy5nZXRQbHVnaW5TdGF0ZSgpLmlzRHJhZ2dpbmdPdmVyID8gJ3VwcHktRHJhZ0Ryb3AtLWlzRHJhZ2dpbmdPdmVyJyA6ICcnfVxuICAgIGBcblxuICAgIGNvbnN0IGRyYWdEcm9wU3R5bGUgPSB7XG4gICAgICB3aWR0aDogdGhpcy5vcHRzLndpZHRoLFxuICAgICAgaGVpZ2h0OiB0aGlzLm9wdHMuaGVpZ2h0XG4gICAgfVxuXG4gICAgcmV0dXJuIChcbiAgICAgIDxidXR0b25cbiAgICAgICAgdHlwZT1cImJ1dHRvblwiXG4gICAgICAgIGNsYXNzPXtkcmFnRHJvcENsYXNzfVxuICAgICAgICBzdHlsZT17ZHJhZ0Ryb3BTdHlsZX1cbiAgICAgICAgb25DbGljaz17KCkgPT4gdGhpcy5maWxlSW5wdXRSZWYuY2xpY2soKX1cbiAgICAgICAgb25EcmFnT3Zlcj17dGhpcy5oYW5kbGVEcmFnT3Zlcn1cbiAgICAgICAgb25EcmFnTGVhdmU9e3RoaXMuaGFuZGxlRHJhZ0xlYXZlfVxuICAgICAgICBvbkRyb3A9e3RoaXMuaGFuZGxlRHJvcH1cbiAgICAgID5cbiAgICAgICAge3RoaXMucmVuZGVySGlkZGVuRmlsZUlucHV0KCl9XG4gICAgICAgIDxkaXYgY2xhc3M9XCJ1cHB5LURyYWdEcm9wLWlubmVyXCI+XG4gICAgICAgICAge3RoaXMucmVuZGVyQXJyb3dTdmcoKX1cbiAgICAgICAgICB7dGhpcy5yZW5kZXJMYWJlbCgpfVxuICAgICAgICAgIHt0aGlzLnJlbmRlck5vdGUoKX1cbiAgICAgICAgPC9kaXY+XG4gICAgICA8L2J1dHRvbj5cbiAgICApXG4gIH1cblxuICBpbnN0YWxsICgpIHtcbiAgICB0aGlzLnNldFBsdWdpblN0YXRlKHtcbiAgICAgIGlzRHJhZ2dpbmdPdmVyOiBmYWxzZVxuICAgIH0pXG4gICAgY29uc3QgdGFyZ2V0ID0gdGhpcy5vcHRzLnRhcmdldFxuICAgIGlmICh0YXJnZXQpIHtcbiAgICAgIHRoaXMubW91bnQodGFyZ2V0LCB0aGlzKVxuICAgIH1cbiAgfVxuXG4gIHVuaW5zdGFsbCAoKSB7XG4gICAgdGhpcy51bm1vdW50KClcbiAgfVxufVxuIiwibW9kdWxlLmV4cG9ydHM9e1xuICBcIm5hbWVcIjogXCJAdXBweS9wcm9ncmVzcy1iYXJcIixcbiAgXCJkZXNjcmlwdGlvblwiOiBcIkEgcHJvZ3Jlc3MgYmFyIFVJIGZvciBVcHB5XCIsXG4gIFwidmVyc2lvblwiOiBcIjEuMy4xXCIsXG4gIFwibGljZW5zZVwiOiBcIk1JVFwiLFxuICBcIm1haW5cIjogXCJsaWIvaW5kZXguanNcIixcbiAgXCJzdHlsZVwiOiBcImRpc3Qvc3R5bGUubWluLmNzc1wiLFxuICBcInR5cGVzXCI6IFwidHlwZXMvaW5kZXguZC50c1wiLFxuICBcImtleXdvcmRzXCI6IFtcbiAgICBcImZpbGUgdXBsb2FkZXJcIixcbiAgICBcInVwcHlcIixcbiAgICBcInVwcHktcGx1Z2luXCIsXG4gICAgXCJwcm9ncmVzc1wiLFxuICAgIFwicHJvZ3Jlc3MgYmFyXCIsXG4gICAgXCJ1cGxvYWQgcHJvZ3Jlc3NcIlxuICBdLFxuICBcImhvbWVwYWdlXCI6IFwiaHR0cHM6Ly91cHB5LmlvXCIsXG4gIFwiYnVnc1wiOiB7XG4gICAgXCJ1cmxcIjogXCJodHRwczovL2dpdGh1Yi5jb20vdHJhbnNsb2FkaXQvdXBweS9pc3N1ZXNcIlxuICB9LFxuICBcInJlcG9zaXRvcnlcIjoge1xuICAgIFwidHlwZVwiOiBcImdpdFwiLFxuICAgIFwidXJsXCI6IFwiZ2l0K2h0dHBzOi8vZ2l0aHViLmNvbS90cmFuc2xvYWRpdC91cHB5LmdpdFwiXG4gIH0sXG4gIFwiZGVwZW5kZW5jaWVzXCI6IHtcbiAgICBcIkB1cHB5L3V0aWxzXCI6IFwiZmlsZTouLi91dGlsc1wiLFxuICAgIFwicHJlYWN0XCI6IFwiOC4yLjlcIlxuICB9LFxuICBcInBlZXJEZXBlbmRlbmNpZXNcIjoge1xuICAgIFwiQHVwcHkvY29yZVwiOiBcIl4xLjAuMFwiXG4gIH1cbn1cbiIsImNvbnN0IHsgUGx1Z2luIH0gPSByZXF1aXJlKCdAdXBweS9jb3JlJylcbmNvbnN0IHsgaCB9ID0gcmVxdWlyZSgncHJlYWN0JylcblxuLyoqXG4gKiBQcm9ncmVzcyBiYXJcbiAqXG4gKi9cbm1vZHVsZS5leHBvcnRzID0gY2xhc3MgUHJvZ3Jlc3NCYXIgZXh0ZW5kcyBQbHVnaW4ge1xuICBzdGF0aWMgVkVSU0lPTiA9IHJlcXVpcmUoJy4uL3BhY2thZ2UuanNvbicpLnZlcnNpb25cblxuICBjb25zdHJ1Y3RvciAodXBweSwgb3B0cykge1xuICAgIHN1cGVyKHVwcHksIG9wdHMpXG4gICAgdGhpcy5pZCA9IHRoaXMub3B0cy5pZCB8fCAnUHJvZ3Jlc3NCYXInXG4gICAgdGhpcy50aXRsZSA9ICdQcm9ncmVzcyBCYXInXG4gICAgdGhpcy50eXBlID0gJ3Byb2dyZXNzaW5kaWNhdG9yJ1xuXG4gICAgLy8gc2V0IGRlZmF1bHQgb3B0aW9uc1xuICAgIGNvbnN0IGRlZmF1bHRPcHRpb25zID0ge1xuICAgICAgdGFyZ2V0OiAnYm9keScsXG4gICAgICByZXBsYWNlVGFyZ2V0Q29udGVudDogZmFsc2UsXG4gICAgICBmaXhlZDogZmFsc2UsXG4gICAgICBoaWRlQWZ0ZXJGaW5pc2g6IHRydWVcbiAgICB9XG5cbiAgICAvLyBtZXJnZSBkZWZhdWx0IG9wdGlvbnMgd2l0aCB0aGUgb25lcyBzZXQgYnkgdXNlclxuICAgIHRoaXMub3B0cyA9IE9iamVjdC5hc3NpZ24oe30sIGRlZmF1bHRPcHRpb25zLCBvcHRzKVxuXG4gICAgdGhpcy5yZW5kZXIgPSB0aGlzLnJlbmRlci5iaW5kKHRoaXMpXG4gIH1cblxuICByZW5kZXIgKHN0YXRlKSB7XG4gICAgY29uc3QgcHJvZ3Jlc3MgPSBzdGF0ZS50b3RhbFByb2dyZXNzIHx8IDBcbiAgICBjb25zdCBpc0hpZGRlbiA9IHByb2dyZXNzID09PSAxMDAgJiYgdGhpcy5vcHRzLmhpZGVBZnRlckZpbmlzaFxuICAgIHJldHVybiAoXG4gICAgICA8ZGl2XG4gICAgICAgIGNsYXNzPVwidXBweSB1cHB5LVByb2dyZXNzQmFyXCJcbiAgICAgICAgc3R5bGU9e3sgcG9zaXRpb246IHRoaXMub3B0cy5maXhlZCA/ICdmaXhlZCcgOiAnaW5pdGlhbCcgfX1cbiAgICAgICAgYXJpYS1oaWRkZW49e2lzSGlkZGVufVxuICAgICAgPlxuICAgICAgICA8ZGl2IGNsYXNzPVwidXBweS1Qcm9ncmVzc0Jhci1pbm5lclwiIHN0eWxlPXt7IHdpZHRoOiBwcm9ncmVzcyArICclJyB9fSAvPlxuICAgICAgICA8ZGl2IGNsYXNzPVwidXBweS1Qcm9ncmVzc0Jhci1wZXJjZW50YWdlXCI+e3Byb2dyZXNzfTwvZGl2PlxuICAgICAgPC9kaXY+XG4gICAgKVxuICB9XG5cbiAgaW5zdGFsbCAoKSB7XG4gICAgY29uc3QgdGFyZ2V0ID0gdGhpcy5vcHRzLnRhcmdldFxuICAgIGlmICh0YXJnZXQpIHtcbiAgICAgIHRoaXMubW91bnQodGFyZ2V0LCB0aGlzKVxuICAgIH1cbiAgfVxuXG4gIHVuaW5zdGFsbCAoKSB7XG4gICAgdGhpcy51bm1vdW50KClcbiAgfVxufVxuIiwibW9kdWxlLmV4cG9ydHM9e1xuICBcIm5hbWVcIjogXCJAdXBweS9zdG9yZS1kZWZhdWx0XCIsXG4gIFwiZGVzY3JpcHRpb25cIjogXCJUaGUgZGVmYXVsdCBzaW1wbGUgb2JqZWN0LWJhc2VkIHN0b3JlIGZvciBVcHB5LlwiLFxuICBcInZlcnNpb25cIjogXCIxLjIuMFwiLFxuICBcImxpY2Vuc2VcIjogXCJNSVRcIixcbiAgXCJtYWluXCI6IFwibGliL2luZGV4LmpzXCIsXG4gIFwidHlwZXNcIjogXCJ0eXBlcy9pbmRleC5kLnRzXCIsXG4gIFwia2V5d29yZHNcIjogW1xuICAgIFwiZmlsZSB1cGxvYWRlclwiLFxuICAgIFwidXBweVwiLFxuICAgIFwidXBweS1zdG9yZVwiXG4gIF0sXG4gIFwiaG9tZXBhZ2VcIjogXCJodHRwczovL3VwcHkuaW9cIixcbiAgXCJidWdzXCI6IHtcbiAgICBcInVybFwiOiBcImh0dHBzOi8vZ2l0aHViLmNvbS90cmFuc2xvYWRpdC91cHB5L2lzc3Vlc1wiXG4gIH0sXG4gIFwicmVwb3NpdG9yeVwiOiB7XG4gICAgXCJ0eXBlXCI6IFwiZ2l0XCIsXG4gICAgXCJ1cmxcIjogXCJnaXQraHR0cHM6Ly9naXRodWIuY29tL3RyYW5zbG9hZGl0L3VwcHkuZ2l0XCJcbiAgfVxufVxuIiwiLyoqXG4gKiBEZWZhdWx0IHN0b3JlIHRoYXQga2VlcHMgc3RhdGUgaW4gYSBzaW1wbGUgb2JqZWN0LlxuICovXG5jbGFzcyBEZWZhdWx0U3RvcmUge1xuICBzdGF0aWMgVkVSU0lPTiA9IHJlcXVpcmUoJy4uL3BhY2thZ2UuanNvbicpLnZlcnNpb25cblxuICBjb25zdHJ1Y3RvciAoKSB7XG4gICAgdGhpcy5zdGF0ZSA9IHt9XG4gICAgdGhpcy5jYWxsYmFja3MgPSBbXVxuICB9XG5cbiAgZ2V0U3RhdGUgKCkge1xuICAgIHJldHVybiB0aGlzLnN0YXRlXG4gIH1cblxuICBzZXRTdGF0ZSAocGF0Y2gpIHtcbiAgICBjb25zdCBwcmV2U3RhdGUgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLnN0YXRlKVxuICAgIGNvbnN0IG5leHRTdGF0ZSA9IE9iamVjdC5hc3NpZ24oe30sIHRoaXMuc3RhdGUsIHBhdGNoKVxuXG4gICAgdGhpcy5zdGF0ZSA9IG5leHRTdGF0ZVxuICAgIHRoaXMuX3B1Ymxpc2gocHJldlN0YXRlLCBuZXh0U3RhdGUsIHBhdGNoKVxuICB9XG5cbiAgc3Vic2NyaWJlIChsaXN0ZW5lcikge1xuICAgIHRoaXMuY2FsbGJhY2tzLnB1c2gobGlzdGVuZXIpXG4gICAgcmV0dXJuICgpID0+IHtcbiAgICAgIC8vIFJlbW92ZSB0aGUgbGlzdGVuZXIuXG4gICAgICB0aGlzLmNhbGxiYWNrcy5zcGxpY2UoXG4gICAgICAgIHRoaXMuY2FsbGJhY2tzLmluZGV4T2YobGlzdGVuZXIpLFxuICAgICAgICAxXG4gICAgICApXG4gICAgfVxuICB9XG5cbiAgX3B1Ymxpc2ggKC4uLmFyZ3MpIHtcbiAgICB0aGlzLmNhbGxiYWNrcy5mb3JFYWNoKChsaXN0ZW5lcikgPT4ge1xuICAgICAgbGlzdGVuZXIoLi4uYXJncylcbiAgICB9KVxuICB9XG59XG5cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gZGVmYXVsdFN0b3JlICgpIHtcbiAgcmV0dXJuIG5ldyBEZWZhdWx0U3RvcmUoKVxufVxuIiwiXCJ1c2Ugc3RyaWN0XCI7XG5cbk9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCBcIl9fZXNNb2R1bGVcIiwge1xuICB2YWx1ZTogdHJ1ZVxufSk7XG5leHBvcnRzLmRlZmF1bHQgPSBmaW5nZXJwcmludDtcblxudmFyIF9pc1JlYWN0TmF0aXZlID0gcmVxdWlyZShcIi4vaXNSZWFjdE5hdGl2ZVwiKTtcblxudmFyIF9pc1JlYWN0TmF0aXZlMiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX2lzUmVhY3ROYXRpdmUpO1xuXG52YXIgX2lzQ29yZG92YSA9IHJlcXVpcmUoXCIuL2lzQ29yZG92YVwiKTtcblxudmFyIF9pc0NvcmRvdmEyID0gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChfaXNDb3Jkb3ZhKTtcblxuZnVuY3Rpb24gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChvYmopIHsgcmV0dXJuIG9iaiAmJiBvYmouX19lc01vZHVsZSA/IG9iaiA6IHsgZGVmYXVsdDogb2JqIH07IH1cblxuLyoqXG4gKiBHZW5lcmF0ZSBhIGZpbmdlcnByaW50IGZvciBhIGZpbGUgd2hpY2ggd2lsbCBiZSB1c2VkIHRoZSBzdG9yZSB0aGUgZW5kcG9pbnRcbiAqXG4gKiBAcGFyYW0ge0ZpbGV9IGZpbGVcbiAqIEBwYXJhbSB7T2JqZWN0fSBvcHRpb25zXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBjYWxsYmFja1xuICovXG5mdW5jdGlvbiBmaW5nZXJwcmludChmaWxlLCBvcHRpb25zLCBjYWxsYmFjaykge1xuICBpZiAoKDAsIF9pc0NvcmRvdmEyLmRlZmF1bHQpKCkpIHtcbiAgICByZXR1cm4gY2FsbGJhY2sobmV3IEVycm9yKFwiRmluZ2VycHJpbnQgY2Fubm90IGJlIGNvbXB1dGVkIGZvciBmaWxlIGlucHV0IHR5cGVcIikpO1xuICB9XG5cbiAgaWYgKCgwLCBfaXNSZWFjdE5hdGl2ZTIuZGVmYXVsdCkoKSkge1xuICAgIHJldHVybiBjYWxsYmFjayhudWxsLCByZWFjdE5hdGl2ZUZpbmdlcnByaW50KGZpbGUsIG9wdGlvbnMpKTtcbiAgfVxuXG4gIHJldHVybiBjYWxsYmFjayhudWxsLCBbXCJ0dXMtYnJcIiwgZmlsZS5uYW1lLCBmaWxlLnR5cGUsIGZpbGUuc2l6ZSwgZmlsZS5sYXN0TW9kaWZpZWQsIG9wdGlvbnMuZW5kcG9pbnRdLmpvaW4oXCItXCIpKTtcbn1cblxuZnVuY3Rpb24gcmVhY3ROYXRpdmVGaW5nZXJwcmludChmaWxlLCBvcHRpb25zKSB7XG4gIHZhciBleGlmSGFzaCA9IGZpbGUuZXhpZiA/IGhhc2hDb2RlKEpTT04uc3RyaW5naWZ5KGZpbGUuZXhpZikpIDogXCJub2V4aWZcIjtcbiAgcmV0dXJuIFtcInR1cy1yblwiLCBmaWxlLm5hbWUgfHwgXCJub25hbWVcIiwgZmlsZS5zaXplIHx8IFwibm9zaXplXCIsIGV4aWZIYXNoLCBvcHRpb25zLmVuZHBvaW50XS5qb2luKFwiL1wiKTtcbn1cblxuZnVuY3Rpb24gaGFzaENvZGUoc3RyKSB7XG4gIC8vIGZyb20gaHR0cHM6Ly9zdGFja292ZXJmbG93LmNvbS9hLzg4MzE5MzcvMTUxNjY2XG4gIHZhciBoYXNoID0gMDtcbiAgaWYgKHN0ci5sZW5ndGggPT09IDApIHtcbiAgICByZXR1cm4gaGFzaDtcbiAgfVxuICBmb3IgKHZhciBpID0gMDsgaSA8IHN0ci5sZW5ndGg7IGkrKykge1xuICAgIHZhciBjaGFyID0gc3RyLmNoYXJDb2RlQXQoaSk7XG4gICAgaGFzaCA9IChoYXNoIDw8IDUpIC0gaGFzaCArIGNoYXI7XG4gICAgaGFzaCA9IGhhc2ggJiBoYXNoOyAvLyBDb252ZXJ0IHRvIDMyYml0IGludGVnZXJcbiAgfVxuICByZXR1cm4gaGFzaDtcbn0iLCJcInVzZSBzdHJpY3RcIjtcblxuT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFwiX19lc01vZHVsZVwiLCB7XG4gIHZhbHVlOiB0cnVlXG59KTtcbnZhciBpc0NvcmRvdmEgPSBmdW5jdGlvbiBpc0NvcmRvdmEoKSB7XG4gIHJldHVybiB0eXBlb2Ygd2luZG93ICE9IFwidW5kZWZpbmVkXCIgJiYgKHR5cGVvZiB3aW5kb3cuUGhvbmVHYXAgIT0gXCJ1bmRlZmluZWRcIiB8fCB0eXBlb2Ygd2luZG93LkNvcmRvdmEgIT0gXCJ1bmRlZmluZWRcIiB8fCB0eXBlb2Ygd2luZG93LmNvcmRvdmEgIT0gXCJ1bmRlZmluZWRcIik7XG59O1xuXG5leHBvcnRzLmRlZmF1bHQgPSBpc0NvcmRvdmE7IiwiXCJ1c2Ugc3RyaWN0XCI7XG5cbk9iamVjdC5kZWZpbmVQcm9wZXJ0eShleHBvcnRzLCBcIl9fZXNNb2R1bGVcIiwge1xuICB2YWx1ZTogdHJ1ZVxufSk7XG52YXIgaXNSZWFjdE5hdGl2ZSA9IGZ1bmN0aW9uIGlzUmVhY3ROYXRpdmUoKSB7XG4gIHJldHVybiB0eXBlb2YgbmF2aWdhdG9yICE9PSBcInVuZGVmaW5lZFwiICYmIHR5cGVvZiBuYXZpZ2F0b3IucHJvZHVjdCA9PT0gXCJzdHJpbmdcIiAmJiBuYXZpZ2F0b3IucHJvZHVjdC50b0xvd2VyQ2FzZSgpID09PSBcInJlYWN0bmF0aXZlXCI7XG59O1xuXG5leHBvcnRzLmRlZmF1bHQgPSBpc1JlYWN0TmF0aXZlOyIsIlwidXNlIHN0cmljdFwiO1xuXG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHtcbiAgdmFsdWU6IHRydWVcbn0pO1xuLyoqXG4gKiByZWFkQXNCeXRlQXJyYXkgY29udmVydHMgYSBGaWxlIG9iamVjdCB0byBhIFVpbnQ4QXJyYXkuXG4gKiBUaGlzIGZ1bmN0aW9uIGlzIG9ubHkgdXNlZCBvbiB0aGUgQXBhY2hlIENvcmRvdmEgcGxhdGZvcm0uXG4gKiBTZWUgaHR0cHM6Ly9jb3Jkb3ZhLmFwYWNoZS5vcmcvZG9jcy9lbi9sYXRlc3QvcmVmZXJlbmNlL2NvcmRvdmEtcGx1Z2luLWZpbGUvaW5kZXguaHRtbCNyZWFkLWEtZmlsZVxuICovXG5mdW5jdGlvbiByZWFkQXNCeXRlQXJyYXkoY2h1bmssIGNhbGxiYWNrKSB7XG4gIHZhciByZWFkZXIgPSBuZXcgRmlsZVJlYWRlcigpO1xuICByZWFkZXIub25sb2FkID0gZnVuY3Rpb24gKCkge1xuICAgIGNhbGxiYWNrKG51bGwsIG5ldyBVaW50OEFycmF5KHJlYWRlci5yZXN1bHQpKTtcbiAgfTtcbiAgcmVhZGVyLm9uZXJyb3IgPSBmdW5jdGlvbiAoZXJyKSB7XG4gICAgY2FsbGJhY2soZXJyKTtcbiAgfTtcbiAgcmVhZGVyLnJlYWRBc0FycmF5QnVmZmVyKGNodW5rKTtcbn1cblxuZXhwb3J0cy5kZWZhdWx0ID0gcmVhZEFzQnl0ZUFycmF5OyIsIlwidXNlIHN0cmljdFwiO1xuXG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHtcbiAgdmFsdWU6IHRydWVcbn0pO1xuZXhwb3J0cy5uZXdSZXF1ZXN0ID0gbmV3UmVxdWVzdDtcbmV4cG9ydHMucmVzb2x2ZVVybCA9IHJlc29sdmVVcmw7XG5cbnZhciBfdXJsUGFyc2UgPSByZXF1aXJlKFwidXJsLXBhcnNlXCIpO1xuXG52YXIgX3VybFBhcnNlMiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX3VybFBhcnNlKTtcblxuZnVuY3Rpb24gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChvYmopIHsgcmV0dXJuIG9iaiAmJiBvYmouX19lc01vZHVsZSA/IG9iaiA6IHsgZGVmYXVsdDogb2JqIH07IH1cblxuZnVuY3Rpb24gbmV3UmVxdWVzdCgpIHtcbiAgcmV0dXJuIG5ldyB3aW5kb3cuWE1MSHR0cFJlcXVlc3QoKTtcbn0gLyogZ2xvYmFsIHdpbmRvdyAqL1xuZnVuY3Rpb24gcmVzb2x2ZVVybChvcmlnaW4sIGxpbmspIHtcbiAgcmV0dXJuIG5ldyBfdXJsUGFyc2UyLmRlZmF1bHQobGluaywgb3JpZ2luKS50b1N0cmluZygpO1xufSIsIlwidXNlIHN0cmljdFwiO1xuXG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHtcbiAgdmFsdWU6IHRydWVcbn0pO1xuXG52YXIgX2NyZWF0ZUNsYXNzID0gZnVuY3Rpb24gKCkgeyBmdW5jdGlvbiBkZWZpbmVQcm9wZXJ0aWVzKHRhcmdldCwgcHJvcHMpIHsgZm9yICh2YXIgaSA9IDA7IGkgPCBwcm9wcy5sZW5ndGg7IGkrKykgeyB2YXIgZGVzY3JpcHRvciA9IHByb3BzW2ldOyBkZXNjcmlwdG9yLmVudW1lcmFibGUgPSBkZXNjcmlwdG9yLmVudW1lcmFibGUgfHwgZmFsc2U7IGRlc2NyaXB0b3IuY29uZmlndXJhYmxlID0gdHJ1ZTsgaWYgKFwidmFsdWVcIiBpbiBkZXNjcmlwdG9yKSBkZXNjcmlwdG9yLndyaXRhYmxlID0gdHJ1ZTsgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRhcmdldCwgZGVzY3JpcHRvci5rZXksIGRlc2NyaXB0b3IpOyB9IH0gcmV0dXJuIGZ1bmN0aW9uIChDb25zdHJ1Y3RvciwgcHJvdG9Qcm9wcywgc3RhdGljUHJvcHMpIHsgaWYgKHByb3RvUHJvcHMpIGRlZmluZVByb3BlcnRpZXMoQ29uc3RydWN0b3IucHJvdG90eXBlLCBwcm90b1Byb3BzKTsgaWYgKHN0YXRpY1Byb3BzKSBkZWZpbmVQcm9wZXJ0aWVzKENvbnN0cnVjdG9yLCBzdGF0aWNQcm9wcyk7IHJldHVybiBDb25zdHJ1Y3RvcjsgfTsgfSgpO1xuXG5leHBvcnRzLmdldFNvdXJjZSA9IGdldFNvdXJjZTtcblxudmFyIF9pc1JlYWN0TmF0aXZlID0gcmVxdWlyZShcIi4vaXNSZWFjdE5hdGl2ZVwiKTtcblxudmFyIF9pc1JlYWN0TmF0aXZlMiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX2lzUmVhY3ROYXRpdmUpO1xuXG52YXIgX3VyaVRvQmxvYiA9IHJlcXVpcmUoXCIuL3VyaVRvQmxvYlwiKTtcblxudmFyIF91cmlUb0Jsb2IyID0gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChfdXJpVG9CbG9iKTtcblxudmFyIF9pc0NvcmRvdmEgPSByZXF1aXJlKFwiLi9pc0NvcmRvdmFcIik7XG5cbnZhciBfaXNDb3Jkb3ZhMiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX2lzQ29yZG92YSk7XG5cbnZhciBfcmVhZEFzQnl0ZUFycmF5ID0gcmVxdWlyZShcIi4vcmVhZEFzQnl0ZUFycmF5XCIpO1xuXG52YXIgX3JlYWRBc0J5dGVBcnJheTIgPSBfaW50ZXJvcFJlcXVpcmVEZWZhdWx0KF9yZWFkQXNCeXRlQXJyYXkpO1xuXG5mdW5jdGlvbiBfaW50ZXJvcFJlcXVpcmVEZWZhdWx0KG9iaikgeyByZXR1cm4gb2JqICYmIG9iai5fX2VzTW9kdWxlID8gb2JqIDogeyBkZWZhdWx0OiBvYmogfTsgfVxuXG5mdW5jdGlvbiBfY2xhc3NDYWxsQ2hlY2soaW5zdGFuY2UsIENvbnN0cnVjdG9yKSB7IGlmICghKGluc3RhbmNlIGluc3RhbmNlb2YgQ29uc3RydWN0b3IpKSB7IHRocm93IG5ldyBUeXBlRXJyb3IoXCJDYW5ub3QgY2FsbCBhIGNsYXNzIGFzIGEgZnVuY3Rpb25cIik7IH0gfVxuXG52YXIgRmlsZVNvdXJjZSA9IGZ1bmN0aW9uICgpIHtcbiAgZnVuY3Rpb24gRmlsZVNvdXJjZShmaWxlKSB7XG4gICAgX2NsYXNzQ2FsbENoZWNrKHRoaXMsIEZpbGVTb3VyY2UpO1xuXG4gICAgdGhpcy5fZmlsZSA9IGZpbGU7XG4gICAgdGhpcy5zaXplID0gZmlsZS5zaXplO1xuICB9XG5cbiAgX2NyZWF0ZUNsYXNzKEZpbGVTb3VyY2UsIFt7XG4gICAga2V5OiBcInNsaWNlXCIsXG4gICAgdmFsdWU6IGZ1bmN0aW9uIHNsaWNlKHN0YXJ0LCBlbmQsIGNhbGxiYWNrKSB7XG4gICAgICAvLyBJbiBBcGFjaGUgQ29yZG92YSBhcHBsaWNhdGlvbnMsIGEgRmlsZSBtdXN0IGJlIHJlc29sdmVkIHVzaW5nXG4gICAgICAvLyBGaWxlUmVhZGVyIGluc3RhbmNlcywgc2VlXG4gICAgICAvLyBodHRwczovL2NvcmRvdmEuYXBhY2hlLm9yZy9kb2NzL2VuLzgueC9yZWZlcmVuY2UvY29yZG92YS1wbHVnaW4tZmlsZS9pbmRleC5odG1sI3JlYWQtYS1maWxlXG4gICAgICBpZiAoKDAsIF9pc0NvcmRvdmEyLmRlZmF1bHQpKCkpIHtcbiAgICAgICAgKDAsIF9yZWFkQXNCeXRlQXJyYXkyLmRlZmF1bHQpKHRoaXMuX2ZpbGUuc2xpY2Uoc3RhcnQsIGVuZCksIGZ1bmN0aW9uIChlcnIsIGNodW5rKSB7XG4gICAgICAgICAgaWYgKGVycikgcmV0dXJuIGNhbGxiYWNrKGVycik7XG5cbiAgICAgICAgICBjYWxsYmFjayhudWxsLCBjaHVuayk7XG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIGNhbGxiYWNrKG51bGwsIHRoaXMuX2ZpbGUuc2xpY2Uoc3RhcnQsIGVuZCkpO1xuICAgIH1cbiAgfSwge1xuICAgIGtleTogXCJjbG9zZVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBjbG9zZSgpIHt9XG4gIH1dKTtcblxuICByZXR1cm4gRmlsZVNvdXJjZTtcbn0oKTtcblxudmFyIFN0cmVhbVNvdXJjZSA9IGZ1bmN0aW9uICgpIHtcbiAgZnVuY3Rpb24gU3RyZWFtU291cmNlKHJlYWRlciwgY2h1bmtTaXplKSB7XG4gICAgX2NsYXNzQ2FsbENoZWNrKHRoaXMsIFN0cmVhbVNvdXJjZSk7XG5cbiAgICB0aGlzLl9jaHVua1NpemUgPSBjaHVua1NpemU7XG4gICAgdGhpcy5fYnVmZmVyID0gdW5kZWZpbmVkO1xuICAgIHRoaXMuX2J1ZmZlck9mZnNldCA9IDA7XG4gICAgdGhpcy5fcmVhZGVyID0gcmVhZGVyO1xuICAgIHRoaXMuX2RvbmUgPSBmYWxzZTtcbiAgfVxuXG4gIF9jcmVhdGVDbGFzcyhTdHJlYW1Tb3VyY2UsIFt7XG4gICAga2V5OiBcInNsaWNlXCIsXG4gICAgdmFsdWU6IGZ1bmN0aW9uIHNsaWNlKHN0YXJ0LCBlbmQsIGNhbGxiYWNrKSB7XG4gICAgICBpZiAoc3RhcnQgPCB0aGlzLl9idWZmZXJPZmZzZXQpIHtcbiAgICAgICAgY2FsbGJhY2sobmV3IEVycm9yKFwiUmVxdWVzdGVkIGRhdGEgaXMgYmVmb3JlIHRoZSByZWFkZXIncyBjdXJyZW50IG9mZnNldFwiKSk7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHRoaXMuX3JlYWRVbnRpbEVub3VnaERhdGFPckRvbmUoc3RhcnQsIGVuZCwgY2FsbGJhY2spO1xuICAgIH1cbiAgfSwge1xuICAgIGtleTogXCJfcmVhZFVudGlsRW5vdWdoRGF0YU9yRG9uZVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfcmVhZFVudGlsRW5vdWdoRGF0YU9yRG9uZShzdGFydCwgZW5kLCBjYWxsYmFjaykge1xuICAgICAgdmFyIF90aGlzID0gdGhpcztcblxuICAgICAgdmFyIGhhc0Vub3VnaERhdGEgPSBlbmQgPD0gdGhpcy5fYnVmZmVyT2Zmc2V0ICsgbGVuKHRoaXMuX2J1ZmZlcik7XG4gICAgICBpZiAodGhpcy5fZG9uZSB8fCBoYXNFbm91Z2hEYXRhKSB7XG4gICAgICAgIHZhciB2YWx1ZSA9IHRoaXMuX2dldERhdGFGcm9tQnVmZmVyKHN0YXJ0LCBlbmQpO1xuICAgICAgICBjYWxsYmFjayhudWxsLCB2YWx1ZSwgdmFsdWUgPT0gbnVsbCA/IHRoaXMuX2RvbmUgOiBmYWxzZSk7XG4gICAgICAgIHJldHVybjtcbiAgICAgIH1cbiAgICAgIHRoaXMuX3JlYWRlci5yZWFkKCkudGhlbihmdW5jdGlvbiAoX3JlZikge1xuICAgICAgICB2YXIgdmFsdWUgPSBfcmVmLnZhbHVlLFxuICAgICAgICAgICAgZG9uZSA9IF9yZWYuZG9uZTtcblxuICAgICAgICBpZiAoZG9uZSkge1xuICAgICAgICAgIF90aGlzLl9kb25lID0gdHJ1ZTtcbiAgICAgICAgfSBlbHNlIGlmIChfdGhpcy5fYnVmZmVyID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICBfdGhpcy5fYnVmZmVyID0gdmFsdWU7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgX3RoaXMuX2J1ZmZlciA9IGNvbmNhdChfdGhpcy5fYnVmZmVyLCB2YWx1ZSk7XG4gICAgICAgIH1cblxuICAgICAgICBfdGhpcy5fcmVhZFVudGlsRW5vdWdoRGF0YU9yRG9uZShzdGFydCwgZW5kLCBjYWxsYmFjayk7XG4gICAgICB9KS5jYXRjaChmdW5jdGlvbiAoZXJyKSB7XG4gICAgICAgIGNhbGxiYWNrKG5ldyBFcnJvcihcIkVycm9yIGR1cmluZyByZWFkOiBcIiArIGVycikpO1xuICAgICAgfSk7XG4gICAgfVxuICB9LCB7XG4gICAga2V5OiBcIl9nZXREYXRhRnJvbUJ1ZmZlclwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfZ2V0RGF0YUZyb21CdWZmZXIoc3RhcnQsIGVuZCkge1xuICAgICAgLy8gUmVtb3ZlIGRhdGEgZnJvbSBidWZmZXIgYmVmb3JlIGBzdGFydGAuXG4gICAgICAvLyBEYXRhIG1pZ2h0IGJlIHJlcmVhZCBmcm9tIHRoZSBidWZmZXIgaWYgYW4gdXBsb2FkIGZhaWxzLCBzbyB3ZSBjYW4gb25seVxuICAgICAgLy8gc2FmZWx5IGRlbGV0ZSBkYXRhIHdoZW4gaXQgY29tZXMgKmJlZm9yZSogd2hhdCBpcyBjdXJyZW50bHkgYmVpbmcgcmVhZC5cbiAgICAgIGlmIChzdGFydCA+IHRoaXMuX2J1ZmZlck9mZnNldCkge1xuICAgICAgICB0aGlzLl9idWZmZXIgPSB0aGlzLl9idWZmZXIuc2xpY2Uoc3RhcnQgLSB0aGlzLl9idWZmZXJPZmZzZXQpO1xuICAgICAgICB0aGlzLl9idWZmZXJPZmZzZXQgPSBzdGFydDtcbiAgICAgIH1cbiAgICAgIC8vIElmIHRoZSBidWZmZXIgaXMgZW1wdHkgYWZ0ZXIgcmVtb3Zpbmcgb2xkIGRhdGEsIGFsbCBkYXRhIGhhcyBiZWVuIHJlYWQuXG4gICAgICB2YXIgaGFzQWxsRGF0YUJlZW5SZWFkID0gbGVuKHRoaXMuX2J1ZmZlcikgPT09IDA7XG4gICAgICBpZiAodGhpcy5fZG9uZSAmJiBoYXNBbGxEYXRhQmVlblJlYWQpIHtcbiAgICAgICAgcmV0dXJuIG51bGw7XG4gICAgICB9XG4gICAgICAvLyBXZSBhbHJlYWR5IHJlbW92ZWQgZGF0YSBiZWZvcmUgYHN0YXJ0YCwgc28gd2UganVzdCByZXR1cm4gdGhlIGZpcnN0XG4gICAgICAvLyBjaHVuayBmcm9tIHRoZSBidWZmZXIuXG4gICAgICByZXR1cm4gdGhpcy5fYnVmZmVyLnNsaWNlKDAsIGVuZCAtIHN0YXJ0KTtcbiAgICB9XG4gIH0sIHtcbiAgICBrZXk6IFwiY2xvc2VcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gY2xvc2UoKSB7XG4gICAgICBpZiAodGhpcy5fcmVhZGVyLmNhbmNlbCkge1xuICAgICAgICB0aGlzLl9yZWFkZXIuY2FuY2VsKCk7XG4gICAgICB9XG4gICAgfVxuICB9XSk7XG5cbiAgcmV0dXJuIFN0cmVhbVNvdXJjZTtcbn0oKTtcblxuZnVuY3Rpb24gbGVuKGJsb2JPckFycmF5KSB7XG4gIGlmIChibG9iT3JBcnJheSA9PT0gdW5kZWZpbmVkKSByZXR1cm4gMDtcbiAgaWYgKGJsb2JPckFycmF5LnNpemUgIT09IHVuZGVmaW5lZCkgcmV0dXJuIGJsb2JPckFycmF5LnNpemU7XG4gIHJldHVybiBibG9iT3JBcnJheS5sZW5ndGg7XG59XG5cbi8qXG4gIFR5cGVkIGFycmF5cyBhbmQgYmxvYnMgZG9uJ3QgaGF2ZSBhIGNvbmNhdCBtZXRob2QuXG4gIFRoaXMgZnVuY3Rpb24gaGVscHMgU3RyZWFtU291cmNlIGFjY3VtdWxhdGUgZGF0YSB0byByZWFjaCBjaHVua1NpemUuXG4qL1xuZnVuY3Rpb24gY29uY2F0KGEsIGIpIHtcbiAgaWYgKGEuY29uY2F0KSB7XG4gICAgLy8gSXMgYGFgIGFuIEFycmF5P1xuICAgIHJldHVybiBhLmNvbmNhdChiKTtcbiAgfVxuICBpZiAoYSBpbnN0YW5jZW9mIEJsb2IpIHtcbiAgICByZXR1cm4gbmV3IEJsb2IoW2EsIGJdLCB7IHR5cGU6IGEudHlwZSB9KTtcbiAgfVxuICBpZiAoYS5zZXQpIHtcbiAgICAvLyBJcyBgYWAgYSB0eXBlZCBhcnJheT9cbiAgICB2YXIgYyA9IG5ldyBhLmNvbnN0cnVjdG9yKGEubGVuZ3RoICsgYi5sZW5ndGgpO1xuICAgIGMuc2V0KGEpO1xuICAgIGMuc2V0KGIsIGEubGVuZ3RoKTtcbiAgICByZXR1cm4gYztcbiAgfVxuICB0aHJvdyBuZXcgRXJyb3IoXCJVbmtub3duIGRhdGEgdHlwZVwiKTtcbn1cblxuZnVuY3Rpb24gZ2V0U291cmNlKGlucHV0LCBjaHVua1NpemUsIGNhbGxiYWNrKSB7XG4gIC8vIEluIFJlYWN0IE5hdGl2ZSwgd2hlbiB1c2VyIHNlbGVjdHMgYSBmaWxlLCBpbnN0ZWFkIG9mIGEgRmlsZSBvciBCbG9iLFxuICAvLyB5b3UgdXN1YWxseSBnZXQgYSBmaWxlIG9iamVjdCB7fSB3aXRoIGEgdXJpIHByb3BlcnR5IHRoYXQgY29udGFpbnNcbiAgLy8gYSBsb2NhbCBwYXRoIHRvIHRoZSBmaWxlLiBXZSB1c2UgWE1MSHR0cFJlcXVlc3QgdG8gZmV0Y2hcbiAgLy8gdGhlIGZpbGUgYmxvYiwgYmVmb3JlIHVwbG9hZGluZyB3aXRoIHR1cy5cbiAgaWYgKCgwLCBfaXNSZWFjdE5hdGl2ZTIuZGVmYXVsdCkoKSAmJiBpbnB1dCAmJiB0eXBlb2YgaW5wdXQudXJpICE9PSBcInVuZGVmaW5lZFwiKSB7XG4gICAgKDAsIF91cmlUb0Jsb2IyLmRlZmF1bHQpKGlucHV0LnVyaSwgZnVuY3Rpb24gKGVyciwgYmxvYikge1xuICAgICAgaWYgKGVycikge1xuICAgICAgICByZXR1cm4gY2FsbGJhY2sobmV3IEVycm9yKFwidHVzOiBjYW5ub3QgZmV0Y2ggYGZpbGUudXJpYCBhcyBCbG9iLCBtYWtlIHN1cmUgdGhlIHVyaSBpcyBjb3JyZWN0IGFuZCBhY2Nlc3NpYmxlLiBcIiArIGVycikpO1xuICAgICAgfVxuICAgICAgY2FsbGJhY2sobnVsbCwgbmV3IEZpbGVTb3VyY2UoYmxvYikpO1xuICAgIH0pO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIC8vIFNpbmNlIHdlIGVtdWxhdGUgdGhlIEJsb2IgdHlwZSBpbiBvdXIgdGVzdHMgKG5vdCBhbGwgdGFyZ2V0IGJyb3dzZXJzXG4gIC8vIHN1cHBvcnQgaXQpLCB3ZSBjYW5ub3QgdXNlIGBpbnN0YW5jZW9mYCBmb3IgdGVzdGluZyB3aGV0aGVyIHRoZSBpbnB1dCB2YWx1ZVxuICAvLyBjYW4gYmUgaGFuZGxlZC4gSW5zdGVhZCwgd2Ugc2ltcGx5IGNoZWNrIGlzIHRoZSBzbGljZSgpIGZ1bmN0aW9uIGFuZCB0aGVcbiAgLy8gc2l6ZSBwcm9wZXJ0eSBhcmUgYXZhaWxhYmxlLlxuICBpZiAodHlwZW9mIGlucHV0LnNsaWNlID09PSBcImZ1bmN0aW9uXCIgJiYgdHlwZW9mIGlucHV0LnNpemUgIT09IFwidW5kZWZpbmVkXCIpIHtcbiAgICBjYWxsYmFjayhudWxsLCBuZXcgRmlsZVNvdXJjZShpbnB1dCkpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGlmICh0eXBlb2YgaW5wdXQucmVhZCA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgY2h1bmtTaXplID0gK2NodW5rU2l6ZTtcbiAgICBpZiAoIWlzRmluaXRlKGNodW5rU2l6ZSkpIHtcbiAgICAgIGNhbGxiYWNrKG5ldyBFcnJvcihcImNhbm5vdCBjcmVhdGUgc291cmNlIGZvciBzdHJlYW0gd2l0aG91dCBhIGZpbml0ZSB2YWx1ZSBmb3IgdGhlIGBjaHVua1NpemVgIG9wdGlvblwiKSk7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGNhbGxiYWNrKG51bGwsIG5ldyBTdHJlYW1Tb3VyY2UoaW5wdXQsIGNodW5rU2l6ZSkpO1xuICAgIHJldHVybjtcbiAgfVxuXG4gIGNhbGxiYWNrKG5ldyBFcnJvcihcInNvdXJjZSBvYmplY3QgbWF5IG9ubHkgYmUgYW4gaW5zdGFuY2Ugb2YgRmlsZSwgQmxvYiwgb3IgUmVhZGVyIGluIHRoaXMgZW52aXJvbm1lbnRcIikpO1xufSIsIlwidXNlIHN0cmljdFwiO1xuXG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHtcbiAgdmFsdWU6IHRydWVcbn0pO1xuXG52YXIgX2NyZWF0ZUNsYXNzID0gZnVuY3Rpb24gKCkgeyBmdW5jdGlvbiBkZWZpbmVQcm9wZXJ0aWVzKHRhcmdldCwgcHJvcHMpIHsgZm9yICh2YXIgaSA9IDA7IGkgPCBwcm9wcy5sZW5ndGg7IGkrKykgeyB2YXIgZGVzY3JpcHRvciA9IHByb3BzW2ldOyBkZXNjcmlwdG9yLmVudW1lcmFibGUgPSBkZXNjcmlwdG9yLmVudW1lcmFibGUgfHwgZmFsc2U7IGRlc2NyaXB0b3IuY29uZmlndXJhYmxlID0gdHJ1ZTsgaWYgKFwidmFsdWVcIiBpbiBkZXNjcmlwdG9yKSBkZXNjcmlwdG9yLndyaXRhYmxlID0gdHJ1ZTsgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRhcmdldCwgZGVzY3JpcHRvci5rZXksIGRlc2NyaXB0b3IpOyB9IH0gcmV0dXJuIGZ1bmN0aW9uIChDb25zdHJ1Y3RvciwgcHJvdG9Qcm9wcywgc3RhdGljUHJvcHMpIHsgaWYgKHByb3RvUHJvcHMpIGRlZmluZVByb3BlcnRpZXMoQ29uc3RydWN0b3IucHJvdG90eXBlLCBwcm90b1Byb3BzKTsgaWYgKHN0YXRpY1Byb3BzKSBkZWZpbmVQcm9wZXJ0aWVzKENvbnN0cnVjdG9yLCBzdGF0aWNQcm9wcyk7IHJldHVybiBDb25zdHJ1Y3RvcjsgfTsgfSgpO1xuXG5leHBvcnRzLmdldFN0b3JhZ2UgPSBnZXRTdG9yYWdlO1xuXG5mdW5jdGlvbiBfY2xhc3NDYWxsQ2hlY2soaW5zdGFuY2UsIENvbnN0cnVjdG9yKSB7IGlmICghKGluc3RhbmNlIGluc3RhbmNlb2YgQ29uc3RydWN0b3IpKSB7IHRocm93IG5ldyBUeXBlRXJyb3IoXCJDYW5ub3QgY2FsbCBhIGNsYXNzIGFzIGEgZnVuY3Rpb25cIik7IH0gfVxuXG4vKiBnbG9iYWwgd2luZG93LCBsb2NhbFN0b3JhZ2UgKi9cblxudmFyIGhhc1N0b3JhZ2UgPSBmYWxzZTtcbnRyeSB7XG4gIGhhc1N0b3JhZ2UgPSBcImxvY2FsU3RvcmFnZVwiIGluIHdpbmRvdztcblxuICAvLyBBdHRlbXB0IHRvIHN0b3JlIGFuZCByZWFkIGVudHJpZXMgZnJvbSB0aGUgbG9jYWwgc3RvcmFnZSB0byBkZXRlY3QgUHJpdmF0ZVxuICAvLyBNb2RlIG9uIFNhZmFyaSBvbiBpT1MgKHNlZSAjNDkpXG4gIHZhciBrZXkgPSBcInR1c1N1cHBvcnRcIjtcbiAgbG9jYWxTdG9yYWdlLnNldEl0ZW0oa2V5LCBsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpKTtcbn0gY2F0Y2ggKGUpIHtcbiAgLy8gSWYgd2UgdHJ5IHRvIGFjY2VzcyBsb2NhbFN0b3JhZ2UgaW5zaWRlIGEgc2FuZGJveGVkIGlmcmFtZSwgYSBTZWN1cml0eUVycm9yXG4gIC8vIGlzIHRocm93bi4gV2hlbiBpbiBwcml2YXRlIG1vZGUgb24gaU9TIFNhZmFyaSwgYSBRdW90YUV4Y2VlZGVkRXJyb3IgaXNcbiAgLy8gdGhyb3duIChzZWUgIzQ5KVxuICBpZiAoZS5jb2RlID09PSBlLlNFQ1VSSVRZX0VSUiB8fCBlLmNvZGUgPT09IGUuUVVPVEFfRVhDRUVERURfRVJSKSB7XG4gICAgaGFzU3RvcmFnZSA9IGZhbHNlO1xuICB9IGVsc2Uge1xuICAgIHRocm93IGU7XG4gIH1cbn1cblxudmFyIGNhblN0b3JlVVJMcyA9IGV4cG9ydHMuY2FuU3RvcmVVUkxzID0gaGFzU3RvcmFnZTtcblxudmFyIExvY2FsU3RvcmFnZSA9IGZ1bmN0aW9uICgpIHtcbiAgZnVuY3Rpb24gTG9jYWxTdG9yYWdlKCkge1xuICAgIF9jbGFzc0NhbGxDaGVjayh0aGlzLCBMb2NhbFN0b3JhZ2UpO1xuICB9XG5cbiAgX2NyZWF0ZUNsYXNzKExvY2FsU3RvcmFnZSwgW3tcbiAgICBrZXk6IFwic2V0SXRlbVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBzZXRJdGVtKGtleSwgdmFsdWUsIGNiKSB7XG4gICAgICBjYihudWxsLCBsb2NhbFN0b3JhZ2Uuc2V0SXRlbShrZXksIHZhbHVlKSk7XG4gICAgfVxuICB9LCB7XG4gICAga2V5OiBcImdldEl0ZW1cIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gZ2V0SXRlbShrZXksIGNiKSB7XG4gICAgICBjYihudWxsLCBsb2NhbFN0b3JhZ2UuZ2V0SXRlbShrZXkpKTtcbiAgICB9XG4gIH0sIHtcbiAgICBrZXk6IFwicmVtb3ZlSXRlbVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiByZW1vdmVJdGVtKGtleSwgY2IpIHtcbiAgICAgIGNiKG51bGwsIGxvY2FsU3RvcmFnZS5yZW1vdmVJdGVtKGtleSkpO1xuICAgIH1cbiAgfV0pO1xuXG4gIHJldHVybiBMb2NhbFN0b3JhZ2U7XG59KCk7XG5cbmZ1bmN0aW9uIGdldFN0b3JhZ2UoKSB7XG4gIHJldHVybiBoYXNTdG9yYWdlID8gbmV3IExvY2FsU3RvcmFnZSgpIDogbnVsbDtcbn0iLCJcInVzZSBzdHJpY3RcIjtcblxuT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFwiX19lc01vZHVsZVwiLCB7XG4gIHZhbHVlOiB0cnVlXG59KTtcbi8qKlxuICogdXJpVG9CbG9iIHJlc29sdmVzIGEgVVJJIHRvIGEgQmxvYiBvYmplY3QuIFRoaXMgaXMgdXNlZCBmb3JcbiAqIFJlYWN0IE5hdGl2ZSB0byByZXRyaWV2ZSBhIGZpbGUgKGlkZW50aWZpZWQgYnkgYSBmaWxlOi8vXG4gKiBVUkkpIGFzIGEgYmxvYi5cbiAqL1xuZnVuY3Rpb24gdXJpVG9CbG9iKHVyaSwgZG9uZSkge1xuICB2YXIgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KCk7XG4gIHhoci5yZXNwb25zZVR5cGUgPSBcImJsb2JcIjtcbiAgeGhyLm9ubG9hZCA9IGZ1bmN0aW9uICgpIHtcbiAgICB2YXIgYmxvYiA9IHhoci5yZXNwb25zZTtcbiAgICBkb25lKG51bGwsIGJsb2IpO1xuICB9O1xuICB4aHIub25lcnJvciA9IGZ1bmN0aW9uIChlcnIpIHtcbiAgICBkb25lKGVycik7XG4gIH07XG4gIHhoci5vcGVuKFwiR0VUXCIsIHVyaSk7XG4gIHhoci5zZW5kKCk7XG59XG5cbmV4cG9ydHMuZGVmYXVsdCA9IHVyaVRvQmxvYjsiLCJcInVzZSBzdHJpY3RcIjtcblxuT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFwiX19lc01vZHVsZVwiLCB7XG4gIHZhbHVlOiB0cnVlXG59KTtcblxuZnVuY3Rpb24gX2NsYXNzQ2FsbENoZWNrKGluc3RhbmNlLCBDb25zdHJ1Y3RvcikgeyBpZiAoIShpbnN0YW5jZSBpbnN0YW5jZW9mIENvbnN0cnVjdG9yKSkgeyB0aHJvdyBuZXcgVHlwZUVycm9yKFwiQ2Fubm90IGNhbGwgYSBjbGFzcyBhcyBhIGZ1bmN0aW9uXCIpOyB9IH1cblxuZnVuY3Rpb24gX3Bvc3NpYmxlQ29uc3RydWN0b3JSZXR1cm4oc2VsZiwgY2FsbCkgeyBpZiAoIXNlbGYpIHsgdGhyb3cgbmV3IFJlZmVyZW5jZUVycm9yKFwidGhpcyBoYXNuJ3QgYmVlbiBpbml0aWFsaXNlZCAtIHN1cGVyKCkgaGFzbid0IGJlZW4gY2FsbGVkXCIpOyB9IHJldHVybiBjYWxsICYmICh0eXBlb2YgY2FsbCA9PT0gXCJvYmplY3RcIiB8fCB0eXBlb2YgY2FsbCA9PT0gXCJmdW5jdGlvblwiKSA/IGNhbGwgOiBzZWxmOyB9XG5cbmZ1bmN0aW9uIF9pbmhlcml0cyhzdWJDbGFzcywgc3VwZXJDbGFzcykgeyBpZiAodHlwZW9mIHN1cGVyQ2xhc3MgIT09IFwiZnVuY3Rpb25cIiAmJiBzdXBlckNsYXNzICE9PSBudWxsKSB7IHRocm93IG5ldyBUeXBlRXJyb3IoXCJTdXBlciBleHByZXNzaW9uIG11c3QgZWl0aGVyIGJlIG51bGwgb3IgYSBmdW5jdGlvbiwgbm90IFwiICsgdHlwZW9mIHN1cGVyQ2xhc3MpOyB9IHN1YkNsYXNzLnByb3RvdHlwZSA9IE9iamVjdC5jcmVhdGUoc3VwZXJDbGFzcyAmJiBzdXBlckNsYXNzLnByb3RvdHlwZSwgeyBjb25zdHJ1Y3RvcjogeyB2YWx1ZTogc3ViQ2xhc3MsIGVudW1lcmFibGU6IGZhbHNlLCB3cml0YWJsZTogdHJ1ZSwgY29uZmlndXJhYmxlOiB0cnVlIH0gfSk7IGlmIChzdXBlckNsYXNzKSBPYmplY3Quc2V0UHJvdG90eXBlT2YgPyBPYmplY3Quc2V0UHJvdG90eXBlT2Yoc3ViQ2xhc3MsIHN1cGVyQ2xhc3MpIDogc3ViQ2xhc3MuX19wcm90b19fID0gc3VwZXJDbGFzczsgfVxuXG52YXIgRGV0YWlsZWRFcnJvciA9IGZ1bmN0aW9uIChfRXJyb3IpIHtcbiAgX2luaGVyaXRzKERldGFpbGVkRXJyb3IsIF9FcnJvcik7XG5cbiAgZnVuY3Rpb24gRGV0YWlsZWRFcnJvcihlcnJvcikge1xuICAgIHZhciBjYXVzaW5nRXJyID0gYXJndW1lbnRzLmxlbmd0aCA+IDEgJiYgYXJndW1lbnRzWzFdICE9PSB1bmRlZmluZWQgPyBhcmd1bWVudHNbMV0gOiBudWxsO1xuICAgIHZhciB4aHIgPSBhcmd1bWVudHMubGVuZ3RoID4gMiAmJiBhcmd1bWVudHNbMl0gIT09IHVuZGVmaW5lZCA/IGFyZ3VtZW50c1syXSA6IG51bGw7XG5cbiAgICBfY2xhc3NDYWxsQ2hlY2sodGhpcywgRGV0YWlsZWRFcnJvcik7XG5cbiAgICB2YXIgX3RoaXMgPSBfcG9zc2libGVDb25zdHJ1Y3RvclJldHVybih0aGlzLCAoRGV0YWlsZWRFcnJvci5fX3Byb3RvX18gfHwgT2JqZWN0LmdldFByb3RvdHlwZU9mKERldGFpbGVkRXJyb3IpKS5jYWxsKHRoaXMsIGVycm9yLm1lc3NhZ2UpKTtcblxuICAgIF90aGlzLm9yaWdpbmFsUmVxdWVzdCA9IHhocjtcbiAgICBfdGhpcy5jYXVzaW5nRXJyb3IgPSBjYXVzaW5nRXJyO1xuXG4gICAgdmFyIG1lc3NhZ2UgPSBlcnJvci5tZXNzYWdlO1xuICAgIGlmIChjYXVzaW5nRXJyICE9IG51bGwpIHtcbiAgICAgIG1lc3NhZ2UgKz0gXCIsIGNhdXNlZCBieSBcIiArIGNhdXNpbmdFcnIudG9TdHJpbmcoKTtcbiAgICB9XG4gICAgaWYgKHhociAhPSBudWxsKSB7XG4gICAgICBtZXNzYWdlICs9IFwiLCBvcmlnaW5hdGVkIGZyb20gcmVxdWVzdCAocmVzcG9uc2UgY29kZTogXCIgKyB4aHIuc3RhdHVzICsgXCIsIHJlc3BvbnNlIHRleHQ6IFwiICsgeGhyLnJlc3BvbnNlVGV4dCArIFwiKVwiO1xuICAgIH1cbiAgICBfdGhpcy5tZXNzYWdlID0gbWVzc2FnZTtcbiAgICByZXR1cm4gX3RoaXM7XG4gIH1cblxuICByZXR1cm4gRGV0YWlsZWRFcnJvcjtcbn0oRXJyb3IpO1xuXG5leHBvcnRzLmRlZmF1bHQgPSBEZXRhaWxlZEVycm9yOyIsIlwidXNlIHN0cmljdFwiO1xuXG52YXIgX3VwbG9hZCA9IHJlcXVpcmUoXCIuL3VwbG9hZFwiKTtcblxudmFyIF91cGxvYWQyID0gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChfdXBsb2FkKTtcblxudmFyIF9zdG9yYWdlID0gcmVxdWlyZShcIi4vbm9kZS9zdG9yYWdlXCIpO1xuXG52YXIgc3RvcmFnZSA9IF9pbnRlcm9wUmVxdWlyZVdpbGRjYXJkKF9zdG9yYWdlKTtcblxuZnVuY3Rpb24gX2ludGVyb3BSZXF1aXJlV2lsZGNhcmQob2JqKSB7IGlmIChvYmogJiYgb2JqLl9fZXNNb2R1bGUpIHsgcmV0dXJuIG9iajsgfSBlbHNlIHsgdmFyIG5ld09iaiA9IHt9OyBpZiAob2JqICE9IG51bGwpIHsgZm9yICh2YXIga2V5IGluIG9iaikgeyBpZiAoT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwga2V5KSkgbmV3T2JqW2tleV0gPSBvYmpba2V5XTsgfSB9IG5ld09iai5kZWZhdWx0ID0gb2JqOyByZXR1cm4gbmV3T2JqOyB9IH1cblxuZnVuY3Rpb24gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChvYmopIHsgcmV0dXJuIG9iaiAmJiBvYmouX19lc01vZHVsZSA/IG9iaiA6IHsgZGVmYXVsdDogb2JqIH07IH1cblxuLyogZ2xvYmFsIHdpbmRvdyAqL1xudmFyIGRlZmF1bHRPcHRpb25zID0gX3VwbG9hZDIuZGVmYXVsdC5kZWZhdWx0T3B0aW9ucztcblxuXG52YXIgbW9kdWxlRXhwb3J0ID0ge1xuICBVcGxvYWQ6IF91cGxvYWQyLmRlZmF1bHQsXG4gIGNhblN0b3JlVVJMczogc3RvcmFnZS5jYW5TdG9yZVVSTHMsXG4gIGRlZmF1bHRPcHRpb25zOiBkZWZhdWx0T3B0aW9uc1xufTtcblxuaWYgKHR5cGVvZiB3aW5kb3cgIT09IFwidW5kZWZpbmVkXCIpIHtcbiAgLy8gQnJvd3NlciBlbnZpcm9ubWVudCB1c2luZyBYTUxIdHRwUmVxdWVzdFxuICB2YXIgX3dpbmRvdyA9IHdpbmRvdyxcbiAgICAgIFhNTEh0dHBSZXF1ZXN0ID0gX3dpbmRvdy5YTUxIdHRwUmVxdWVzdCxcbiAgICAgIEJsb2IgPSBfd2luZG93LkJsb2I7XG5cblxuICBtb2R1bGVFeHBvcnQuaXNTdXBwb3J0ZWQgPSBYTUxIdHRwUmVxdWVzdCAmJiBCbG9iICYmIHR5cGVvZiBCbG9iLnByb3RvdHlwZS5zbGljZSA9PT0gXCJmdW5jdGlvblwiO1xufSBlbHNlIHtcbiAgLy8gTm9kZS5qcyBlbnZpcm9ubWVudCB1c2luZyBodHRwIG1vZHVsZVxuICBtb2R1bGVFeHBvcnQuaXNTdXBwb3J0ZWQgPSB0cnVlO1xuICAvLyBtYWtlIEZpbGVTdG9yYWdlIG1vZHVsZSBhdmFpbGFibGUgYXMgaXQgd2lsbCBub3QgYmUgc2V0IGJ5IGRlZmF1bHQuXG4gIG1vZHVsZUV4cG9ydC5GaWxlU3RvcmFnZSA9IHN0b3JhZ2UuRmlsZVN0b3JhZ2U7XG59XG5cbi8vIFRoZSB1c2FnZSBvZiB0aGUgY29tbW9uanMgZXhwb3J0aW5nIHN5bnRheCBpbnN0ZWFkIG9mIHRoZSBuZXcgRUNNQVNjcmlwdFxuLy8gb25lIGlzIGFjdHVhbGx5IGludGVkZWQgYW5kIHByZXZlbnRzIHdlaXJkIGJlaGF2aW91ciBpZiB3ZSBhcmUgdHJ5aW5nIHRvXG4vLyBpbXBvcnQgdGhpcyBtb2R1bGUgaW4gYW5vdGhlciBtb2R1bGUgdXNpbmcgQmFiZWwuXG5tb2R1bGUuZXhwb3J0cyA9IG1vZHVsZUV4cG9ydDsiLCJcInVzZSBzdHJpY3RcIjtcblxuT2JqZWN0LmRlZmluZVByb3BlcnR5KGV4cG9ydHMsIFwiX19lc01vZHVsZVwiLCB7XG4gIHZhbHVlOiB0cnVlXG59KTtcblxudmFyIF9jcmVhdGVDbGFzcyA9IGZ1bmN0aW9uICgpIHsgZnVuY3Rpb24gZGVmaW5lUHJvcGVydGllcyh0YXJnZXQsIHByb3BzKSB7IGZvciAodmFyIGkgPSAwOyBpIDwgcHJvcHMubGVuZ3RoOyBpKyspIHsgdmFyIGRlc2NyaXB0b3IgPSBwcm9wc1tpXTsgZGVzY3JpcHRvci5lbnVtZXJhYmxlID0gZGVzY3JpcHRvci5lbnVtZXJhYmxlIHx8IGZhbHNlOyBkZXNjcmlwdG9yLmNvbmZpZ3VyYWJsZSA9IHRydWU7IGlmIChcInZhbHVlXCIgaW4gZGVzY3JpcHRvcikgZGVzY3JpcHRvci53cml0YWJsZSA9IHRydWU7IE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0YXJnZXQsIGRlc2NyaXB0b3Iua2V5LCBkZXNjcmlwdG9yKTsgfSB9IHJldHVybiBmdW5jdGlvbiAoQ29uc3RydWN0b3IsIHByb3RvUHJvcHMsIHN0YXRpY1Byb3BzKSB7IGlmIChwcm90b1Byb3BzKSBkZWZpbmVQcm9wZXJ0aWVzKENvbnN0cnVjdG9yLnByb3RvdHlwZSwgcHJvdG9Qcm9wcyk7IGlmIChzdGF0aWNQcm9wcykgZGVmaW5lUHJvcGVydGllcyhDb25zdHJ1Y3Rvciwgc3RhdGljUHJvcHMpOyByZXR1cm4gQ29uc3RydWN0b3I7IH07IH0oKTsgLyogZ2xvYmFsIHdpbmRvdyAqL1xuXG5cbi8vIFdlIGltcG9ydCB0aGUgZmlsZXMgdXNlZCBpbnNpZGUgdGhlIE5vZGUgZW52aXJvbm1lbnQgd2hpY2ggYXJlIHJld3JpdHRlblxuLy8gZm9yIGJyb3dzZXJzIHVzaW5nIHRoZSBydWxlcyBkZWZpbmVkIGluIHRoZSBwYWNrYWdlLmpzb25cblxuXG52YXIgX2Vycm9yID0gcmVxdWlyZShcIi4vZXJyb3JcIik7XG5cbnZhciBfZXJyb3IyID0gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChfZXJyb3IpO1xuXG52YXIgX2V4dGVuZCA9IHJlcXVpcmUoXCJleHRlbmRcIik7XG5cbnZhciBfZXh0ZW5kMiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX2V4dGVuZCk7XG5cbnZhciBfanNCYXNlID0gcmVxdWlyZShcImpzLWJhc2U2NFwiKTtcblxudmFyIF9yZXF1ZXN0ID0gcmVxdWlyZShcIi4vbm9kZS9yZXF1ZXN0XCIpO1xuXG52YXIgX3NvdXJjZSA9IHJlcXVpcmUoXCIuL25vZGUvc291cmNlXCIpO1xuXG52YXIgX3N0b3JhZ2UgPSByZXF1aXJlKFwiLi9ub2RlL3N0b3JhZ2VcIik7XG5cbnZhciBfZmluZ2VycHJpbnQgPSByZXF1aXJlKFwiLi9ub2RlL2ZpbmdlcnByaW50XCIpO1xuXG52YXIgX2ZpbmdlcnByaW50MiA9IF9pbnRlcm9wUmVxdWlyZURlZmF1bHQoX2ZpbmdlcnByaW50KTtcblxuZnVuY3Rpb24gX2ludGVyb3BSZXF1aXJlRGVmYXVsdChvYmopIHsgcmV0dXJuIG9iaiAmJiBvYmouX19lc01vZHVsZSA/IG9iaiA6IHsgZGVmYXVsdDogb2JqIH07IH1cblxuZnVuY3Rpb24gX2NsYXNzQ2FsbENoZWNrKGluc3RhbmNlLCBDb25zdHJ1Y3RvcikgeyBpZiAoIShpbnN0YW5jZSBpbnN0YW5jZW9mIENvbnN0cnVjdG9yKSkgeyB0aHJvdyBuZXcgVHlwZUVycm9yKFwiQ2Fubm90IGNhbGwgYSBjbGFzcyBhcyBhIGZ1bmN0aW9uXCIpOyB9IH1cblxudmFyIGRlZmF1bHRPcHRpb25zID0ge1xuICBlbmRwb2ludDogbnVsbCxcbiAgZmluZ2VycHJpbnQ6IF9maW5nZXJwcmludDIuZGVmYXVsdCxcbiAgcmVzdW1lOiB0cnVlLFxuICBvblByb2dyZXNzOiBudWxsLFxuICBvbkNodW5rQ29tcGxldGU6IG51bGwsXG4gIG9uU3VjY2VzczogbnVsbCxcbiAgb25FcnJvcjogbnVsbCxcbiAgaGVhZGVyczoge30sXG4gIGNodW5rU2l6ZTogSW5maW5pdHksXG4gIHdpdGhDcmVkZW50aWFsczogZmFsc2UsXG4gIHVwbG9hZFVybDogbnVsbCxcbiAgdXBsb2FkU2l6ZTogbnVsbCxcbiAgb3ZlcnJpZGVQYXRjaE1ldGhvZDogZmFsc2UsXG4gIHJldHJ5RGVsYXlzOiBudWxsLFxuICByZW1vdmVGaW5nZXJwcmludE9uU3VjY2VzczogZmFsc2UsXG4gIHVwbG9hZExlbmd0aERlZmVycmVkOiBmYWxzZSxcbiAgdXJsU3RvcmFnZTogbnVsbCxcbiAgZmlsZVJlYWRlcjogbnVsbFxufTtcblxudmFyIFVwbG9hZCA9IGZ1bmN0aW9uICgpIHtcbiAgZnVuY3Rpb24gVXBsb2FkKGZpbGUsIG9wdGlvbnMpIHtcbiAgICBfY2xhc3NDYWxsQ2hlY2sodGhpcywgVXBsb2FkKTtcblxuICAgIHRoaXMub3B0aW9ucyA9ICgwLCBfZXh0ZW5kMi5kZWZhdWx0KSh0cnVlLCB7fSwgZGVmYXVsdE9wdGlvbnMsIG9wdGlvbnMpO1xuXG4gICAgLy8gVGhlIHN0b3JhZ2UgbW9kdWxlIHVzZWQgdG8gc3RvcmUgVVJMc1xuICAgIHRoaXMuX3N0b3JhZ2UgPSB0aGlzLm9wdGlvbnMudXJsU3RvcmFnZTtcblxuICAgIC8vIFRoZSB1bmRlcmx5aW5nIEZpbGUvQmxvYiBvYmplY3RcbiAgICB0aGlzLmZpbGUgPSBmaWxlO1xuXG4gICAgLy8gVGhlIFVSTCBhZ2FpbnN0IHdoaWNoIHRoZSBmaWxlIHdpbGwgYmUgdXBsb2FkZWRcbiAgICB0aGlzLnVybCA9IG51bGw7XG5cbiAgICAvLyBUaGUgdW5kZXJseWluZyBYSFIgb2JqZWN0IGZvciB0aGUgY3VycmVudCBQQVRDSCByZXF1ZXN0XG4gICAgdGhpcy5feGhyID0gbnVsbDtcblxuICAgIC8vIFRoZSBmaW5nZXJwaW5ydCBmb3IgdGhlIGN1cnJlbnQgZmlsZSAoc2V0IGFmdGVyIHN0YXJ0KCkpXG4gICAgdGhpcy5fZmluZ2VycHJpbnQgPSBudWxsO1xuXG4gICAgLy8gVGhlIG9mZnNldCB1c2VkIGluIHRoZSBjdXJyZW50IFBBVENIIHJlcXVlc3RcbiAgICB0aGlzLl9vZmZzZXQgPSBudWxsO1xuXG4gICAgLy8gVHJ1ZSBpZiB0aGUgY3VycmVudCBQQVRDSCByZXF1ZXN0IGhhcyBiZWVuIGFib3J0ZWRcbiAgICB0aGlzLl9hYm9ydGVkID0gZmFsc2U7XG5cbiAgICAvLyBUaGUgZmlsZSdzIHNpemUgaW4gYnl0ZXNcbiAgICB0aGlzLl9zaXplID0gbnVsbDtcblxuICAgIC8vIFRoZSBTb3VyY2Ugb2JqZWN0IHdoaWNoIHdpbGwgd3JhcCBhcm91bmQgdGhlIGdpdmVuIGZpbGUgYW5kIHByb3ZpZGVzIHVzXG4gICAgLy8gd2l0aCBhIHVuaWZpZWQgaW50ZXJmYWNlIGZvciBnZXR0aW5nIGl0cyBzaXplIGFuZCBzbGljZSBjaHVua3MgZnJvbSBpdHNcbiAgICAvLyBjb250ZW50IGFsbG93aW5nIHVzIHRvIGVhc2lseSBoYW5kbGUgRmlsZXMsIEJsb2JzLCBCdWZmZXJzIGFuZCBTdHJlYW1zLlxuICAgIHRoaXMuX3NvdXJjZSA9IG51bGw7XG5cbiAgICAvLyBUaGUgY3VycmVudCBjb3VudCBvZiBhdHRlbXB0cyB3aGljaCBoYXZlIGJlZW4gbWFkZS4gTnVsbCBpbmRpY2F0ZXMgbm9uZS5cbiAgICB0aGlzLl9yZXRyeUF0dGVtcHQgPSAwO1xuXG4gICAgLy8gVGhlIHRpbWVvdXQncyBJRCB3aGljaCBpcyB1c2VkIHRvIGRlbGF5IHRoZSBuZXh0IHJldHJ5XG4gICAgdGhpcy5fcmV0cnlUaW1lb3V0ID0gbnVsbDtcblxuICAgIC8vIFRoZSBvZmZzZXQgb2YgdGhlIHJlbW90ZSB1cGxvYWQgYmVmb3JlIHRoZSBsYXRlc3QgYXR0ZW1wdCB3YXMgc3RhcnRlZC5cbiAgICB0aGlzLl9vZmZzZXRCZWZvcmVSZXRyeSA9IDA7XG4gIH1cblxuICBfY3JlYXRlQ2xhc3MoVXBsb2FkLCBbe1xuICAgIGtleTogXCJzdGFydFwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBzdGFydCgpIHtcbiAgICAgIHZhciBfdGhpcyA9IHRoaXM7XG5cbiAgICAgIHZhciBmaWxlID0gdGhpcy5maWxlO1xuXG4gICAgICBpZiAoIWZpbGUpIHtcbiAgICAgICAgdGhpcy5fZW1pdEVycm9yKG5ldyBFcnJvcihcInR1czogbm8gZmlsZSBvciBzdHJlYW0gdG8gdXBsb2FkIHByb3ZpZGVkXCIpKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICBpZiAoIXRoaXMub3B0aW9ucy5lbmRwb2ludCAmJiAhdGhpcy5vcHRpb25zLnVwbG9hZFVybCkge1xuICAgICAgICB0aGlzLl9lbWl0RXJyb3IobmV3IEVycm9yKFwidHVzOiBuZWl0aGVyIGFuIGVuZHBvaW50IG9yIGFuIHVwbG9hZCBVUkwgaXMgcHJvdmlkZWRcIikpO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIGlmICh0aGlzLm9wdGlvbnMucmVzdW1lICYmIHRoaXMuX3N0b3JhZ2UgPT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9zdG9yYWdlID0gKDAsIF9zdG9yYWdlLmdldFN0b3JhZ2UpKCk7XG4gICAgICB9XG5cbiAgICAgIGlmICh0aGlzLl9zb3VyY2UpIHtcbiAgICAgICAgdGhpcy5fc3RhcnQodGhpcy5fc291cmNlKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHZhciBmaWxlUmVhZGVyID0gdGhpcy5vcHRpb25zLmZpbGVSZWFkZXIgfHwgX3NvdXJjZS5nZXRTb3VyY2U7XG4gICAgICAgIGZpbGVSZWFkZXIoZmlsZSwgdGhpcy5vcHRpb25zLmNodW5rU2l6ZSwgZnVuY3Rpb24gKGVyciwgc291cmNlKSB7XG4gICAgICAgICAgaWYgKGVycikge1xuICAgICAgICAgICAgX3RoaXMuX2VtaXRFcnJvcihlcnIpO1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIF90aGlzLl9zb3VyY2UgPSBzb3VyY2U7XG4gICAgICAgICAgX3RoaXMuX3N0YXJ0KHNvdXJjZSk7XG4gICAgICAgIH0pO1xuICAgICAgfVxuICAgIH1cbiAgfSwge1xuICAgIGtleTogXCJfc3RhcnRcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gX3N0YXJ0KHNvdXJjZSkge1xuICAgICAgdmFyIF90aGlzMiA9IHRoaXM7XG5cbiAgICAgIHZhciBmaWxlID0gdGhpcy5maWxlO1xuXG4gICAgICAvLyBGaXJzdCwgd2UgbG9vayBhdCB0aGUgdXBsb2FkTGVuZ3RoRGVmZXJyZWQgb3B0aW9uLlxuICAgICAgLy8gTmV4dCwgd2UgY2hlY2sgaWYgdGhlIGNhbGxlciBoYXMgc3VwcGxpZWQgYSBtYW51YWwgdXBsb2FkIHNpemUuXG4gICAgICAvLyBGaW5hbGx5LCB3ZSB0cnkgdG8gdXNlIHRoZSBjYWxjdWxhdGVkIHNpemUgZnJvbSB0aGUgc291cmNlIG9iamVjdC5cbiAgICAgIGlmICh0aGlzLm9wdGlvbnMudXBsb2FkTGVuZ3RoRGVmZXJyZWQpIHtcbiAgICAgICAgdGhpcy5fc2l6ZSA9IG51bGw7XG4gICAgICB9IGVsc2UgaWYgKHRoaXMub3B0aW9ucy51cGxvYWRTaXplICE9IG51bGwpIHtcbiAgICAgICAgdGhpcy5fc2l6ZSA9ICt0aGlzLm9wdGlvbnMudXBsb2FkU2l6ZTtcbiAgICAgICAgaWYgKGlzTmFOKHRoaXMuX3NpemUpKSB7XG4gICAgICAgICAgdGhpcy5fZW1pdEVycm9yKG5ldyBFcnJvcihcInR1czogY2Fubm90IGNvbnZlcnQgYHVwbG9hZFNpemVgIG9wdGlvbiBpbnRvIGEgbnVtYmVyXCIpKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX3NpemUgPSBzb3VyY2Uuc2l6ZTtcbiAgICAgICAgaWYgKHRoaXMuX3NpemUgPT0gbnVsbCkge1xuICAgICAgICAgIHRoaXMuX2VtaXRFcnJvcihuZXcgRXJyb3IoXCJ0dXM6IGNhbm5vdCBhdXRvbWF0aWNhbGx5IGRlcml2ZSB1cGxvYWQncyBzaXplIGZyb20gaW5wdXQgYW5kIG11c3QgYmUgc3BlY2lmaWVkIG1hbnVhbGx5IHVzaW5nIHRoZSBgdXBsb2FkU2l6ZWAgb3B0aW9uXCIpKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgdmFyIHJldHJ5RGVsYXlzID0gdGhpcy5vcHRpb25zLnJldHJ5RGVsYXlzO1xuICAgICAgaWYgKHJldHJ5RGVsYXlzICE9IG51bGwpIHtcbiAgICAgICAgaWYgKE9iamVjdC5wcm90b3R5cGUudG9TdHJpbmcuY2FsbChyZXRyeURlbGF5cykgIT09IFwiW29iamVjdCBBcnJheV1cIikge1xuICAgICAgICAgIHRoaXMuX2VtaXRFcnJvcihuZXcgRXJyb3IoXCJ0dXM6IHRoZSBgcmV0cnlEZWxheXNgIG9wdGlvbiBtdXN0IGVpdGhlciBiZSBhbiBhcnJheSBvciBudWxsXCIpKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgdmFyIGVycm9yQ2FsbGJhY2sgPSB0aGlzLm9wdGlvbnMub25FcnJvcjtcbiAgICAgICAgICB0aGlzLm9wdGlvbnMub25FcnJvciA9IGZ1bmN0aW9uIChlcnIpIHtcbiAgICAgICAgICAgIC8vIFJlc3RvcmUgdGhlIG9yaWdpbmFsIGVycm9yIGNhbGxiYWNrIHdoaWNoIG1heSBoYXZlIGJlZW4gc2V0LlxuICAgICAgICAgICAgX3RoaXMyLm9wdGlvbnMub25FcnJvciA9IGVycm9yQ2FsbGJhY2s7XG5cbiAgICAgICAgICAgIC8vIFdlIHdpbGwgcmVzZXQgdGhlIGF0dGVtcHQgY291bnRlciBpZlxuICAgICAgICAgICAgLy8gLSB3ZSB3ZXJlIGFscmVhZHkgYWJsZSB0byBjb25uZWN0IHRvIHRoZSBzZXJ2ZXIgKG9mZnNldCAhPSBudWxsKSBhbmRcbiAgICAgICAgICAgIC8vIC0gd2Ugd2VyZSBhYmxlIHRvIHVwbG9hZCBhIHNtYWxsIGNodW5rIG9mIGRhdGEgdG8gdGhlIHNlcnZlclxuICAgICAgICAgICAgdmFyIHNob3VsZFJlc2V0RGVsYXlzID0gX3RoaXMyLl9vZmZzZXQgIT0gbnVsbCAmJiBfdGhpczIuX29mZnNldCA+IF90aGlzMi5fb2Zmc2V0QmVmb3JlUmV0cnk7XG4gICAgICAgICAgICBpZiAoc2hvdWxkUmVzZXREZWxheXMpIHtcbiAgICAgICAgICAgICAgX3RoaXMyLl9yZXRyeUF0dGVtcHQgPSAwO1xuICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICB2YXIgaXNPbmxpbmUgPSB0cnVlO1xuICAgICAgICAgICAgaWYgKHR5cGVvZiB3aW5kb3cgIT09IFwidW5kZWZpbmVkXCIgJiYgXCJuYXZpZ2F0b3JcIiBpbiB3aW5kb3cgJiYgd2luZG93Lm5hdmlnYXRvci5vbkxpbmUgPT09IGZhbHNlKSB7XG4gICAgICAgICAgICAgIGlzT25saW5lID0gZmFsc2U7XG4gICAgICAgICAgICB9XG5cbiAgICAgICAgICAgIC8vIFdlIG9ubHkgYXR0ZW1wdCBhIHJldHJ5IGlmXG4gICAgICAgICAgICAvLyAtIHdlIGRpZG4ndCBleGNlZWQgdGhlIG1heGl1bSBudW1iZXIgb2YgcmV0cmllcywgeWV0LCBhbmRcbiAgICAgICAgICAgIC8vIC0gdGhpcyBlcnJvciB3YXMgY2F1c2VkIGJ5IGEgcmVxdWVzdCBvciBpdCdzIHJlc3BvbnNlIGFuZFxuICAgICAgICAgICAgLy8gLSB0aGUgZXJyb3IgaXMgc2VydmVyIGVycm9yIChpLmUuIG5vIGEgc3RhdHVzIDR4eCBvciBhIDQwOSBvciA0MjMpIGFuZFxuICAgICAgICAgICAgLy8gLSB0aGUgYnJvd3NlciBkb2VzIG5vdCBpbmRpY2F0ZSB0aGF0IHdlIGFyZSBvZmZsaW5lXG4gICAgICAgICAgICB2YXIgc3RhdHVzID0gZXJyLm9yaWdpbmFsUmVxdWVzdCA/IGVyci5vcmlnaW5hbFJlcXVlc3Quc3RhdHVzIDogMDtcbiAgICAgICAgICAgIHZhciBpc1NlcnZlckVycm9yID0gIWluU3RhdHVzQ2F0ZWdvcnkoc3RhdHVzLCA0MDApIHx8IHN0YXR1cyA9PT0gNDA5IHx8IHN0YXR1cyA9PT0gNDIzO1xuICAgICAgICAgICAgdmFyIHNob3VsZFJldHJ5ID0gX3RoaXMyLl9yZXRyeUF0dGVtcHQgPCByZXRyeURlbGF5cy5sZW5ndGggJiYgZXJyLm9yaWdpbmFsUmVxdWVzdCAhPSBudWxsICYmIGlzU2VydmVyRXJyb3IgJiYgaXNPbmxpbmU7XG5cbiAgICAgICAgICAgIGlmICghc2hvdWxkUmV0cnkpIHtcbiAgICAgICAgICAgICAgX3RoaXMyLl9lbWl0RXJyb3IoZXJyKTtcbiAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICB2YXIgZGVsYXkgPSByZXRyeURlbGF5c1tfdGhpczIuX3JldHJ5QXR0ZW1wdCsrXTtcblxuICAgICAgICAgICAgX3RoaXMyLl9vZmZzZXRCZWZvcmVSZXRyeSA9IF90aGlzMi5fb2Zmc2V0O1xuICAgICAgICAgICAgX3RoaXMyLm9wdGlvbnMudXBsb2FkVXJsID0gX3RoaXMyLnVybDtcblxuICAgICAgICAgICAgX3RoaXMyLl9yZXRyeVRpbWVvdXQgPSBzZXRUaW1lb3V0KGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICAgICAgX3RoaXMyLnN0YXJ0KCk7XG4gICAgICAgICAgICB9LCBkZWxheSk7XG4gICAgICAgICAgfTtcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICAvLyBSZXNldCB0aGUgYWJvcnRlZCBmbGFnIHdoZW4gdGhlIHVwbG9hZCBpcyBzdGFydGVkIG9yIGVsc2UgdGhlXG4gICAgICAvLyBfc3RhcnRVcGxvYWQgd2lsbCBzdG9wIGJlZm9yZSBzZW5kaW5nIGEgcmVxdWVzdCBpZiB0aGUgdXBsb2FkIGhhcyBiZWVuXG4gICAgICAvLyBhYm9ydGVkIHByZXZpb3VzbHkuXG4gICAgICB0aGlzLl9hYm9ydGVkID0gZmFsc2U7XG5cbiAgICAgIC8vIFRoZSB1cGxvYWQgaGFkIGJlZW4gc3RhcnRlZCBwcmV2aW91c2x5IGFuZCB3ZSBzaG91bGQgcmV1c2UgdGhpcyBVUkwuXG4gICAgICBpZiAodGhpcy51cmwgIT0gbnVsbCkge1xuICAgICAgICB0aGlzLl9yZXN1bWVVcGxvYWQoKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICAvLyBBIFVSTCBoYXMgbWFudWFsbHkgYmVlbiBzcGVjaWZpZWQsIHNvIHdlIHRyeSB0byByZXN1bWVcbiAgICAgIGlmICh0aGlzLm9wdGlvbnMudXBsb2FkVXJsICE9IG51bGwpIHtcbiAgICAgICAgdGhpcy51cmwgPSB0aGlzLm9wdGlvbnMudXBsb2FkVXJsO1xuICAgICAgICB0aGlzLl9yZXN1bWVVcGxvYWQoKTtcbiAgICAgICAgcmV0dXJuO1xuICAgICAgfVxuXG4gICAgICAvLyBUcnkgdG8gZmluZCB0aGUgZW5kcG9pbnQgZm9yIHRoZSBmaWxlIGluIHRoZSBzdG9yYWdlXG4gICAgICBpZiAodGhpcy5faGFzU3RvcmFnZSgpKSB7XG4gICAgICAgIHRoaXMub3B0aW9ucy5maW5nZXJwcmludChmaWxlLCB0aGlzLm9wdGlvbnMsIGZ1bmN0aW9uIChlcnIsIGZpbmdlcnByaW50VmFsdWUpIHtcbiAgICAgICAgICBpZiAoZXJyKSB7XG4gICAgICAgICAgICBfdGhpczIuX2VtaXRFcnJvcihlcnIpO1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIF90aGlzMi5fZmluZ2VycHJpbnQgPSBmaW5nZXJwcmludFZhbHVlO1xuICAgICAgICAgIF90aGlzMi5fc3RvcmFnZS5nZXRJdGVtKF90aGlzMi5fZmluZ2VycHJpbnQsIGZ1bmN0aW9uIChlcnIsIHJlc3VtZWRVcmwpIHtcbiAgICAgICAgICAgIGlmIChlcnIpIHtcbiAgICAgICAgICAgICAgX3RoaXMyLl9lbWl0RXJyb3IoZXJyKTtcbiAgICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICBpZiAocmVzdW1lZFVybCAhPSBudWxsKSB7XG4gICAgICAgICAgICAgIF90aGlzMi51cmwgPSByZXN1bWVkVXJsO1xuICAgICAgICAgICAgICBfdGhpczIuX3Jlc3VtZVVwbG9hZCgpO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgX3RoaXMyLl9jcmVhdGVVcGxvYWQoKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICAvLyBBbiB1cGxvYWQgaGFzIG5vdCBzdGFydGVkIGZvciB0aGUgZmlsZSB5ZXQsIHNvIHdlIHN0YXJ0IGEgbmV3IG9uZVxuICAgICAgICB0aGlzLl9jcmVhdGVVcGxvYWQoKTtcbiAgICAgIH1cbiAgICB9XG4gIH0sIHtcbiAgICBrZXk6IFwiYWJvcnRcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gYWJvcnQoc2hvdWxkVGVybWluYXRlLCBjYikge1xuICAgICAgdmFyIF90aGlzMyA9IHRoaXM7XG5cbiAgICAgIGlmICh0aGlzLl94aHIgIT09IG51bGwpIHtcbiAgICAgICAgdGhpcy5feGhyLmFib3J0KCk7XG4gICAgICAgIHRoaXMuX3NvdXJjZS5jbG9zZSgpO1xuICAgICAgfVxuICAgICAgdGhpcy5fYWJvcnRlZCA9IHRydWU7XG5cbiAgICAgIGlmICh0aGlzLl9yZXRyeVRpbWVvdXQgIT0gbnVsbCkge1xuICAgICAgICBjbGVhclRpbWVvdXQodGhpcy5fcmV0cnlUaW1lb3V0KTtcbiAgICAgICAgdGhpcy5fcmV0cnlUaW1lb3V0ID0gbnVsbDtcbiAgICAgIH1cblxuICAgICAgY2IgPSBjYiB8fCBmdW5jdGlvbiAoKSB7fTtcbiAgICAgIGlmIChzaG91bGRUZXJtaW5hdGUpIHtcbiAgICAgICAgVXBsb2FkLnRlcm1pbmF0ZSh0aGlzLnVybCwgdGhpcy5vcHRpb25zLCBmdW5jdGlvbiAoZXJyLCB4aHIpIHtcbiAgICAgICAgICBpZiAoZXJyKSB7XG4gICAgICAgICAgICByZXR1cm4gY2IoZXJyLCB4aHIpO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIF90aGlzMy5faGFzU3RvcmFnZSgpID8gX3RoaXMzLl9zdG9yYWdlLnJlbW92ZUl0ZW0oX3RoaXMzLl9maW5nZXJwcmludCwgY2IpIDogY2IoKTtcbiAgICAgICAgfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjYigpO1xuICAgICAgfVxuICAgIH1cbiAgfSwge1xuICAgIGtleTogXCJfaGFzU3RvcmFnZVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfaGFzU3RvcmFnZSgpIHtcbiAgICAgIHJldHVybiB0aGlzLm9wdGlvbnMucmVzdW1lICYmIHRoaXMuX3N0b3JhZ2U7XG4gICAgfVxuICB9LCB7XG4gICAga2V5OiBcIl9lbWl0WGhyRXJyb3JcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gX2VtaXRYaHJFcnJvcih4aHIsIGVyciwgY2F1c2luZ0Vycikge1xuICAgICAgdGhpcy5fZW1pdEVycm9yKG5ldyBfZXJyb3IyLmRlZmF1bHQoZXJyLCBjYXVzaW5nRXJyLCB4aHIpKTtcbiAgICB9XG4gIH0sIHtcbiAgICBrZXk6IFwiX2VtaXRFcnJvclwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfZW1pdEVycm9yKGVycikge1xuICAgICAgaWYgKHR5cGVvZiB0aGlzLm9wdGlvbnMub25FcnJvciA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgIHRoaXMub3B0aW9ucy5vbkVycm9yKGVycik7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBlcnI7XG4gICAgICB9XG4gICAgfVxuICB9LCB7XG4gICAga2V5OiBcIl9lbWl0U3VjY2Vzc1wiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfZW1pdFN1Y2Nlc3MoKSB7XG4gICAgICBpZiAodHlwZW9mIHRoaXMub3B0aW9ucy5vblN1Y2Nlc3MgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICB0aGlzLm9wdGlvbnMub25TdWNjZXNzKCk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLyoqXG4gICAgICogUHVibGlzaGVzIG5vdGlmaWNhdGlvbiB3aGVuIGRhdGEgaGFzIGJlZW4gc2VudCB0byB0aGUgc2VydmVyLiBUaGlzXG4gICAgICogZGF0YSBtYXkgbm90IGhhdmUgYmVlbiBhY2NlcHRlZCBieSB0aGUgc2VydmVyIHlldC5cbiAgICAgKiBAcGFyYW0gIHtudW1iZXJ9IGJ5dGVzU2VudCAgTnVtYmVyIG9mIGJ5dGVzIHNlbnQgdG8gdGhlIHNlcnZlci5cbiAgICAgKiBAcGFyYW0gIHtudW1iZXJ9IGJ5dGVzVG90YWwgVG90YWwgbnVtYmVyIG9mIGJ5dGVzIHRvIGJlIHNlbnQgdG8gdGhlIHNlcnZlci5cbiAgICAgKi9cblxuICB9LCB7XG4gICAga2V5OiBcIl9lbWl0UHJvZ3Jlc3NcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gX2VtaXRQcm9ncmVzcyhieXRlc1NlbnQsIGJ5dGVzVG90YWwpIHtcbiAgICAgIGlmICh0eXBlb2YgdGhpcy5vcHRpb25zLm9uUHJvZ3Jlc3MgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICB0aGlzLm9wdGlvbnMub25Qcm9ncmVzcyhieXRlc1NlbnQsIGJ5dGVzVG90YWwpO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8qKlxuICAgICAqIFB1Ymxpc2hlcyBub3RpZmljYXRpb24gd2hlbiBhIGNodW5rIG9mIGRhdGEgaGFzIGJlZW4gc2VudCB0byB0aGUgc2VydmVyXG4gICAgICogYW5kIGFjY2VwdGVkIGJ5IHRoZSBzZXJ2ZXIuXG4gICAgICogQHBhcmFtICB7bnVtYmVyfSBjaHVua1NpemUgIFNpemUgb2YgdGhlIGNodW5rIHRoYXQgd2FzIGFjY2VwdGVkIGJ5IHRoZVxuICAgICAqICAgICAgICAgICAgICAgICAgICAgICAgICAgICBzZXJ2ZXIuXG4gICAgICogQHBhcmFtICB7bnVtYmVyfSBieXRlc0FjY2VwdGVkIFRvdGFsIG51bWJlciBvZiBieXRlcyB0aGF0IGhhdmUgYmVlblxuICAgICAqICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBhY2NlcHRlZCBieSB0aGUgc2VydmVyLlxuICAgICAqIEBwYXJhbSAge251bWJlcn0gYnl0ZXNUb3RhbCBUb3RhbCBudW1iZXIgb2YgYnl0ZXMgdG8gYmUgc2VudCB0byB0aGUgc2VydmVyLlxuICAgICAqL1xuXG4gIH0sIHtcbiAgICBrZXk6IFwiX2VtaXRDaHVua0NvbXBsZXRlXCIsXG4gICAgdmFsdWU6IGZ1bmN0aW9uIF9lbWl0Q2h1bmtDb21wbGV0ZShjaHVua1NpemUsIGJ5dGVzQWNjZXB0ZWQsIGJ5dGVzVG90YWwpIHtcbiAgICAgIGlmICh0eXBlb2YgdGhpcy5vcHRpb25zLm9uQ2h1bmtDb21wbGV0ZSA9PT0gXCJmdW5jdGlvblwiKSB7XG4gICAgICAgIHRoaXMub3B0aW9ucy5vbkNodW5rQ29tcGxldGUoY2h1bmtTaXplLCBieXRlc0FjY2VwdGVkLCBieXRlc1RvdGFsKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvKipcbiAgICAgKiBTZXQgdGhlIGhlYWRlcnMgdXNlZCBpbiB0aGUgcmVxdWVzdCBhbmQgdGhlIHdpdGhDcmVkZW50aWFscyBwcm9wZXJ0eVxuICAgICAqIGFzIGRlZmluZWQgaW4gdGhlIG9wdGlvbnNcbiAgICAgKlxuICAgICAqIEBwYXJhbSB7WE1MSHR0cFJlcXVlc3R9IHhoclxuICAgICAqL1xuXG4gIH0sIHtcbiAgICBrZXk6IFwiX3NldHVwWEhSXCIsXG4gICAgdmFsdWU6IGZ1bmN0aW9uIF9zZXR1cFhIUih4aHIpIHtcbiAgICAgIHRoaXMuX3hociA9IHhocjtcbiAgICAgIHNldHVwWEhSKHhociwgdGhpcy5vcHRpb25zKTtcbiAgICB9XG5cbiAgICAvKipcbiAgICAgKiBDcmVhdGUgYSBuZXcgdXBsb2FkIHVzaW5nIHRoZSBjcmVhdGlvbiBleHRlbnNpb24gYnkgc2VuZGluZyBhIFBPU1RcbiAgICAgKiByZXF1ZXN0IHRvIHRoZSBlbmRwb2ludC4gQWZ0ZXIgc3VjY2Vzc2Z1bCBjcmVhdGlvbiB0aGUgZmlsZSB3aWxsIGJlXG4gICAgICogdXBsb2FkZWRcbiAgICAgKlxuICAgICAqIEBhcGkgcHJpdmF0ZVxuICAgICAqL1xuXG4gIH0sIHtcbiAgICBrZXk6IFwiX2NyZWF0ZVVwbG9hZFwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiBfY3JlYXRlVXBsb2FkKCkge1xuICAgICAgdmFyIF90aGlzNCA9IHRoaXM7XG5cbiAgICAgIGlmICghdGhpcy5vcHRpb25zLmVuZHBvaW50KSB7XG4gICAgICAgIHRoaXMuX2VtaXRFcnJvcihuZXcgRXJyb3IoXCJ0dXM6IHVuYWJsZSB0byBjcmVhdGUgdXBsb2FkIGJlY2F1c2Ugbm8gZW5kcG9pbnQgaXMgcHJvdmlkZWRcIikpO1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIHZhciB4aHIgPSAoMCwgX3JlcXVlc3QubmV3UmVxdWVzdCkoKTtcbiAgICAgIHhoci5vcGVuKFwiUE9TVFwiLCB0aGlzLm9wdGlvbnMuZW5kcG9pbnQsIHRydWUpO1xuXG4gICAgICB4aHIub25sb2FkID0gZnVuY3Rpb24gKCkge1xuICAgICAgICBpZiAoIWluU3RhdHVzQ2F0ZWdvcnkoeGhyLnN0YXR1cywgMjAwKSkge1xuICAgICAgICAgIF90aGlzNC5fZW1pdFhockVycm9yKHhociwgbmV3IEVycm9yKFwidHVzOiB1bmV4cGVjdGVkIHJlc3BvbnNlIHdoaWxlIGNyZWF0aW5nIHVwbG9hZFwiKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgdmFyIGxvY2F0aW9uID0geGhyLmdldFJlc3BvbnNlSGVhZGVyKFwiTG9jYXRpb25cIik7XG4gICAgICAgIGlmIChsb2NhdGlvbiA9PSBudWxsKSB7XG4gICAgICAgICAgX3RoaXM0Ll9lbWl0WGhyRXJyb3IoeGhyLCBuZXcgRXJyb3IoXCJ0dXM6IGludmFsaWQgb3IgbWlzc2luZyBMb2NhdGlvbiBoZWFkZXJcIikpO1xuICAgICAgICAgIHJldHVybjtcbiAgICAgICAgfVxuXG4gICAgICAgIF90aGlzNC51cmwgPSAoMCwgX3JlcXVlc3QucmVzb2x2ZVVybCkoX3RoaXM0Lm9wdGlvbnMuZW5kcG9pbnQsIGxvY2F0aW9uKTtcblxuICAgICAgICBpZiAoX3RoaXM0Ll9zaXplID09PSAwKSB7XG4gICAgICAgICAgLy8gTm90aGluZyB0byB1cGxvYWQgYW5kIGZpbGUgd2FzIHN1Y2Nlc3NmdWxseSBjcmVhdGVkXG4gICAgICAgICAgX3RoaXM0Ll9lbWl0U3VjY2VzcygpO1xuICAgICAgICAgIF90aGlzNC5fc291cmNlLmNsb3NlKCk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKF90aGlzNC5faGFzU3RvcmFnZSgpKSB7XG4gICAgICAgICAgX3RoaXM0Ll9zdG9yYWdlLnNldEl0ZW0oX3RoaXM0Ll9maW5nZXJwcmludCwgX3RoaXM0LnVybCwgZnVuY3Rpb24gKGVycikge1xuICAgICAgICAgICAgaWYgKGVycikge1xuICAgICAgICAgICAgICBfdGhpczQuX2VtaXRFcnJvcihlcnIpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG5cbiAgICAgICAgX3RoaXM0Ll9vZmZzZXQgPSAwO1xuICAgICAgICBfdGhpczQuX3N0YXJ0VXBsb2FkKCk7XG4gICAgICB9O1xuXG4gICAgICB4aHIub25lcnJvciA9IGZ1bmN0aW9uIChlcnIpIHtcbiAgICAgICAgX3RoaXM0Ll9lbWl0WGhyRXJyb3IoeGhyLCBuZXcgRXJyb3IoXCJ0dXM6IGZhaWxlZCB0byBjcmVhdGUgdXBsb2FkXCIpLCBlcnIpO1xuICAgICAgfTtcblxuICAgICAgdGhpcy5fc2V0dXBYSFIoeGhyKTtcbiAgICAgIGlmICh0aGlzLm9wdGlvbnMudXBsb2FkTGVuZ3RoRGVmZXJyZWQpIHtcbiAgICAgICAgeGhyLnNldFJlcXVlc3RIZWFkZXIoXCJVcGxvYWQtRGVmZXItTGVuZ3RoXCIsIDEpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgeGhyLnNldFJlcXVlc3RIZWFkZXIoXCJVcGxvYWQtTGVuZ3RoXCIsIHRoaXMuX3NpemUpO1xuICAgICAgfVxuXG4gICAgICAvLyBBZGQgbWV0YWRhdGEgaWYgdmFsdWVzIGhhdmUgYmVlbiBhZGRlZFxuICAgICAgdmFyIG1ldGFkYXRhID0gZW5jb2RlTWV0YWRhdGEodGhpcy5vcHRpb25zLm1ldGFkYXRhKTtcbiAgICAgIGlmIChtZXRhZGF0YSAhPT0gXCJcIikge1xuICAgICAgICB4aHIuc2V0UmVxdWVzdEhlYWRlcihcIlVwbG9hZC1NZXRhZGF0YVwiLCBtZXRhZGF0YSk7XG4gICAgICB9XG5cbiAgICAgIHhoci5zZW5kKG51bGwpO1xuICAgIH1cblxuICAgIC8qXG4gICAgICogVHJ5IHRvIHJlc3VtZSBhbiBleGlzdGluZyB1cGxvYWQuIEZpcnN0IGEgSEVBRCByZXF1ZXN0IHdpbGwgYmUgc2VudFxuICAgICAqIHRvIHJldHJpZXZlIHRoZSBvZmZzZXQuIElmIHRoZSByZXF1ZXN0IGZhaWxzIGEgbmV3IHVwbG9hZCB3aWxsIGJlXG4gICAgICogY3JlYXRlZC4gSW4gdGhlIGNhc2Ugb2YgYSBzdWNjZXNzZnVsIHJlc3BvbnNlIHRoZSBmaWxlIHdpbGwgYmUgdXBsb2FkZWQuXG4gICAgICpcbiAgICAgKiBAYXBpIHByaXZhdGVcbiAgICAgKi9cblxuICB9LCB7XG4gICAga2V5OiBcIl9yZXN1bWVVcGxvYWRcIixcbiAgICB2YWx1ZTogZnVuY3Rpb24gX3Jlc3VtZVVwbG9hZCgpIHtcbiAgICAgIHZhciBfdGhpczUgPSB0aGlzO1xuXG4gICAgICB2YXIgeGhyID0gKDAsIF9yZXF1ZXN0Lm5ld1JlcXVlc3QpKCk7XG4gICAgICB4aHIub3BlbihcIkhFQURcIiwgdGhpcy51cmwsIHRydWUpO1xuXG4gICAgICB4aHIub25sb2FkID0gZnVuY3Rpb24gKCkge1xuICAgICAgICBpZiAoIWluU3RhdHVzQ2F0ZWdvcnkoeGhyLnN0YXR1cywgMjAwKSkge1xuICAgICAgICAgIGlmIChfdGhpczUuX2hhc1N0b3JhZ2UoKSAmJiBpblN0YXR1c0NhdGVnb3J5KHhoci5zdGF0dXMsIDQwMCkpIHtcbiAgICAgICAgICAgIC8vIFJlbW92ZSBzdG9yZWQgZmluZ2VycHJpbnQgYW5kIGNvcnJlc3BvbmRpbmcgZW5kcG9pbnQsXG4gICAgICAgICAgICAvLyBvbiBjbGllbnQgZXJyb3JzIHNpbmNlIHRoZSBmaWxlIGNhbiBub3QgYmUgZm91bmRcbiAgICAgICAgICAgIF90aGlzNS5fc3RvcmFnZS5yZW1vdmVJdGVtKF90aGlzNS5fZmluZ2VycHJpbnQsIGZ1bmN0aW9uIChlcnIpIHtcbiAgICAgICAgICAgICAgaWYgKGVycikge1xuICAgICAgICAgICAgICAgIF90aGlzNS5fZW1pdEVycm9yKGVycik7XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIC8vIElmIHRoZSB1cGxvYWQgaXMgbG9ja2VkIChpbmRpY2F0ZWQgYnkgdGhlIDQyMyBMb2NrZWQgc3RhdHVzIGNvZGUpLCB3ZVxuICAgICAgICAgIC8vIGVtaXQgYW4gZXJyb3IgaW5zdGVhZCBvZiBkaXJlY3RseSBzdGFydGluZyBhIG5ldyB1cGxvYWQuIFRoaXMgd2F5IHRoZVxuICAgICAgICAgIC8vIHJldHJ5IGxvZ2ljIGNhbiBjYXRjaCB0aGUgZXJyb3IgYW5kIHdpbGwgcmV0cnkgdGhlIHVwbG9hZC4gQW4gdXBsb2FkXG4gICAgICAgICAgLy8gaXMgdXN1YWxseSBsb2NrZWQgZm9yIGEgc2hvcnQgcGVyaW9kIG9mIHRpbWUgYW5kIHdpbGwgYmUgYXZhaWxhYmxlXG4gICAgICAgICAgLy8gYWZ0ZXJ3YXJkcy5cbiAgICAgICAgICBpZiAoeGhyLnN0YXR1cyA9PT0gNDIzKSB7XG4gICAgICAgICAgICBfdGhpczUuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogdXBsb2FkIGlzIGN1cnJlbnRseSBsb2NrZWQ7IHJldHJ5IGxhdGVyXCIpKTtcbiAgICAgICAgICAgIHJldHVybjtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBpZiAoIV90aGlzNS5vcHRpb25zLmVuZHBvaW50KSB7XG4gICAgICAgICAgICAvLyBEb24ndCBhdHRlbXB0IHRvIGNyZWF0ZSBhIG5ldyB1cGxvYWQgaWYgbm8gZW5kcG9pbnQgaXMgcHJvdmlkZWQuXG4gICAgICAgICAgICBfdGhpczUuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogdW5hYmxlIHRvIHJlc3VtZSB1cGxvYWQgKG5ldyB1cGxvYWQgY2Fubm90IGJlIGNyZWF0ZWQgd2l0aG91dCBhbiBlbmRwb2ludClcIikpO1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIC8vIFRyeSB0byBjcmVhdGUgYSBuZXcgdXBsb2FkXG4gICAgICAgICAgX3RoaXM1LnVybCA9IG51bGw7XG4gICAgICAgICAgX3RoaXM1Ll9jcmVhdGVVcGxvYWQoKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cblxuICAgICAgICB2YXIgb2Zmc2V0ID0gcGFyc2VJbnQoeGhyLmdldFJlc3BvbnNlSGVhZGVyKFwiVXBsb2FkLU9mZnNldFwiKSwgMTApO1xuICAgICAgICBpZiAoaXNOYU4ob2Zmc2V0KSkge1xuICAgICAgICAgIF90aGlzNS5fZW1pdFhockVycm9yKHhociwgbmV3IEVycm9yKFwidHVzOiBpbnZhbGlkIG9yIG1pc3Npbmcgb2Zmc2V0IHZhbHVlXCIpKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cblxuICAgICAgICB2YXIgbGVuZ3RoID0gcGFyc2VJbnQoeGhyLmdldFJlc3BvbnNlSGVhZGVyKFwiVXBsb2FkLUxlbmd0aFwiKSwgMTApO1xuICAgICAgICBpZiAoaXNOYU4obGVuZ3RoKSAmJiAhX3RoaXM1Lm9wdGlvbnMudXBsb2FkTGVuZ3RoRGVmZXJyZWQpIHtcbiAgICAgICAgICBfdGhpczUuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogaW52YWxpZCBvciBtaXNzaW5nIGxlbmd0aCB2YWx1ZVwiKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgLy8gVXBsb2FkIGhhcyBhbHJlYWR5IGJlZW4gY29tcGxldGVkIGFuZCB3ZSBkbyBub3QgbmVlZCB0byBzZW5kIGFkZGl0aW9uYWxcbiAgICAgICAgLy8gZGF0YSB0byB0aGUgc2VydmVyXG4gICAgICAgIGlmIChvZmZzZXQgPT09IGxlbmd0aCkge1xuICAgICAgICAgIF90aGlzNS5fZW1pdFByb2dyZXNzKGxlbmd0aCwgbGVuZ3RoKTtcbiAgICAgICAgICBfdGhpczUuX2VtaXRTdWNjZXNzKCk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgX3RoaXM1Ll9vZmZzZXQgPSBvZmZzZXQ7XG4gICAgICAgIF90aGlzNS5fc3RhcnRVcGxvYWQoKTtcbiAgICAgIH07XG5cbiAgICAgIHhoci5vbmVycm9yID0gZnVuY3Rpb24gKGVycikge1xuICAgICAgICBfdGhpczUuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogZmFpbGVkIHRvIHJlc3VtZSB1cGxvYWRcIiksIGVycik7XG4gICAgICB9O1xuXG4gICAgICB0aGlzLl9zZXR1cFhIUih4aHIpO1xuICAgICAgeGhyLnNlbmQobnVsbCk7XG4gICAgfVxuXG4gICAgLyoqXG4gICAgICogU3RhcnQgdXBsb2FkaW5nIHRoZSBmaWxlIHVzaW5nIFBBVENIIHJlcXVlc3RzLiBUaGUgZmlsZSB3aWxsIGJlIGRpdmlkZWRcbiAgICAgKiBpbnRvIGNodW5rcyBhcyBzcGVjaWZpZWQgaW4gdGhlIGNodW5rU2l6ZSBvcHRpb24uIER1cmluZyB0aGUgdXBsb2FkXG4gICAgICogdGhlIG9uUHJvZ3Jlc3MgZXZlbnQgaGFuZGxlciBtYXkgYmUgaW52b2tlZCBtdWx0aXBsZSB0aW1lcy5cbiAgICAgKlxuICAgICAqIEBhcGkgcHJpdmF0ZVxuICAgICAqL1xuXG4gIH0sIHtcbiAgICBrZXk6IFwiX3N0YXJ0VXBsb2FkXCIsXG4gICAgdmFsdWU6IGZ1bmN0aW9uIF9zdGFydFVwbG9hZCgpIHtcbiAgICAgIHZhciBfdGhpczYgPSB0aGlzO1xuXG4gICAgICAvLyBJZiB0aGUgdXBsb2FkIGhhcyBiZWVuIGFib3J0ZWQsIHdlIHdpbGwgbm90IHNlbmQgdGhlIG5leHQgUEFUQ0ggcmVxdWVzdC5cbiAgICAgIC8vIFRoaXMgaXMgaW1wb3J0YW50IGlmIHRoZSBhYm9ydCBtZXRob2Qgd2FzIGNhbGxlZCBkdXJpbmcgYSBjYWxsYmFjaywgc3VjaFxuICAgICAgLy8gYXMgb25DaHVua0NvbXBsZXRlIG9yIG9uUHJvZ3Jlc3MuXG4gICAgICBpZiAodGhpcy5fYWJvcnRlZCkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG5cbiAgICAgIHZhciB4aHIgPSAoMCwgX3JlcXVlc3QubmV3UmVxdWVzdCkoKTtcblxuICAgICAgLy8gU29tZSBicm93c2VyIGFuZCBzZXJ2ZXJzIG1heSBub3Qgc3VwcG9ydCB0aGUgUEFUQ0ggbWV0aG9kLiBGb3IgdGhvc2VcbiAgICAgIC8vIGNhc2VzLCB5b3UgY2FuIHRlbGwgdHVzLWpzLWNsaWVudCB0byB1c2UgYSBQT1NUIHJlcXVlc3Qgd2l0aCB0aGVcbiAgICAgIC8vIFgtSFRUUC1NZXRob2QtT3ZlcnJpZGUgaGVhZGVyIGZvciBzaW11bGF0aW5nIGEgUEFUQ0ggcmVxdWVzdC5cbiAgICAgIGlmICh0aGlzLm9wdGlvbnMub3ZlcnJpZGVQYXRjaE1ldGhvZCkge1xuICAgICAgICB4aHIub3BlbihcIlBPU1RcIiwgdGhpcy51cmwsIHRydWUpO1xuICAgICAgICB4aHIuc2V0UmVxdWVzdEhlYWRlcihcIlgtSFRUUC1NZXRob2QtT3ZlcnJpZGVcIiwgXCJQQVRDSFwiKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHhoci5vcGVuKFwiUEFUQ0hcIiwgdGhpcy51cmwsIHRydWUpO1xuICAgICAgfVxuXG4gICAgICB4aHIub25sb2FkID0gZnVuY3Rpb24gKCkge1xuICAgICAgICBpZiAoIWluU3RhdHVzQ2F0ZWdvcnkoeGhyLnN0YXR1cywgMjAwKSkge1xuICAgICAgICAgIF90aGlzNi5fZW1pdFhockVycm9yKHhociwgbmV3IEVycm9yKFwidHVzOiB1bmV4cGVjdGVkIHJlc3BvbnNlIHdoaWxlIHVwbG9hZGluZyBjaHVua1wiKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgdmFyIG9mZnNldCA9IHBhcnNlSW50KHhoci5nZXRSZXNwb25zZUhlYWRlcihcIlVwbG9hZC1PZmZzZXRcIiksIDEwKTtcbiAgICAgICAgaWYgKGlzTmFOKG9mZnNldCkpIHtcbiAgICAgICAgICBfdGhpczYuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogaW52YWxpZCBvciBtaXNzaW5nIG9mZnNldCB2YWx1ZVwiKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgX3RoaXM2Ll9lbWl0UHJvZ3Jlc3Mob2Zmc2V0LCBfdGhpczYuX3NpemUpO1xuICAgICAgICBfdGhpczYuX2VtaXRDaHVua0NvbXBsZXRlKG9mZnNldCAtIF90aGlzNi5fb2Zmc2V0LCBvZmZzZXQsIF90aGlzNi5fc2l6ZSk7XG5cbiAgICAgICAgX3RoaXM2Ll9vZmZzZXQgPSBvZmZzZXQ7XG5cbiAgICAgICAgaWYgKG9mZnNldCA9PSBfdGhpczYuX3NpemUpIHtcbiAgICAgICAgICBpZiAoX3RoaXM2Lm9wdGlvbnMucmVtb3ZlRmluZ2VycHJpbnRPblN1Y2Nlc3MgJiYgX3RoaXM2Lm9wdGlvbnMucmVzdW1lKSB7XG4gICAgICAgICAgICAvLyBSZW1vdmUgc3RvcmVkIGZpbmdlcnByaW50IGFuZCBjb3JyZXNwb25kaW5nIGVuZHBvaW50LiBUaGlzIGNhdXNlc1xuICAgICAgICAgICAgLy8gbmV3IHVwbG9hZCBvZiB0aGUgc2FtZSBmaWxlIG11c3QgYmUgdHJlYXRlZCBhcyBhIGRpZmZlcmVudCBmaWxlLlxuICAgICAgICAgICAgX3RoaXM2Ll9zdG9yYWdlLnJlbW92ZUl0ZW0oX3RoaXM2Ll9maW5nZXJwcmludCwgZnVuY3Rpb24gKGVycikge1xuICAgICAgICAgICAgICBpZiAoZXJyKSB7XG4gICAgICAgICAgICAgICAgX3RoaXM2Ll9lbWl0RXJyb3IoZXJyKTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuXG4gICAgICAgICAgLy8gWWF5LCBmaW5hbGx5IGRvbmUgOilcbiAgICAgICAgICBfdGhpczYuX2VtaXRTdWNjZXNzKCk7XG4gICAgICAgICAgX3RoaXM2Ll9zb3VyY2UuY2xvc2UoKTtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cblxuICAgICAgICBfdGhpczYuX3N0YXJ0VXBsb2FkKCk7XG4gICAgICB9O1xuXG4gICAgICB4aHIub25lcnJvciA9IGZ1bmN0aW9uIChlcnIpIHtcbiAgICAgICAgLy8gRG9uJ3QgZW1pdCBhbiBlcnJvciBpZiB0aGUgdXBsb2FkIHdhcyBhYm9ydGVkIG1hbnVhbGx5XG4gICAgICAgIGlmIChfdGhpczYuX2Fib3J0ZWQpIHtcbiAgICAgICAgICByZXR1cm47XG4gICAgICAgIH1cblxuICAgICAgICBfdGhpczYuX2VtaXRYaHJFcnJvcih4aHIsIG5ldyBFcnJvcihcInR1czogZmFpbGVkIHRvIHVwbG9hZCBjaHVuayBhdCBvZmZzZXQgXCIgKyBfdGhpczYuX29mZnNldCksIGVycik7XG4gICAgICB9O1xuXG4gICAgICAvLyBUZXN0IHN1cHBvcnQgZm9yIHByb2dyZXNzIGV2ZW50cyBiZWZvcmUgYXR0YWNoaW5nIGFuIGV2ZW50IGxpc3RlbmVyXG4gICAgICBpZiAoXCJ1cGxvYWRcIiBpbiB4aHIpIHtcbiAgICAgICAgeGhyLnVwbG9hZC5vbnByb2dyZXNzID0gZnVuY3Rpb24gKGUpIHtcbiAgICAgICAgICBpZiAoIWUubGVuZ3RoQ29tcHV0YWJsZSkge1xuICAgICAgICAgICAgcmV0dXJuO1xuICAgICAgICAgIH1cblxuICAgICAgICAgIF90aGlzNi5fZW1pdFByb2dyZXNzKHN0YXJ0ICsgZS5sb2FkZWQsIF90aGlzNi5fc2l6ZSk7XG4gICAgICAgIH07XG4gICAgICB9XG5cbiAgICAgIHRoaXMuX3NldHVwWEhSKHhocik7XG5cbiAgICAgIHhoci5zZXRSZXF1ZXN0SGVhZGVyKFwiVXBsb2FkLU9mZnNldFwiLCB0aGlzLl9vZmZzZXQpO1xuICAgICAgeGhyLnNldFJlcXVlc3RIZWFkZXIoXCJDb250ZW50LVR5cGVcIiwgXCJhcHBsaWNhdGlvbi9vZmZzZXQrb2N0ZXQtc3RyZWFtXCIpO1xuXG4gICAgICB2YXIgc3RhcnQgPSB0aGlzLl9vZmZzZXQ7XG4gICAgICB2YXIgZW5kID0gdGhpcy5fb2Zmc2V0ICsgdGhpcy5vcHRpb25zLmNodW5rU2l6ZTtcblxuICAgICAgLy8gVGhlIHNwZWNpZmllZCBjaHVua1NpemUgbWF5IGJlIEluZmluaXR5IG9yIHRoZSBjYWxjbHVhdGVkIGVuZCBwb3NpdGlvblxuICAgICAgLy8gbWF5IGV4Y2VlZCB0aGUgZmlsZSdzIHNpemUuIEluIGJvdGggY2FzZXMsIHdlIGxpbWl0IHRoZSBlbmQgcG9zaXRpb24gdG9cbiAgICAgIC8vIHRoZSBpbnB1dCdzIHRvdGFsIHNpemUgZm9yIHNpbXBsZXIgY2FsY3VsYXRpb25zIGFuZCBjb3JyZWN0bmVzcy5cbiAgICAgIGlmICgoZW5kID09PSBJbmZpbml0eSB8fCBlbmQgPiB0aGlzLl9zaXplKSAmJiAhdGhpcy5vcHRpb25zLnVwbG9hZExlbmd0aERlZmVycmVkKSB7XG4gICAgICAgIGVuZCA9IHRoaXMuX3NpemU7XG4gICAgICB9XG5cbiAgICAgIHRoaXMuX3NvdXJjZS5zbGljZShzdGFydCwgZW5kLCBmdW5jdGlvbiAoZXJyLCB2YWx1ZSwgY29tcGxldGUpIHtcbiAgICAgICAgaWYgKGVycikge1xuICAgICAgICAgIF90aGlzNi5fZW1pdEVycm9yKGVycik7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgaWYgKF90aGlzNi5vcHRpb25zLnVwbG9hZExlbmd0aERlZmVycmVkKSB7XG4gICAgICAgICAgaWYgKGNvbXBsZXRlKSB7XG4gICAgICAgICAgICBfdGhpczYuX3NpemUgPSBfdGhpczYuX29mZnNldCArICh2YWx1ZSAmJiB2YWx1ZS5zaXplID8gdmFsdWUuc2l6ZSA6IDApO1xuICAgICAgICAgICAgeGhyLnNldFJlcXVlc3RIZWFkZXIoXCJVcGxvYWQtTGVuZ3RoXCIsIF90aGlzNi5fc2l6ZSk7XG4gICAgICAgICAgfVxuICAgICAgICB9XG5cbiAgICAgICAgaWYgKHZhbHVlID09PSBudWxsKSB7XG4gICAgICAgICAgeGhyLnNlbmQoKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICB4aHIuc2VuZCh2YWx1ZSk7XG4gICAgICAgICAgX3RoaXM2Ll9lbWl0UHJvZ3Jlc3MoX3RoaXM2Ll9vZmZzZXQsIF90aGlzNi5fc2l6ZSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfV0sIFt7XG4gICAga2V5OiBcInRlcm1pbmF0ZVwiLFxuICAgIHZhbHVlOiBmdW5jdGlvbiB0ZXJtaW5hdGUodXJsLCBvcHRpb25zLCBjYikge1xuICAgICAgaWYgKHR5cGVvZiBvcHRpb25zICE9PSBcImZ1bmN0aW9uXCIgJiYgdHlwZW9mIGNiICE9PSBcImZ1bmN0aW9uXCIpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKFwidHVzOiBhIGNhbGxiYWNrIGZ1bmN0aW9uIG11c3QgYmUgc3BlY2lmaWVkXCIpO1xuICAgICAgfVxuXG4gICAgICBpZiAodHlwZW9mIG9wdGlvbnMgPT09IFwiZnVuY3Rpb25cIikge1xuICAgICAgICBjYiA9IG9wdGlvbnM7XG4gICAgICAgIG9wdGlvbnMgPSB7fTtcbiAgICAgIH1cblxuICAgICAgdmFyIHhociA9ICgwLCBfcmVxdWVzdC5uZXdSZXF1ZXN0KSgpO1xuICAgICAgeGhyLm9wZW4oXCJERUxFVEVcIiwgdXJsLCB0cnVlKTtcblxuICAgICAgeGhyLm9ubG9hZCA9IGZ1bmN0aW9uICgpIHtcbiAgICAgICAgaWYgKHhoci5zdGF0dXMgIT09IDIwNCkge1xuICAgICAgICAgIGNiKG5ldyBfZXJyb3IyLmRlZmF1bHQobmV3IEVycm9yKFwidHVzOiB1bmV4cGVjdGVkIHJlc3BvbnNlIHdoaWxlIHRlcm1pbmF0aW5nIHVwbG9hZFwiKSwgbnVsbCwgeGhyKSk7XG4gICAgICAgICAgcmV0dXJuO1xuICAgICAgICB9XG5cbiAgICAgICAgY2IoKTtcbiAgICAgIH07XG5cbiAgICAgIHhoci5vbmVycm9yID0gZnVuY3Rpb24gKGVycikge1xuICAgICAgICBjYihuZXcgX2Vycm9yMi5kZWZhdWx0KGVyciwgbmV3IEVycm9yKFwidHVzOiBmYWlsZWQgdG8gdGVybWluYXRlIHVwbG9hZFwiKSwgeGhyKSk7XG4gICAgICB9O1xuXG4gICAgICBzZXR1cFhIUih4aHIsIG9wdGlvbnMpO1xuICAgICAgeGhyLnNlbmQobnVsbCk7XG4gICAgfVxuICB9XSk7XG5cbiAgcmV0dXJuIFVwbG9hZDtcbn0oKTtcblxuZnVuY3Rpb24gZW5jb2RlTWV0YWRhdGEobWV0YWRhdGEpIHtcbiAgdmFyIGVuY29kZWQgPSBbXTtcblxuICBmb3IgKHZhciBrZXkgaW4gbWV0YWRhdGEpIHtcbiAgICBlbmNvZGVkLnB1c2goa2V5ICsgXCIgXCIgKyBfanNCYXNlLkJhc2U2NC5lbmNvZGUobWV0YWRhdGFba2V5XSkpO1xuICB9XG5cbiAgcmV0dXJuIGVuY29kZWQuam9pbihcIixcIik7XG59XG5cbi8qKlxuICogQ2hlY2tzIHdoZXRoZXIgYSBnaXZlbiBzdGF0dXMgaXMgaW4gdGhlIHJhbmdlIG9mIHRoZSBleHBlY3RlZCBjYXRlZ29yeS5cbiAqIEZvciBleGFtcGxlLCBvbmx5IGEgc3RhdHVzIGJldHdlZW4gMjAwIGFuZCAyOTkgd2lsbCBzYXRpc2Z5IHRoZSBjYXRlZ29yeSAyMDAuXG4gKlxuICogQGFwaSBwcml2YXRlXG4gKi9cbmZ1bmN0aW9uIGluU3RhdHVzQ2F0ZWdvcnkoc3RhdHVzLCBjYXRlZ29yeSkge1xuICByZXR1cm4gc3RhdHVzID49IGNhdGVnb3J5ICYmIHN0YXR1cyA8IGNhdGVnb3J5ICsgMTAwO1xufVxuXG5mdW5jdGlvbiBzZXR1cFhIUih4aHIsIG9wdGlvbnMpIHtcbiAgeGhyLnNldFJlcXVlc3RIZWFkZXIoXCJUdXMtUmVzdW1hYmxlXCIsIFwiMS4wLjBcIik7XG4gIHZhciBoZWFkZXJzID0gb3B0aW9ucy5oZWFkZXJzIHx8IHt9O1xuXG4gIGZvciAodmFyIG5hbWUgaW4gaGVhZGVycykge1xuICAgIHhoci5zZXRSZXF1ZXN0SGVhZGVyKG5hbWUsIGhlYWRlcnNbbmFtZV0pO1xuICB9XG5cbiAgeGhyLndpdGhDcmVkZW50aWFscyA9IG9wdGlvbnMud2l0aENyZWRlbnRpYWxzO1xufVxuXG5VcGxvYWQuZGVmYXVsdE9wdGlvbnMgPSBkZWZhdWx0T3B0aW9ucztcblxuZXhwb3J0cy5kZWZhdWx0ID0gVXBsb2FkOyIsIm1vZHVsZS5leHBvcnRzPXtcbiAgXCJuYW1lXCI6IFwiQHVwcHkvdHVzXCIsXG4gIFwiZGVzY3JpcHRpb25cIjogXCJSZXN1bWFibGUgdXBsb2FkcyBmb3IgVXBweSB1c2luZyBUdXMuaW9cIixcbiAgXCJ2ZXJzaW9uXCI6IFwiMS40LjJcIixcbiAgXCJsaWNlbnNlXCI6IFwiTUlUXCIsXG4gIFwibWFpblwiOiBcImxpYi9pbmRleC5qc1wiLFxuICBcInR5cGVzXCI6IFwidHlwZXMvaW5kZXguZC50c1wiLFxuICBcImtleXdvcmRzXCI6IFtcbiAgICBcImZpbGUgdXBsb2FkZXJcIixcbiAgICBcInVwcHlcIixcbiAgICBcInVwcHktcGx1Z2luXCIsXG4gICAgXCJ1cGxvYWRcIixcbiAgICBcInJlc3VtYWJsZVwiLFxuICAgIFwidHVzXCJcbiAgXSxcbiAgXCJob21lcGFnZVwiOiBcImh0dHBzOi8vdXBweS5pb1wiLFxuICBcImJ1Z3NcIjoge1xuICAgIFwidXJsXCI6IFwiaHR0cHM6Ly9naXRodWIuY29tL3RyYW5zbG9hZGl0L3VwcHkvaXNzdWVzXCJcbiAgfSxcbiAgXCJyZXBvc2l0b3J5XCI6IHtcbiAgICBcInR5cGVcIjogXCJnaXRcIixcbiAgICBcInVybFwiOiBcImdpdCtodHRwczovL2dpdGh1Yi5jb20vdHJhbnNsb2FkaXQvdXBweS5naXRcIlxuICB9LFxuICBcImRlcGVuZGVuY2llc1wiOiB7XG4gICAgXCJAdXBweS9jb21wYW5pb24tY2xpZW50XCI6IFwiZmlsZTouLi9jb21wYW5pb24tY2xpZW50XCIsXG4gICAgXCJAdXBweS91dGlsc1wiOiBcImZpbGU6Li4vdXRpbHNcIixcbiAgICBcInR1cy1qcy1jbGllbnRcIjogXCJeMS44LjAtMlwiXG4gIH0sXG4gIFwicGVlckRlcGVuZGVuY2llc1wiOiB7XG4gICAgXCJAdXBweS9jb3JlXCI6IFwiXjEuMC4wXCJcbiAgfVxufVxuIiwiY29uc3QgdHVzID0gcmVxdWlyZSgndHVzLWpzLWNsaWVudCcpXG5cbmZ1bmN0aW9uIGlzQ29yZG92YSAoKSB7XG4gIHJldHVybiB0eXBlb2Ygd2luZG93ICE9PSAndW5kZWZpbmVkJyAmJiAoXG4gICAgdHlwZW9mIHdpbmRvdy5QaG9uZUdhcCAhPT0gJ3VuZGVmaW5lZCcgfHxcbiAgICB0eXBlb2Ygd2luZG93LkNvcmRvdmEgIT09ICd1bmRlZmluZWQnIHx8XG4gICAgdHlwZW9mIHdpbmRvdy5jb3Jkb3ZhICE9PSAndW5kZWZpbmVkJ1xuICApXG59XG5cbmZ1bmN0aW9uIGlzUmVhY3ROYXRpdmUgKCkge1xuICByZXR1cm4gdHlwZW9mIG5hdmlnYXRvciAhPT0gJ3VuZGVmaW5lZCcgJiZcbiAgICB0eXBlb2YgbmF2aWdhdG9yLnByb2R1Y3QgPT09ICdzdHJpbmcnICYmXG4gICAgbmF2aWdhdG9yLnByb2R1Y3QudG9Mb3dlckNhc2UoKSA9PT0gJ3JlYWN0bmF0aXZlJ1xufVxuXG4vLyBXZSBvdmVycmlkZSB0dXMgZmluZ2VycHJpbnQgdG8gdXBweeKAmXMgYGZpbGUuaWRgLCBzaW5jZSB0aGUgYGZpbGUuaWRgXG4vLyBub3cgYWxzbyBpbmNsdWRlcyBgcmVsYXRpdmVQYXRoYCBmb3IgZmlsZXMgYWRkZWQgZnJvbSBmb2xkZXJzLlxuLy8gVGhpcyBtZWFucyB5b3UgY2FuIGFkZCAyIGlkZW50aWNhbCBmaWxlcywgaWYgb25lIGlzIGluIGZvbGRlciBhLFxuLy8gdGhlIG90aGVyIGluIGZvbGRlciBiIOKAlCBgYS9maWxlLmpwZ2AgYW5kIGBiL2ZpbGUuanBnYCwgd2hlbiBhZGRlZFxuLy8gdG9nZXRoZXIgd2l0aCBhIGZvbGRlciwgd2lsbCBiZSB0cmVhdGVkIGFzIDIgc2VwYXJhdGUgZmlsZXMuXG4vL1xuLy8gRm9yIFJlYWN0IE5hdGl2ZSBhbmQgQ29yZG92YSwgd2UgbGV0IHR1cy1qcy1jbGllbnTigJlzIGRlZmF1bHRcbi8vIGZpbmdlcnByaW50IGhhbmRsaW5nIHRha2UgY2hhcmdlLlxubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBnZXRGaW5nZXJwcmludCAodXBweUZpbGVPYmopIHtcbiAgcmV0dXJuIGZ1bmN0aW9uIChmaWxlLCBvcHRpb25zLCBjYWxsYmFjaykge1xuICAgIGlmIChpc0NvcmRvdmEoKSB8fCBpc1JlYWN0TmF0aXZlKCkpIHtcbiAgICAgIHJldHVybiB0dXMuVXBsb2FkLmRlZmF1bHRPcHRpb25zLmZpbmdlcnByaW50KGZpbGUsIG9wdGlvbnMsIGNhbGxiYWNrKVxuICAgIH1cblxuICAgIGNvbnN0IHVwcHlGaW5nZXJwcmludCA9IFtcbiAgICAgICd0dXMnLFxuICAgICAgdXBweUZpbGVPYmouaWQsXG4gICAgICBvcHRpb25zLmVuZHBvaW50XG4gICAgXS5qb2luKCctJylcblxuICAgIHJldHVybiBjYWxsYmFjayhudWxsLCB1cHB5RmluZ2VycHJpbnQpXG4gIH1cbn1cbiIsImNvbnN0IHsgUGx1Z2luIH0gPSByZXF1aXJlKCdAdXBweS9jb3JlJylcbmNvbnN0IHR1cyA9IHJlcXVpcmUoJ3R1cy1qcy1jbGllbnQnKVxuY29uc3QgeyBQcm92aWRlciwgUmVxdWVzdENsaWVudCwgU29ja2V0IH0gPSByZXF1aXJlKCdAdXBweS9jb21wYW5pb24tY2xpZW50JylcbmNvbnN0IGVtaXRTb2NrZXRQcm9ncmVzcyA9IHJlcXVpcmUoJ0B1cHB5L3V0aWxzL2xpYi9lbWl0U29ja2V0UHJvZ3Jlc3MnKVxuY29uc3QgZ2V0U29ja2V0SG9zdCA9IHJlcXVpcmUoJ0B1cHB5L3V0aWxzL2xpYi9nZXRTb2NrZXRIb3N0JylcbmNvbnN0IHNldHRsZSA9IHJlcXVpcmUoJ0B1cHB5L3V0aWxzL2xpYi9zZXR0bGUnKVxuY29uc3QgRXZlbnRUcmFja2VyID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL0V2ZW50VHJhY2tlcicpXG5jb25zdCBSYXRlTGltaXRlZFF1ZXVlID0gcmVxdWlyZSgnQHVwcHkvdXRpbHMvbGliL1JhdGVMaW1pdGVkUXVldWUnKVxuY29uc3QgZ2V0RmluZ2VycHJpbnQgPSByZXF1aXJlKCcuL2dldEZpbmdlcnByaW50JylcblxuLyoqIEB0eXBlZGVmIHtpbXBvcnQoJy4uJykuVHVzT3B0aW9uc30gVHVzT3B0aW9ucyAqL1xuLyoqIEB0eXBlZGVmIHtpbXBvcnQoJ0B1cHB5L2NvcmUnKS5VcHB5fSBVcHB5ICovXG4vKiogQHR5cGVkZWYge2ltcG9ydCgnQHVwcHkvY29yZScpLlVwcHlGaWxlfSBVcHB5RmlsZSAqL1xuLyoqIEB0eXBlZGVmIHtpbXBvcnQoJ0B1cHB5L2NvcmUnKS5GYWlsZWRVcHB5RmlsZTx7fT59IEZhaWxlZFVwcHlGaWxlICovXG5cbi8qKlxuICogRXh0cmFjdGVkIGZyb20gaHR0cHM6Ly9naXRodWIuY29tL3R1cy90dXMtanMtY2xpZW50L2Jsb2IvbWFzdGVyL2xpYi91cGxvYWQuanMjTDEzXG4gKiBleGNlcHRlZCB3ZSByZW1vdmVkICdmaW5nZXJwcmludCcga2V5IHRvIGF2b2lkIGFkZGluZyBtb3JlIGRlcGVuZGVuY2llc1xuICpcbiAqIEB0eXBlIHtUdXNPcHRpb25zfVxuICovXG5jb25zdCB0dXNEZWZhdWx0T3B0aW9ucyA9IHtcbiAgZW5kcG9pbnQ6ICcnLFxuICByZXN1bWU6IHRydWUsXG4gIG9uUHJvZ3Jlc3M6IG51bGwsXG4gIG9uQ2h1bmtDb21wbGV0ZTogbnVsbCxcbiAgb25TdWNjZXNzOiBudWxsLFxuICBvbkVycm9yOiBudWxsLFxuICBoZWFkZXJzOiB7fSxcbiAgY2h1bmtTaXplOiBJbmZpbml0eSxcbiAgd2l0aENyZWRlbnRpYWxzOiBmYWxzZSxcbiAgdXBsb2FkVXJsOiBudWxsLFxuICB1cGxvYWRTaXplOiBudWxsLFxuICBvdmVycmlkZVBhdGNoTWV0aG9kOiBmYWxzZSxcbiAgcmV0cnlEZWxheXM6IG51bGxcbn1cblxuLyoqXG4gKiBUdXMgcmVzdW1hYmxlIGZpbGUgdXBsb2FkZXJcbiAqL1xubW9kdWxlLmV4cG9ydHMgPSBjbGFzcyBUdXMgZXh0ZW5kcyBQbHVnaW4ge1xuICBzdGF0aWMgVkVSU0lPTiA9IHJlcXVpcmUoJy4uL3BhY2thZ2UuanNvbicpLnZlcnNpb25cblxuICAvKipcbiAgICogQHBhcmFtIHtVcHB5fSB1cHB5XG4gICAqIEBwYXJhbSB7VHVzT3B0aW9uc30gb3B0c1xuICAgKi9cbiAgY29uc3RydWN0b3IgKHVwcHksIG9wdHMpIHtcbiAgICBzdXBlcih1cHB5LCBvcHRzKVxuICAgIHRoaXMudHlwZSA9ICd1cGxvYWRlcidcbiAgICB0aGlzLmlkID0gdGhpcy5vcHRzLmlkIHx8ICdUdXMnXG4gICAgdGhpcy50aXRsZSA9ICdUdXMnXG5cbiAgICAvLyBzZXQgZGVmYXVsdCBvcHRpb25zXG4gICAgY29uc3QgZGVmYXVsdE9wdGlvbnMgPSB7XG4gICAgICByZXN1bWU6IHRydWUsXG4gICAgICBhdXRvUmV0cnk6IHRydWUsXG4gICAgICB1c2VGYXN0UmVtb3RlUmV0cnk6IHRydWUsXG4gICAgICBsaW1pdDogMCxcbiAgICAgIHJldHJ5RGVsYXlzOiBbMCwgMTAwMCwgMzAwMCwgNTAwMF1cbiAgICB9XG5cbiAgICAvLyBtZXJnZSBkZWZhdWx0IG9wdGlvbnMgd2l0aCB0aGUgb25lcyBzZXQgYnkgdXNlclxuICAgIC8qKiBAdHlwZSB7aW1wb3J0KFwiLi5cIikuVHVzT3B0aW9uc30gKi9cbiAgICB0aGlzLm9wdHMgPSBPYmplY3QuYXNzaWduKHt9LCBkZWZhdWx0T3B0aW9ucywgb3B0cylcblxuICAgIC8qKlxuICAgICAqIFNpbXVsdGFuZW91cyB1cGxvYWQgbGltaXRpbmcgaXMgc2hhcmVkIGFjcm9zcyBhbGwgdXBsb2FkcyB3aXRoIHRoaXMgcGx1Z2luLlxuICAgICAqXG4gICAgICogQHR5cGUge1JhdGVMaW1pdGVkUXVldWV9XG4gICAgICovXG4gICAgdGhpcy5yZXF1ZXN0cyA9IG5ldyBSYXRlTGltaXRlZFF1ZXVlKHRoaXMub3B0cy5saW1pdClcblxuICAgIHRoaXMudXBsb2FkZXJzID0gT2JqZWN0LmNyZWF0ZShudWxsKVxuICAgIHRoaXMudXBsb2FkZXJFdmVudHMgPSBPYmplY3QuY3JlYXRlKG51bGwpXG4gICAgdGhpcy51cGxvYWRlclNvY2tldHMgPSBPYmplY3QuY3JlYXRlKG51bGwpXG5cbiAgICB0aGlzLmhhbmRsZVJlc2V0UHJvZ3Jlc3MgPSB0aGlzLmhhbmRsZVJlc2V0UHJvZ3Jlc3MuYmluZCh0aGlzKVxuICAgIHRoaXMuaGFuZGxlVXBsb2FkID0gdGhpcy5oYW5kbGVVcGxvYWQuYmluZCh0aGlzKVxuICB9XG5cbiAgaGFuZGxlUmVzZXRQcm9ncmVzcyAoKSB7XG4gICAgY29uc3QgZmlsZXMgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLnVwcHkuZ2V0U3RhdGUoKS5maWxlcylcbiAgICBPYmplY3Qua2V5cyhmaWxlcykuZm9yRWFjaCgoZmlsZUlEKSA9PiB7XG4gICAgICAvLyBPbmx5IGNsb25lIHRoZSBmaWxlIG9iamVjdCBpZiBpdCBoYXMgYSBUdXMgYHVwbG9hZFVybGAgYXR0YWNoZWQuXG4gICAgICBpZiAoZmlsZXNbZmlsZUlEXS50dXMgJiYgZmlsZXNbZmlsZUlEXS50dXMudXBsb2FkVXJsKSB7XG4gICAgICAgIGNvbnN0IHR1c1N0YXRlID0gT2JqZWN0LmFzc2lnbih7fSwgZmlsZXNbZmlsZUlEXS50dXMpXG4gICAgICAgIGRlbGV0ZSB0dXNTdGF0ZS51cGxvYWRVcmxcbiAgICAgICAgZmlsZXNbZmlsZUlEXSA9IE9iamVjdC5hc3NpZ24oe30sIGZpbGVzW2ZpbGVJRF0sIHsgdHVzOiB0dXNTdGF0ZSB9KVxuICAgICAgfVxuICAgIH0pXG5cbiAgICB0aGlzLnVwcHkuc2V0U3RhdGUoeyBmaWxlcyB9KVxuICB9XG5cbiAgLyoqXG4gICAqIENsZWFuIHVwIGFsbCByZWZlcmVuY2VzIGZvciBhIGZpbGUncyB1cGxvYWQ6IHRoZSB0dXMuVXBsb2FkIGluc3RhbmNlLFxuICAgKiBhbnkgZXZlbnRzIHJlbGF0ZWQgdG8gdGhlIGZpbGUsIGFuZCB0aGUgQ29tcGFuaW9uIFdlYlNvY2tldCBjb25uZWN0aW9uLlxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gZmlsZUlEXG4gICAqL1xuICByZXNldFVwbG9hZGVyUmVmZXJlbmNlcyAoZmlsZUlELCBvcHRzID0ge30pIHtcbiAgICBpZiAodGhpcy51cGxvYWRlcnNbZmlsZUlEXSkge1xuICAgICAgY29uc3QgdXBsb2FkZXIgPSB0aGlzLnVwbG9hZGVyc1tmaWxlSURdXG4gICAgICB1cGxvYWRlci5hYm9ydCgpXG4gICAgICBpZiAob3B0cy5hYm9ydCkge1xuICAgICAgICAvLyB0byBhdm9pZCA0MjMgZXJyb3IgZnJvbSB0dXMgc2VydmVyLCB3ZSB3YWl0XG4gICAgICAgIC8vIHRvIGJlIHN1cmUgdGhlIHByZXZpb3VzIHJlcXVlc3QgaGFzIGJlZW4gYWJvcnRlZCBiZWZvcmUgdGVybWluYXRpbmcgdGhlIHVwbG9hZFxuICAgICAgICAvLyBAdG9kbyByZW1vdmUgdGhlIHRpbWVvdXQgd2hlbiB0aGlzIFwid2FpdFwiIGlzIGhhbmRsZWQgaW4gdHVzLWpzLWNsaWVudCBpbnRlcm5hbGx5XG4gICAgICAgIHNldFRpbWVvdXQoKCkgPT4gdXBsb2FkZXIuYWJvcnQodHJ1ZSksIDEwMDApXG4gICAgICB9XG4gICAgICB0aGlzLnVwbG9hZGVyc1tmaWxlSURdID0gbnVsbFxuICAgIH1cbiAgICBpZiAodGhpcy51cGxvYWRlckV2ZW50c1tmaWxlSURdKSB7XG4gICAgICB0aGlzLnVwbG9hZGVyRXZlbnRzW2ZpbGVJRF0ucmVtb3ZlKClcbiAgICAgIHRoaXMudXBsb2FkZXJFdmVudHNbZmlsZUlEXSA9IG51bGxcbiAgICB9XG4gICAgaWYgKHRoaXMudXBsb2FkZXJTb2NrZXRzW2ZpbGVJRF0pIHtcbiAgICAgIHRoaXMudXBsb2FkZXJTb2NrZXRzW2ZpbGVJRF0uY2xvc2UoKVxuICAgICAgdGhpcy51cGxvYWRlclNvY2tldHNbZmlsZUlEXSA9IG51bGxcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogQ3JlYXRlIGEgbmV3IFR1cyB1cGxvYWQuXG4gICAqXG4gICAqIEEgbG90IGNhbiBoYXBwZW4gZHVyaW5nIGFuIHVwbG9hZCwgc28gdGhpcyBpcyBxdWl0ZSBoYXJkIHRvIGZvbGxvdyFcbiAgICogLSBGaXJzdCwgdGhlIHVwbG9hZCBpcyBzdGFydGVkLiBJZiB0aGUgZmlsZSB3YXMgYWxyZWFkeSBwYXVzZWQgYnkgdGhlIHRpbWUgdGhlIHVwbG9hZCBzdGFydHMsIG5vdGhpbmcgc2hvdWxkIGhhcHBlbi5cbiAgICogICBJZiB0aGUgYGxpbWl0YCBvcHRpb24gaXMgdXNlZCwgdGhlIHVwbG9hZCBtdXN0IGJlIHF1ZXVlZCBvbnRvIHRoZSBgdGhpcy5yZXF1ZXN0c2AgcXVldWUuXG4gICAqICAgV2hlbiBhbiB1cGxvYWQgc3RhcnRzLCB3ZSBzdG9yZSB0aGUgdHVzLlVwbG9hZCBpbnN0YW5jZSwgYW5kIGFuIEV2ZW50VHJhY2tlciBpbnN0YW5jZSB0aGF0IG1hbmFnZXMgdGhlIGV2ZW50IGxpc3RlbmVyc1xuICAgKiAgIGZvciBwYXVzaW5nLCBjYW5jZWxsYXRpb24sIHJlbW92YWwsIGV0Yy5cbiAgICogLSBXaGlsZSB0aGUgdXBsb2FkIGlzIGluIHByb2dyZXNzLCBpdCBtYXkgYmUgcGF1c2VkIG9yIGNhbmNlbGxlZC5cbiAgICogICBQYXVzaW5nIGFib3J0cyB0aGUgdW5kZXJseWluZyB0dXMuVXBsb2FkLCBhbmQgcmVtb3ZlcyB0aGUgdXBsb2FkIGZyb20gdGhlIGB0aGlzLnJlcXVlc3RzYCBxdWV1ZS4gQWxsIG90aGVyIHN0YXRlIGlzXG4gICAqICAgbWFpbnRhaW5lZC5cbiAgICogICBDYW5jZWxsaW5nIHJlbW92ZXMgdGhlIHVwbG9hZCBmcm9tIHRoZSBgdGhpcy5yZXF1ZXN0c2AgcXVldWUsIGFuZCBjb21wbGV0ZWx5IGFib3J0cyB0aGUgdXBsb2FkLS10aGUgdHVzLlVwbG9hZCBpbnN0YW5jZVxuICAgKiAgIGlzIGFib3J0ZWQgYW5kIGRpc2NhcmRlZCwgdGhlIEV2ZW50VHJhY2tlciBpbnN0YW5jZSBpcyBkZXN0cm95ZWQgKHJlbW92aW5nIGFsbCBsaXN0ZW5lcnMpLlxuICAgKiAgIFJlc3VtaW5nIHRoZSB1cGxvYWQgdXNlcyB0aGUgYHRoaXMucmVxdWVzdHNgIHF1ZXVlIGFzIHdlbGwsIHRvIHByZXZlbnQgc2VsZWN0aXZlbHkgcGF1c2luZyBhbmQgcmVzdW1pbmcgdXBsb2FkcyBmcm9tXG4gICAqICAgYnlwYXNzaW5nIHRoZSBsaW1pdC5cbiAgICogLSBBZnRlciBjb21wbGV0aW5nIGFuIHVwbG9hZCwgdGhlIHR1cy5VcGxvYWQgYW5kIEV2ZW50VHJhY2tlciBpbnN0YW5jZXMgYXJlIGNsZWFuZWQgdXAsIGFuZCB0aGUgdXBsb2FkIGlzIG1hcmtlZCBhcyBkb25lXG4gICAqICAgaW4gdGhlIGB0aGlzLnJlcXVlc3RzYCBxdWV1ZS5cbiAgICogLSBXaGVuIGFuIHVwbG9hZCBjb21wbGV0ZWQgd2l0aCBhbiBlcnJvciwgdGhlIHNhbWUgaGFwcGVucyBhcyBvbiBzdWNjZXNzZnVsIGNvbXBsZXRpb24sIGJ1dCB0aGUgYHVwbG9hZCgpYCBwcm9taXNlIGlzIHJlamVjdGVkLlxuICAgKlxuICAgKiBXaGVuIHdvcmtpbmcgb24gdGhpcyBmdW5jdGlvbiwga2VlcCBpbiBtaW5kOlxuICAgKiAgLSBXaGVuIGFuIHVwbG9hZCBpcyBjb21wbGV0ZWQgb3IgY2FuY2VsbGVkIGZvciBhbnkgcmVhc29uLCB0aGUgdHVzLlVwbG9hZCBhbmQgRXZlbnRUcmFja2VyIGluc3RhbmNlcyBuZWVkIHRvIGJlIGNsZWFuZWQgdXAgdXNpbmcgdGhpcy5yZXNldFVwbG9hZGVyUmVmZXJlbmNlcygpLlxuICAgKiAgLSBXaGVuIGFuIHVwbG9hZCBpcyBjYW5jZWxsZWQgb3IgcGF1c2VkLCBmb3IgYW55IHJlYXNvbiwgaXQgbmVlZHMgdG8gYmUgcmVtb3ZlZCBmcm9tIHRoZSBgdGhpcy5yZXF1ZXN0c2AgcXVldWUgdXNpbmcgYHF1ZXVlZFJlcXVlc3QuYWJvcnQoKWAuXG4gICAqICAtIFdoZW4gYW4gdXBsb2FkIGlzIGNvbXBsZXRlZCBmb3IgYW55IHJlYXNvbiwgaW5jbHVkaW5nIGVycm9ycywgaXQgbmVlZHMgdG8gYmUgbWFya2VkIGFzIHN1Y2ggdXNpbmcgYHF1ZXVlZFJlcXVlc3QuZG9uZSgpYC5cbiAgICogIC0gV2hlbiBhbiB1cGxvYWQgaXMgc3RhcnRlZCBvciByZXN1bWVkLCBpdCBuZWVkcyB0byBnbyB0aHJvdWdoIHRoZSBgdGhpcy5yZXF1ZXN0c2AgcXVldWUuIFRoZSBgcXVldWVkUmVxdWVzdGAgdmFyaWFibGUgbXVzdCBiZSB1cGRhdGVkIHNvIHRoZSBvdGhlciB1c2VzIG9mIGl0IGFyZSB2YWxpZC5cbiAgICogIC0gQmVmb3JlIHJlcGxhY2luZyB0aGUgYHF1ZXVlZFJlcXVlc3RgIHZhcmlhYmxlLCB0aGUgcHJldmlvdXMgYHF1ZXVlZFJlcXVlc3RgIG11c3QgYmUgYWJvcnRlZCwgZWxzZSBpdCB3aWxsIGtlZXAgdGFraW5nIHVwIGEgc3BvdCBpbiB0aGUgcXVldWUuXG4gICAqXG4gICAqIEBwYXJhbSB7VXBweUZpbGV9IGZpbGUgZm9yIHVzZSB3aXRoIHVwbG9hZFxuICAgKiBAcGFyYW0ge251bWJlcn0gY3VycmVudCBmaWxlIGluIGEgcXVldWVcbiAgICogQHBhcmFtIHtudW1iZXJ9IHRvdGFsIG51bWJlciBvZiBmaWxlcyBpbiBhIHF1ZXVlXG4gICAqIEByZXR1cm5zIHtQcm9taXNlPHZvaWQ+fVxuICAgKi9cbiAgdXBsb2FkIChmaWxlLCBjdXJyZW50LCB0b3RhbCkge1xuICAgIHRoaXMucmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoZmlsZS5pZClcblxuICAgIC8vIENyZWF0ZSBhIG5ldyB0dXMgdXBsb2FkXG4gICAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgIHRoaXMudXBweS5lbWl0KCd1cGxvYWQtc3RhcnRlZCcsIGZpbGUpXG5cbiAgICAgIGNvbnN0IG9wdHNUdXMgPSBPYmplY3QuYXNzaWduKFxuICAgICAgICB7fSxcbiAgICAgICAgdHVzRGVmYXVsdE9wdGlvbnMsXG4gICAgICAgIHRoaXMub3B0cyxcbiAgICAgICAgLy8gSW5zdGFsbCBmaWxlLXNwZWNpZmljIHVwbG9hZCBvdmVycmlkZXMuXG4gICAgICAgIGZpbGUudHVzIHx8IHt9XG4gICAgICApXG5cbiAgICAgIC8vIFdlIG92ZXJyaWRlIHR1cyBmaW5nZXJwcmludCB0byB1cHB54oCZcyBgZmlsZS5pZGAsIHNpbmNlIHRoZSBgZmlsZS5pZGBcbiAgICAgIC8vIG5vdyBhbHNvIGluY2x1ZGVzIGByZWxhdGl2ZVBhdGhgIGZvciBmaWxlcyBhZGRlZCBmcm9tIGZvbGRlcnMuXG4gICAgICAvLyBUaGlzIG1lYW5zIHlvdSBjYW4gYWRkIDIgaWRlbnRpY2FsIGZpbGVzLCBpZiBvbmUgaXMgaW4gZm9sZGVyIGEsXG4gICAgICAvLyB0aGUgb3RoZXIgaW4gZm9sZGVyIGIuXG4gICAgICBvcHRzVHVzLmZpbmdlcnByaW50ID0gZ2V0RmluZ2VycHJpbnQoZmlsZSlcblxuICAgICAgb3B0c1R1cy5vbkVycm9yID0gKGVycikgPT4ge1xuICAgICAgICB0aGlzLnVwcHkubG9nKGVycilcbiAgICAgICAgdGhpcy51cHB5LmVtaXQoJ3VwbG9hZC1lcnJvcicsIGZpbGUsIGVycilcbiAgICAgICAgZXJyLm1lc3NhZ2UgPSBgRmFpbGVkIGJlY2F1c2U6ICR7ZXJyLm1lc3NhZ2V9YFxuXG4gICAgICAgIHRoaXMucmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoZmlsZS5pZClcbiAgICAgICAgcXVldWVkUmVxdWVzdC5kb25lKClcbiAgICAgICAgcmVqZWN0KGVycilcbiAgICAgIH1cblxuICAgICAgb3B0c1R1cy5vblByb2dyZXNzID0gKGJ5dGVzVXBsb2FkZWQsIGJ5dGVzVG90YWwpID0+IHtcbiAgICAgICAgdGhpcy5vblJlY2VpdmVVcGxvYWRVcmwoZmlsZSwgdXBsb2FkLnVybClcbiAgICAgICAgdGhpcy51cHB5LmVtaXQoJ3VwbG9hZC1wcm9ncmVzcycsIGZpbGUsIHtcbiAgICAgICAgICB1cGxvYWRlcjogdGhpcyxcbiAgICAgICAgICBieXRlc1VwbG9hZGVkOiBieXRlc1VwbG9hZGVkLFxuICAgICAgICAgIGJ5dGVzVG90YWw6IGJ5dGVzVG90YWxcbiAgICAgICAgfSlcbiAgICAgIH1cblxuICAgICAgb3B0c1R1cy5vblN1Y2Nlc3MgPSAoKSA9PiB7XG4gICAgICAgIGNvbnN0IHVwbG9hZFJlc3AgPSB7XG4gICAgICAgICAgdXBsb2FkVVJMOiB1cGxvYWQudXJsXG4gICAgICAgIH1cblxuICAgICAgICB0aGlzLnVwcHkuZW1pdCgndXBsb2FkLXN1Y2Nlc3MnLCBmaWxlLCB1cGxvYWRSZXNwKVxuXG4gICAgICAgIGlmICh1cGxvYWQudXJsKSB7XG4gICAgICAgICAgdGhpcy51cHB5LmxvZygnRG93bmxvYWQgJyArIHVwbG9hZC5maWxlLm5hbWUgKyAnIGZyb20gJyArIHVwbG9hZC51cmwpXG4gICAgICAgIH1cblxuICAgICAgICB0aGlzLnJlc2V0VXBsb2FkZXJSZWZlcmVuY2VzKGZpbGUuaWQpXG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuZG9uZSgpXG4gICAgICAgIHJlc29sdmUodXBsb2FkKVxuICAgICAgfVxuXG4gICAgICBjb25zdCBjb3B5UHJvcCA9IChvYmosIHNyY1Byb3AsIGRlc3RQcm9wKSA9PiB7XG4gICAgICAgIGlmIChcbiAgICAgICAgICBPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwob2JqLCBzcmNQcm9wKSAmJlxuICAgICAgICAgICFPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwob2JqLCBkZXN0UHJvcClcbiAgICAgICAgKSB7XG4gICAgICAgICAgb2JqW2Rlc3RQcm9wXSA9IG9ialtzcmNQcm9wXVxuICAgICAgICB9XG4gICAgICB9XG5cbiAgICAgIGNvbnN0IG1ldGEgPSB7fVxuICAgICAgY29uc3QgbWV0YUZpZWxkcyA9IEFycmF5LmlzQXJyYXkob3B0c1R1cy5tZXRhRmllbGRzKVxuICAgICAgICA/IG9wdHNUdXMubWV0YUZpZWxkc1xuICAgICAgICAvLyBTZW5kIGFsb25nIGFsbCBmaWVsZHMgYnkgZGVmYXVsdC5cbiAgICAgICAgOiBPYmplY3Qua2V5cyhmaWxlLm1ldGEpXG4gICAgICBtZXRhRmllbGRzLmZvckVhY2goKGl0ZW0pID0+IHtcbiAgICAgICAgbWV0YVtpdGVtXSA9IGZpbGUubWV0YVtpdGVtXVxuICAgICAgfSlcblxuICAgICAgLy8gdHVzZCB1c2VzIG1ldGFkYXRhIGZpZWxkcyAnZmlsZXR5cGUnIGFuZCAnZmlsZW5hbWUnXG4gICAgICBjb3B5UHJvcChtZXRhLCAndHlwZScsICdmaWxldHlwZScpXG4gICAgICBjb3B5UHJvcChtZXRhLCAnbmFtZScsICdmaWxlbmFtZScpXG5cbiAgICAgIG9wdHNUdXMubWV0YWRhdGEgPSBtZXRhXG5cbiAgICAgIGNvbnN0IHVwbG9hZCA9IG5ldyB0dXMuVXBsb2FkKGZpbGUuZGF0YSwgb3B0c1R1cylcbiAgICAgIHRoaXMudXBsb2FkZXJzW2ZpbGUuaWRdID0gdXBsb2FkXG4gICAgICB0aGlzLnVwbG9hZGVyRXZlbnRzW2ZpbGUuaWRdID0gbmV3IEV2ZW50VHJhY2tlcih0aGlzLnVwcHkpXG5cbiAgICAgIGxldCBxdWV1ZWRSZXF1ZXN0ID0gdGhpcy5yZXF1ZXN0cy5ydW4oKCkgPT4ge1xuICAgICAgICBpZiAoIWZpbGUuaXNQYXVzZWQpIHtcbiAgICAgICAgICB1cGxvYWQuc3RhcnQoKVxuICAgICAgICB9XG4gICAgICAgIC8vIERvbid0IGRvIGFueXRoaW5nIGhlcmUsIHRoZSBjYWxsZXIgd2lsbCB0YWtlIGNhcmUgb2YgY2FuY2VsbGluZyB0aGUgdXBsb2FkIGl0c2VsZlxuICAgICAgICAvLyB1c2luZyByZXNldFVwbG9hZGVyUmVmZXJlbmNlcygpLiBUaGlzIGlzIGJlY2F1c2UgcmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoKSBoYXMgdG8gYmVcbiAgICAgICAgLy8gY2FsbGVkIHdoZW4gdGhpcyByZXF1ZXN0IGlzIHN0aWxsIGluIHRoZSBxdWV1ZSwgYW5kIGhhcyBub3QgYmVlbiBzdGFydGVkIHlldCwgdG9vLiBBdFxuICAgICAgICAvLyB0aGF0IHBvaW50IHRoaXMgY2FuY2VsbGF0aW9uIGZ1bmN0aW9uIGlzIG5vdCBnb2luZyB0byBiZSBjYWxsZWQuXG4gICAgICAgIC8vIEFsc28sIHdlIG5lZWQgdG8gcmVtb3ZlIHRoZSByZXF1ZXN0IGZyb20gdGhlIHF1ZXVlIF93aXRob3V0XyBkZXN0cm95aW5nIGV2ZXJ5dGhpbmdcbiAgICAgICAgLy8gcmVsYXRlZCB0byB0aGlzIHVwbG9hZCB0byBoYW5kbGUgcGF1c2VzLlxuICAgICAgICByZXR1cm4gKCkgPT4ge31cbiAgICAgIH0pXG5cbiAgICAgIHRoaXMub25GaWxlUmVtb3ZlKGZpbGUuaWQsICh0YXJnZXRGaWxlSUQpID0+IHtcbiAgICAgICAgcXVldWVkUmVxdWVzdC5hYm9ydCgpXG4gICAgICAgIHRoaXMucmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoZmlsZS5pZCwgeyBhYm9ydDogISF1cGxvYWQudXJsIH0pXG4gICAgICAgIHJlc29sdmUoYHVwbG9hZCAke3RhcmdldEZpbGVJRH0gd2FzIHJlbW92ZWRgKVxuICAgICAgfSlcblxuICAgICAgdGhpcy5vblBhdXNlKGZpbGUuaWQsIChpc1BhdXNlZCkgPT4ge1xuICAgICAgICBpZiAoaXNQYXVzZWQpIHtcbiAgICAgICAgICAvLyBSZW1vdmUgdGhpcyBmaWxlIGZyb20gdGhlIHF1ZXVlIHNvIGFub3RoZXIgZmlsZSBjYW4gc3RhcnQgaW4gaXRzIHBsYWNlLlxuICAgICAgICAgIHF1ZXVlZFJlcXVlc3QuYWJvcnQoKVxuICAgICAgICAgIHVwbG9hZC5hYm9ydCgpXG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgLy8gUmVzdW1pbmcgYW4gdXBsb2FkIHNob3VsZCBiZSBxdWV1ZWQsIGVsc2UgeW91IGNvdWxkIHBhdXNlIGFuZCB0aGVuIHJlc3VtZSBhIHF1ZXVlZCB1cGxvYWQgdG8gbWFrZSBpdCBza2lwIHRoZSBxdWV1ZS5cbiAgICAgICAgICBxdWV1ZWRSZXF1ZXN0LmFib3J0KClcbiAgICAgICAgICBxdWV1ZWRSZXF1ZXN0ID0gdGhpcy5yZXF1ZXN0cy5ydW4oKCkgPT4ge1xuICAgICAgICAgICAgdXBsb2FkLnN0YXJ0KClcbiAgICAgICAgICAgIHJldHVybiAoKSA9PiB7fVxuICAgICAgICAgIH0pXG4gICAgICAgIH1cbiAgICAgIH0pXG5cbiAgICAgIHRoaXMub25QYXVzZUFsbChmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuYWJvcnQoKVxuICAgICAgICB1cGxvYWQuYWJvcnQoKVxuICAgICAgfSlcblxuICAgICAgdGhpcy5vbkNhbmNlbEFsbChmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuYWJvcnQoKVxuICAgICAgICB0aGlzLnJlc2V0VXBsb2FkZXJSZWZlcmVuY2VzKGZpbGUuaWQsIHsgYWJvcnQ6ICEhdXBsb2FkLnVybCB9KVxuICAgICAgICByZXNvbHZlKGB1cGxvYWQgJHtmaWxlLmlkfSB3YXMgY2FuY2VsZWRgKVxuICAgICAgfSlcblxuICAgICAgdGhpcy5vblJlc3VtZUFsbChmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuYWJvcnQoKVxuICAgICAgICBpZiAoZmlsZS5lcnJvcikge1xuICAgICAgICAgIHVwbG9hZC5hYm9ydCgpXG4gICAgICAgIH1cbiAgICAgICAgcXVldWVkUmVxdWVzdCA9IHRoaXMucmVxdWVzdHMucnVuKCgpID0+IHtcbiAgICAgICAgICB1cGxvYWQuc3RhcnQoKVxuICAgICAgICAgIHJldHVybiAoKSA9PiB7fVxuICAgICAgICB9KVxuICAgICAgfSlcbiAgICB9KS5jYXRjaCgoZXJyKSA9PiB7XG4gICAgICB0aGlzLnVwcHkuZW1pdCgndXBsb2FkLWVycm9yJywgZmlsZSwgZXJyKVxuICAgICAgdGhyb3cgZXJyXG4gICAgfSlcbiAgfVxuXG4gIC8qKlxuICAgKiBAcGFyYW0ge1VwcHlGaWxlfSBmaWxlIGZvciB1c2Ugd2l0aCB1cGxvYWRcbiAgICogQHBhcmFtIHtudW1iZXJ9IGN1cnJlbnQgZmlsZSBpbiBhIHF1ZXVlXG4gICAqIEBwYXJhbSB7bnVtYmVyfSB0b3RhbCBudW1iZXIgb2YgZmlsZXMgaW4gYSBxdWV1ZVxuICAgKiBAcmV0dXJucyB7UHJvbWlzZTx2b2lkPn1cbiAgICovXG4gIHVwbG9hZFJlbW90ZSAoZmlsZSwgY3VycmVudCwgdG90YWwpIHtcbiAgICB0aGlzLnJlc2V0VXBsb2FkZXJSZWZlcmVuY2VzKGZpbGUuaWQpXG5cbiAgICBjb25zdCBvcHRzID0geyAuLi50aGlzLm9wdHMgfVxuICAgIGlmIChmaWxlLnR1cykge1xuICAgICAgLy8gSW5zdGFsbCBmaWxlLXNwZWNpZmljIHVwbG9hZCBvdmVycmlkZXMuXG4gICAgICBPYmplY3QuYXNzaWduKG9wdHMsIGZpbGUudHVzKVxuICAgIH1cblxuICAgIHRoaXMudXBweS5lbWl0KCd1cGxvYWQtc3RhcnRlZCcsIGZpbGUpXG4gICAgdGhpcy51cHB5LmxvZyhmaWxlLnJlbW90ZS51cmwpXG5cbiAgICBpZiAoZmlsZS5zZXJ2ZXJUb2tlbikge1xuICAgICAgcmV0dXJuIHRoaXMuY29ubmVjdFRvU2VydmVyU29ja2V0KGZpbGUpXG4gICAgfVxuXG4gICAgcmV0dXJuIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgIGNvbnN0IENsaWVudCA9IGZpbGUucmVtb3RlLnByb3ZpZGVyT3B0aW9ucy5wcm92aWRlciA/IFByb3ZpZGVyIDogUmVxdWVzdENsaWVudFxuICAgICAgY29uc3QgY2xpZW50ID0gbmV3IENsaWVudCh0aGlzLnVwcHksIGZpbGUucmVtb3RlLnByb3ZpZGVyT3B0aW9ucylcblxuICAgICAgLy8gISEgY2FuY2VsbGF0aW9uIGlzIE5PVCBzdXBwb3J0ZWQgYXQgdGhpcyBzdGFnZSB5ZXRcbiAgICAgIGNsaWVudC5wb3N0KGZpbGUucmVtb3RlLnVybCwge1xuICAgICAgICAuLi5maWxlLnJlbW90ZS5ib2R5LFxuICAgICAgICBlbmRwb2ludDogb3B0cy5lbmRwb2ludCxcbiAgICAgICAgdXBsb2FkVXJsOiBvcHRzLnVwbG9hZFVybCxcbiAgICAgICAgcHJvdG9jb2w6ICd0dXMnLFxuICAgICAgICBzaXplOiBmaWxlLmRhdGEuc2l6ZSxcbiAgICAgICAgbWV0YWRhdGE6IGZpbGUubWV0YVxuICAgICAgfSkudGhlbigocmVzKSA9PiB7XG4gICAgICAgIHRoaXMudXBweS5zZXRGaWxlU3RhdGUoZmlsZS5pZCwgeyBzZXJ2ZXJUb2tlbjogcmVzLnRva2VuIH0pXG4gICAgICAgIGZpbGUgPSB0aGlzLnVwcHkuZ2V0RmlsZShmaWxlLmlkKVxuICAgICAgICByZXR1cm4gdGhpcy5jb25uZWN0VG9TZXJ2ZXJTb2NrZXQoZmlsZSlcbiAgICAgIH0pLnRoZW4oKCkgPT4ge1xuICAgICAgICByZXNvbHZlKClcbiAgICAgIH0pLmNhdGNoKChlcnIpID0+IHtcbiAgICAgICAgcmVqZWN0KG5ldyBFcnJvcihlcnIpKVxuICAgICAgfSlcbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIFNlZSB0aGUgY29tbWVudCBvbiB0aGUgdXBsb2FkKCkgbWV0aG9kLlxuICAgKlxuICAgKiBBZGRpdGlvbmFsbHksIHdoZW4gYW4gdXBsb2FkIGlzIHJlbW92ZWQsIGNvbXBsZXRlZCwgb3IgY2FuY2VsbGVkLCB3ZSBuZWVkIHRvIGNsb3NlIHRoZSBXZWJTb2NrZXQgY29ubmVjdGlvbi4gVGhpcyBpcyBoYW5kbGVkIGJ5IHRoZSByZXNldFVwbG9hZGVyUmVmZXJlbmNlcygpIGZ1bmN0aW9uLCBzbyB0aGUgc2FtZSBndWlkZWxpbmVzIGFwcGx5IGFzIGluIHVwbG9hZCgpLlxuICAgKlxuICAgKiBAcGFyYW0ge1VwcHlGaWxlfSBmaWxlXG4gICAqL1xuICBjb25uZWN0VG9TZXJ2ZXJTb2NrZXQgKGZpbGUpIHtcbiAgICByZXR1cm4gbmV3IFByb21pc2UoKHJlc29sdmUsIHJlamVjdCkgPT4ge1xuICAgICAgY29uc3QgdG9rZW4gPSBmaWxlLnNlcnZlclRva2VuXG4gICAgICBjb25zdCBob3N0ID0gZ2V0U29ja2V0SG9zdChmaWxlLnJlbW90ZS5jb21wYW5pb25VcmwpXG4gICAgICBjb25zdCBzb2NrZXQgPSBuZXcgU29ja2V0KHsgdGFyZ2V0OiBgJHtob3N0fS9hcGkvJHt0b2tlbn1gLCBhdXRvT3BlbjogZmFsc2UgfSlcbiAgICAgIHRoaXMudXBsb2FkZXJTb2NrZXRzW2ZpbGUuaWRdID0gc29ja2V0XG4gICAgICB0aGlzLnVwbG9hZGVyRXZlbnRzW2ZpbGUuaWRdID0gbmV3IEV2ZW50VHJhY2tlcih0aGlzLnVwcHkpXG5cbiAgICAgIHRoaXMub25GaWxlUmVtb3ZlKGZpbGUuaWQsICgpID0+IHtcbiAgICAgICAgcXVldWVkUmVxdWVzdC5hYm9ydCgpXG4gICAgICAgIC8vIHN0aWxsIHNlbmQgcGF1c2UgZXZlbnQgaW4gY2FzZSB3ZSBhcmUgZGVhbGluZyB3aXRoIG9sZGVyIHZlcnNpb24gb2YgY29tcGFuaW9uXG4gICAgICAgIC8vIEB0b2RvIGRvbid0IHNlbmQgcGF1c2UgZXZlbnQgaW4gdGhlIG5leHQgbWFqb3IgcmVsZWFzZS5cbiAgICAgICAgc29ja2V0LnNlbmQoJ3BhdXNlJywge30pXG4gICAgICAgIHNvY2tldC5zZW5kKCdjYW5jZWwnLCB7fSlcbiAgICAgICAgdGhpcy5yZXNldFVwbG9hZGVyUmVmZXJlbmNlcyhmaWxlLmlkKVxuICAgICAgICByZXNvbHZlKGB1cGxvYWQgJHtmaWxlLmlkfSB3YXMgcmVtb3ZlZGApXG4gICAgICB9KVxuXG4gICAgICB0aGlzLm9uUGF1c2UoZmlsZS5pZCwgKGlzUGF1c2VkKSA9PiB7XG4gICAgICAgIGlmIChpc1BhdXNlZCkge1xuICAgICAgICAgIC8vIFJlbW92ZSB0aGlzIGZpbGUgZnJvbSB0aGUgcXVldWUgc28gYW5vdGhlciBmaWxlIGNhbiBzdGFydCBpbiBpdHMgcGxhY2UuXG4gICAgICAgICAgcXVldWVkUmVxdWVzdC5hYm9ydCgpXG4gICAgICAgICAgc29ja2V0LnNlbmQoJ3BhdXNlJywge30pXG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgLy8gUmVzdW1pbmcgYW4gdXBsb2FkIHNob3VsZCBiZSBxdWV1ZWQsIGVsc2UgeW91IGNvdWxkIHBhdXNlIGFuZCB0aGVuIHJlc3VtZSBhIHF1ZXVlZCB1cGxvYWQgdG8gbWFrZSBpdCBza2lwIHRoZSBxdWV1ZS5cbiAgICAgICAgICBxdWV1ZWRSZXF1ZXN0LmFib3J0KClcbiAgICAgICAgICBxdWV1ZWRSZXF1ZXN0ID0gdGhpcy5yZXF1ZXN0cy5ydW4oKCkgPT4ge1xuICAgICAgICAgICAgc29ja2V0LnNlbmQoJ3Jlc3VtZScsIHt9KVxuICAgICAgICAgICAgcmV0dXJuICgpID0+IHt9XG4gICAgICAgICAgfSlcbiAgICAgICAgfVxuICAgICAgfSlcblxuICAgICAgdGhpcy5vblBhdXNlQWxsKGZpbGUuaWQsICgpID0+IHtcbiAgICAgICAgcXVldWVkUmVxdWVzdC5hYm9ydCgpXG4gICAgICAgIHNvY2tldC5zZW5kKCdwYXVzZScsIHt9KVxuICAgICAgfSlcblxuICAgICAgdGhpcy5vbkNhbmNlbEFsbChmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuYWJvcnQoKVxuICAgICAgICAvLyBzdGlsbCBzZW5kIHBhdXNlIGV2ZW50IGluIGNhc2Ugd2UgYXJlIGRlYWxpbmcgd2l0aCBvbGRlciB2ZXJzaW9uIG9mIGNvbXBhbmlvblxuICAgICAgICAvLyBAdG9kbyBkb24ndCBzZW5kIHBhdXNlIGV2ZW50IGluIHRoZSBuZXh0IG1ham9yIHJlbGVhc2UuXG4gICAgICAgIHNvY2tldC5zZW5kKCdwYXVzZScsIHt9KVxuICAgICAgICBzb2NrZXQuc2VuZCgnY2FuY2VsJywge30pXG4gICAgICAgIHRoaXMucmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoZmlsZS5pZClcbiAgICAgICAgcmVzb2x2ZShgdXBsb2FkICR7ZmlsZS5pZH0gd2FzIGNhbmNlbGVkYClcbiAgICAgIH0pXG5cbiAgICAgIHRoaXMub25SZXN1bWVBbGwoZmlsZS5pZCwgKCkgPT4ge1xuICAgICAgICBxdWV1ZWRSZXF1ZXN0LmFib3J0KClcbiAgICAgICAgaWYgKGZpbGUuZXJyb3IpIHtcbiAgICAgICAgICBzb2NrZXQuc2VuZCgncGF1c2UnLCB7fSlcbiAgICAgICAgfVxuICAgICAgICBxdWV1ZWRSZXF1ZXN0ID0gdGhpcy5yZXF1ZXN0cy5ydW4oKCkgPT4ge1xuICAgICAgICAgIHNvY2tldC5zZW5kKCdyZXN1bWUnLCB7fSlcbiAgICAgICAgICByZXR1cm4gKCkgPT4ge31cbiAgICAgICAgfSlcbiAgICAgIH0pXG5cbiAgICAgIHRoaXMub25SZXRyeShmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIC8vIE9ubHkgZG8gdGhlIHJldHJ5IGlmIHRoZSB1cGxvYWQgaXMgYWN0dWFsbHkgaW4gcHJvZ3Jlc3M7XG4gICAgICAgIC8vIGVsc2Ugd2UgY291bGQgdHJ5IHRvIHNlbmQgdGhlc2UgbWVzc2FnZXMgd2hlbiB0aGUgdXBsb2FkIGlzIHN0aWxsIHF1ZXVlZC5cbiAgICAgICAgLy8gV2UgbWF5IG5lZWQgYSBiZXR0ZXIgY2hlY2sgZm9yIHRoaXMgc2luY2UgdGhlIHNvY2tldCBtYXkgYWxzbyBiZSBjbG9zZWRcbiAgICAgICAgLy8gZm9yIG90aGVyIHJlYXNvbnMsIGxpa2UgbmV0d29yayBmYWlsdXJlcy5cbiAgICAgICAgaWYgKHNvY2tldC5pc09wZW4pIHtcbiAgICAgICAgICBzb2NrZXQuc2VuZCgncGF1c2UnLCB7fSlcbiAgICAgICAgICBzb2NrZXQuc2VuZCgncmVzdW1lJywge30pXG4gICAgICAgIH1cbiAgICAgIH0pXG5cbiAgICAgIHRoaXMub25SZXRyeUFsbChmaWxlLmlkLCAoKSA9PiB7XG4gICAgICAgIC8vIFNlZSB0aGUgY29tbWVudCBpbiB0aGUgb25SZXRyeSgpIGNhbGxcbiAgICAgICAgaWYgKHNvY2tldC5pc09wZW4pIHtcbiAgICAgICAgICBzb2NrZXQuc2VuZCgncGF1c2UnLCB7fSlcbiAgICAgICAgICBzb2NrZXQuc2VuZCgncmVzdW1lJywge30pXG4gICAgICAgIH1cbiAgICAgIH0pXG5cbiAgICAgIHNvY2tldC5vbigncHJvZ3Jlc3MnLCAocHJvZ3Jlc3NEYXRhKSA9PiBlbWl0U29ja2V0UHJvZ3Jlc3ModGhpcywgcHJvZ3Jlc3NEYXRhLCBmaWxlKSlcblxuICAgICAgc29ja2V0Lm9uKCdlcnJvcicsIChlcnJEYXRhKSA9PiB7XG4gICAgICAgIGNvbnN0IHsgbWVzc2FnZSB9ID0gZXJyRGF0YS5lcnJvclxuICAgICAgICBjb25zdCBlcnJvciA9IE9iamVjdC5hc3NpZ24obmV3IEVycm9yKG1lc3NhZ2UpLCB7IGNhdXNlOiBlcnJEYXRhLmVycm9yIH0pXG5cbiAgICAgICAgLy8gSWYgdGhlIHJlbW90ZSByZXRyeSBvcHRpbWlzYXRpb24gc2hvdWxkIG5vdCBiZSB1c2VkLFxuICAgICAgICAvLyBjbG9zZSB0aGUgc29ja2V04oCUdGhpcyB3aWxsIHRlbGwgY29tcGFuaW9uIHRvIGNsZWFyIHN0YXRlIGFuZCBkZWxldGUgdGhlIGZpbGUuXG4gICAgICAgIGlmICghdGhpcy5vcHRzLnVzZUZhc3RSZW1vdGVSZXRyeSkge1xuICAgICAgICAgIHRoaXMucmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoZmlsZS5pZClcbiAgICAgICAgICAvLyBSZW1vdmUgdGhlIHNlcnZlclRva2VuIHNvIHRoYXQgYSBuZXcgb25lIHdpbGwgYmUgY3JlYXRlZCBmb3IgdGhlIHJldHJ5LlxuICAgICAgICAgIHRoaXMudXBweS5zZXRGaWxlU3RhdGUoZmlsZS5pZCwge1xuICAgICAgICAgICAgc2VydmVyVG9rZW46IG51bGxcbiAgICAgICAgICB9KVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHNvY2tldC5jbG9zZSgpXG4gICAgICAgIH1cblxuICAgICAgICB0aGlzLnVwcHkuZW1pdCgndXBsb2FkLWVycm9yJywgZmlsZSwgZXJyb3IpXG4gICAgICAgIHF1ZXVlZFJlcXVlc3QuZG9uZSgpXG4gICAgICAgIHJlamVjdChlcnJvcilcbiAgICAgIH0pXG5cbiAgICAgIHNvY2tldC5vbignc3VjY2VzcycsIChkYXRhKSA9PiB7XG4gICAgICAgIGNvbnN0IHVwbG9hZFJlc3AgPSB7XG4gICAgICAgICAgdXBsb2FkVVJMOiBkYXRhLnVybFxuICAgICAgICB9XG5cbiAgICAgICAgdGhpcy51cHB5LmVtaXQoJ3VwbG9hZC1zdWNjZXNzJywgZmlsZSwgdXBsb2FkUmVzcClcbiAgICAgICAgdGhpcy5yZXNldFVwbG9hZGVyUmVmZXJlbmNlcyhmaWxlLmlkKVxuICAgICAgICBxdWV1ZWRSZXF1ZXN0LmRvbmUoKVxuICAgICAgICByZXNvbHZlKClcbiAgICAgIH0pXG5cbiAgICAgIGxldCBxdWV1ZWRSZXF1ZXN0ID0gdGhpcy5yZXF1ZXN0cy5ydW4oKCkgPT4ge1xuICAgICAgICBzb2NrZXQub3BlbigpXG4gICAgICAgIGlmIChmaWxlLmlzUGF1c2VkKSB7XG4gICAgICAgICAgc29ja2V0LnNlbmQoJ3BhdXNlJywge30pXG4gICAgICAgIH1cblxuICAgICAgICAvLyBEb24ndCBkbyBhbnl0aGluZyBoZXJlLCB0aGUgY2FsbGVyIHdpbGwgdGFrZSBjYXJlIG9mIGNhbmNlbGxpbmcgdGhlIHVwbG9hZCBpdHNlbGZcbiAgICAgICAgLy8gdXNpbmcgcmVzZXRVcGxvYWRlclJlZmVyZW5jZXMoKS4gVGhpcyBpcyBiZWNhdXNlIHJlc2V0VXBsb2FkZXJSZWZlcmVuY2VzKCkgaGFzIHRvIGJlXG4gICAgICAgIC8vIGNhbGxlZCB3aGVuIHRoaXMgcmVxdWVzdCBpcyBzdGlsbCBpbiB0aGUgcXVldWUsIGFuZCBoYXMgbm90IGJlZW4gc3RhcnRlZCB5ZXQsIHRvby4gQXRcbiAgICAgICAgLy8gdGhhdCBwb2ludCB0aGlzIGNhbmNlbGxhdGlvbiBmdW5jdGlvbiBpcyBub3QgZ29pbmcgdG8gYmUgY2FsbGVkLlxuICAgICAgICAvLyBBbHNvLCB3ZSBuZWVkIHRvIHJlbW92ZSB0aGUgcmVxdWVzdCBmcm9tIHRoZSBxdWV1ZSBfd2l0aG91dF8gZGVzdHJveWluZyBldmVyeXRoaW5nXG4gICAgICAgIC8vIHJlbGF0ZWQgdG8gdGhpcyB1cGxvYWQgdG8gaGFuZGxlIHBhdXNlcy5cbiAgICAgICAgcmV0dXJuICgpID0+IHt9XG4gICAgICB9KVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogU3RvcmUgdGhlIHVwbG9hZFVybCBvbiB0aGUgZmlsZSBvcHRpb25zLCBzbyB0aGF0IHdoZW4gR29sZGVuIFJldHJpZXZlclxuICAgKiByZXN0b3JlcyBzdGF0ZSwgd2Ugd2lsbCBjb250aW51ZSB1cGxvYWRpbmcgdG8gdGhlIGNvcnJlY3QgVVJMLlxuICAgKlxuICAgKiBAcGFyYW0ge1VwcHlGaWxlfSBmaWxlXG4gICAqIEBwYXJhbSB7c3RyaW5nfSB1cGxvYWRVUkxcbiAgICovXG4gIG9uUmVjZWl2ZVVwbG9hZFVybCAoZmlsZSwgdXBsb2FkVVJMKSB7XG4gICAgY29uc3QgY3VycmVudEZpbGUgPSB0aGlzLnVwcHkuZ2V0RmlsZShmaWxlLmlkKVxuICAgIGlmICghY3VycmVudEZpbGUpIHJldHVyblxuICAgIC8vIE9ubHkgZG8gdGhlIHVwZGF0ZSBpZiB3ZSBkaWRuJ3QgaGF2ZSBhbiB1cGxvYWQgVVJMIHlldC5cbiAgICBpZiAoIWN1cnJlbnRGaWxlLnR1cyB8fCBjdXJyZW50RmlsZS50dXMudXBsb2FkVXJsICE9PSB1cGxvYWRVUkwpIHtcbiAgICAgIHRoaXMudXBweS5sb2coJ1tUdXNdIFN0b3JpbmcgdXBsb2FkIHVybCcpXG4gICAgICB0aGlzLnVwcHkuc2V0RmlsZVN0YXRlKGN1cnJlbnRGaWxlLmlkLCB7XG4gICAgICAgIHR1czogT2JqZWN0LmFzc2lnbih7fSwgY3VycmVudEZpbGUudHVzLCB7XG4gICAgICAgICAgdXBsb2FkVXJsOiB1cGxvYWRVUkxcbiAgICAgICAgfSlcbiAgICAgIH0pXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbihzdHJpbmcpOiB2b2lkfSBjYlxuICAgKi9cbiAgb25GaWxlUmVtb3ZlIChmaWxlSUQsIGNiKSB7XG4gICAgdGhpcy51cGxvYWRlckV2ZW50c1tmaWxlSURdLm9uKCdmaWxlLXJlbW92ZWQnLCAoZmlsZSkgPT4ge1xuICAgICAgaWYgKGZpbGVJRCA9PT0gZmlsZS5pZCkgY2IoZmlsZS5pZClcbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbihib29sZWFuKTogdm9pZH0gY2JcbiAgICovXG4gIG9uUGF1c2UgKGZpbGVJRCwgY2IpIHtcbiAgICB0aGlzLnVwbG9hZGVyRXZlbnRzW2ZpbGVJRF0ub24oJ3VwbG9hZC1wYXVzZScsICh0YXJnZXRGaWxlSUQsIGlzUGF1c2VkKSA9PiB7XG4gICAgICBpZiAoZmlsZUlEID09PSB0YXJnZXRGaWxlSUQpIHtcbiAgICAgICAgLy8gY29uc3QgaXNQYXVzZWQgPSB0aGlzLnVwcHkucGF1c2VSZXN1bWUoZmlsZUlEKVxuICAgICAgICBjYihpc1BhdXNlZClcbiAgICAgIH1cbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbigpOiB2b2lkfSBjYlxuICAgKi9cbiAgb25SZXRyeSAoZmlsZUlELCBjYikge1xuICAgIHRoaXMudXBsb2FkZXJFdmVudHNbZmlsZUlEXS5vbigndXBsb2FkLXJldHJ5JywgKHRhcmdldEZpbGVJRCkgPT4ge1xuICAgICAgaWYgKGZpbGVJRCA9PT0gdGFyZ2V0RmlsZUlEKSB7XG4gICAgICAgIGNiKClcbiAgICAgIH1cbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbigpOiB2b2lkfSBjYlxuICAgKi9cbiAgb25SZXRyeUFsbCAoZmlsZUlELCBjYikge1xuICAgIHRoaXMudXBsb2FkZXJFdmVudHNbZmlsZUlEXS5vbigncmV0cnktYWxsJywgKGZpbGVzVG9SZXRyeSkgPT4ge1xuICAgICAgaWYgKCF0aGlzLnVwcHkuZ2V0RmlsZShmaWxlSUQpKSByZXR1cm5cbiAgICAgIGNiKClcbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbigpOiB2b2lkfSBjYlxuICAgKi9cbiAgb25QYXVzZUFsbCAoZmlsZUlELCBjYikge1xuICAgIHRoaXMudXBsb2FkZXJFdmVudHNbZmlsZUlEXS5vbigncGF1c2UtYWxsJywgKCkgPT4ge1xuICAgICAgaWYgKCF0aGlzLnVwcHkuZ2V0RmlsZShmaWxlSUQpKSByZXR1cm5cbiAgICAgIGNiKClcbiAgICB9KVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBmaWxlSURcbiAgICogQHBhcmFtIHtmdW5jdGlvbigpOiB2b2lkfSBjYlxuICAgKi9cbiAgb25DYW5jZWxBbGwgKGZpbGVJRCwgY2IpIHtcbiAgICB0aGlzLnVwbG9hZGVyRXZlbnRzW2ZpbGVJRF0ub24oJ2NhbmNlbC1hbGwnLCAoKSA9PiB7XG4gICAgICBpZiAoIXRoaXMudXBweS5nZXRGaWxlKGZpbGVJRCkpIHJldHVyblxuICAgICAgY2IoKVxuICAgIH0pXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIHtzdHJpbmd9IGZpbGVJRFxuICAgKiBAcGFyYW0ge2Z1bmN0aW9uKCk6IHZvaWR9IGNiXG4gICAqL1xuICBvblJlc3VtZUFsbCAoZmlsZUlELCBjYikge1xuICAgIHRoaXMudXBsb2FkZXJFdmVudHNbZmlsZUlEXS5vbigncmVzdW1lLWFsbCcsICgpID0+IHtcbiAgICAgIGlmICghdGhpcy51cHB5LmdldEZpbGUoZmlsZUlEKSkgcmV0dXJuXG4gICAgICBjYigpXG4gICAgfSlcbiAgfVxuXG4gIC8qKlxuICAgKiBAcGFyYW0geyhVcHB5RmlsZSB8IEZhaWxlZFVwcHlGaWxlKVtdfSBmaWxlc1xuICAgKi9cbiAgdXBsb2FkRmlsZXMgKGZpbGVzKSB7XG4gICAgY29uc3QgcHJvbWlzZXMgPSBmaWxlcy5tYXAoKGZpbGUsIGkpID0+IHtcbiAgICAgIGNvbnN0IGN1cnJlbnQgPSBpICsgMVxuICAgICAgY29uc3QgdG90YWwgPSBmaWxlcy5sZW5ndGhcblxuICAgICAgaWYgKCdlcnJvcicgaW4gZmlsZSAmJiBmaWxlLmVycm9yKSB7XG4gICAgICAgIHJldHVybiBQcm9taXNlLnJlamVjdChuZXcgRXJyb3IoZmlsZS5lcnJvcikpXG4gICAgICB9IGVsc2UgaWYgKGZpbGUuaXNSZW1vdGUpIHtcbiAgICAgICAgcmV0dXJuIHRoaXMudXBsb2FkUmVtb3RlKGZpbGUsIGN1cnJlbnQsIHRvdGFsKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmV0dXJuIHRoaXMudXBsb2FkKGZpbGUsIGN1cnJlbnQsIHRvdGFsKVxuICAgICAgfVxuICAgIH0pXG5cbiAgICByZXR1cm4gc2V0dGxlKHByb21pc2VzKVxuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSB7c3RyaW5nW119IGZpbGVJRHNcbiAgICovXG4gIGhhbmRsZVVwbG9hZCAoZmlsZUlEcykge1xuICAgIGlmIChmaWxlSURzLmxlbmd0aCA9PT0gMCkge1xuICAgICAgdGhpcy51cHB5LmxvZygnW1R1c10gTm8gZmlsZXMgdG8gdXBsb2FkJylcbiAgICAgIHJldHVybiBQcm9taXNlLnJlc29sdmUoKVxuICAgIH1cblxuICAgIGlmICh0aGlzLm9wdHMubGltaXQgPT09IDApIHtcbiAgICAgIHRoaXMudXBweS5sb2coXG4gICAgICAgICdbVHVzXSBXaGVuIHVwbG9hZGluZyBtdWx0aXBsZSBmaWxlcyBhdCBvbmNlLCBjb25zaWRlciBzZXR0aW5nIHRoZSBgbGltaXRgIG9wdGlvbiAodG8gYDEwYCBmb3IgZXhhbXBsZSksIHRvIGxpbWl0IHRoZSBudW1iZXIgb2YgY29uY3VycmVudCB1cGxvYWRzLCB3aGljaCBoZWxwcyBwcmV2ZW50IG1lbW9yeSBhbmQgbmV0d29yayBpc3N1ZXM6IGh0dHBzOi8vdXBweS5pby9kb2NzL3R1cy8jbGltaXQtMCcsXG4gICAgICAgICd3YXJuaW5nJ1xuICAgICAgKVxuICAgIH1cblxuICAgIHRoaXMudXBweS5sb2coJ1tUdXNdIFVwbG9hZGluZy4uLicpXG4gICAgY29uc3QgZmlsZXNUb1VwbG9hZCA9IGZpbGVJRHMubWFwKChmaWxlSUQpID0+IHRoaXMudXBweS5nZXRGaWxlKGZpbGVJRCkpXG5cbiAgICByZXR1cm4gdGhpcy51cGxvYWRGaWxlcyhmaWxlc1RvVXBsb2FkKVxuICAgICAgLnRoZW4oKCkgPT4gbnVsbClcbiAgfVxuXG4gIGluc3RhbGwgKCkge1xuICAgIHRoaXMudXBweS5zZXRTdGF0ZSh7XG4gICAgICBjYXBhYmlsaXRpZXM6IE9iamVjdC5hc3NpZ24oe30sIHRoaXMudXBweS5nZXRTdGF0ZSgpLmNhcGFiaWxpdGllcywge1xuICAgICAgICByZXN1bWFibGVVcGxvYWRzOiB0cnVlXG4gICAgICB9KVxuICAgIH0pXG4gICAgdGhpcy51cHB5LmFkZFVwbG9hZGVyKHRoaXMuaGFuZGxlVXBsb2FkKVxuXG4gICAgdGhpcy51cHB5Lm9uKCdyZXNldC1wcm9ncmVzcycsIHRoaXMuaGFuZGxlUmVzZXRQcm9ncmVzcylcblxuICAgIGlmICh0aGlzLm9wdHMuYXV0b1JldHJ5KSB7XG4gICAgICB0aGlzLnVwcHkub24oJ2JhY2stb25saW5lJywgdGhpcy51cHB5LnJldHJ5QWxsKVxuICAgIH1cbiAgfVxuXG4gIHVuaW5zdGFsbCAoKSB7XG4gICAgdGhpcy51cHB5LnNldFN0YXRlKHtcbiAgICAgIGNhcGFiaWxpdGllczogT2JqZWN0LmFzc2lnbih7fSwgdGhpcy51cHB5LmdldFN0YXRlKCkuY2FwYWJpbGl0aWVzLCB7XG4gICAgICAgIHJlc3VtYWJsZVVwbG9hZHM6IGZhbHNlXG4gICAgICB9KVxuICAgIH0pXG4gICAgdGhpcy51cHB5LnJlbW92ZVVwbG9hZGVyKHRoaXMuaGFuZGxlVXBsb2FkKVxuXG4gICAgaWYgKHRoaXMub3B0cy5hdXRvUmV0cnkpIHtcbiAgICAgIHRoaXMudXBweS5vZmYoJ2JhY2stb25saW5lJywgdGhpcy51cHB5LnJldHJ5QWxsKVxuICAgIH1cbiAgfVxufVxuIiwiLyoqXG4gKiBDcmVhdGUgYSB3cmFwcGVyIGFyb3VuZCBhbiBldmVudCBlbWl0dGVyIHdpdGggYSBgcmVtb3ZlYCBtZXRob2QgdG8gcmVtb3ZlXG4gKiBhbGwgZXZlbnRzIHRoYXQgd2VyZSBhZGRlZCB1c2luZyB0aGUgd3JhcHBlZCBlbWl0dGVyLlxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGNsYXNzIEV2ZW50VHJhY2tlciB7XG4gIGNvbnN0cnVjdG9yIChlbWl0dGVyKSB7XG4gICAgdGhpcy5fZXZlbnRzID0gW11cbiAgICB0aGlzLl9lbWl0dGVyID0gZW1pdHRlclxuICB9XG5cbiAgb24gKGV2ZW50LCBmbikge1xuICAgIHRoaXMuX2V2ZW50cy5wdXNoKFtldmVudCwgZm5dKVxuICAgIHJldHVybiB0aGlzLl9lbWl0dGVyLm9uKGV2ZW50LCBmbilcbiAgfVxuXG4gIHJlbW92ZSAoKSB7XG4gICAgdGhpcy5fZXZlbnRzLmZvckVhY2goKFtldmVudCwgZm5dKSA9PiB7XG4gICAgICB0aGlzLl9lbWl0dGVyLm9mZihldmVudCwgZm4pXG4gICAgfSlcbiAgfVxufVxuIiwibW9kdWxlLmV4cG9ydHMgPSBjbGFzcyBSYXRlTGltaXRlZFF1ZXVlIHtcbiAgY29uc3RydWN0b3IgKGxpbWl0KSB7XG4gICAgaWYgKHR5cGVvZiBsaW1pdCAhPT0gJ251bWJlcicgfHwgbGltaXQgPT09IDApIHtcbiAgICAgIHRoaXMubGltaXQgPSBJbmZpbml0eVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmxpbWl0ID0gbGltaXRcbiAgICB9XG5cbiAgICB0aGlzLmFjdGl2ZVJlcXVlc3RzID0gMFxuICAgIHRoaXMucXVldWVkSGFuZGxlcnMgPSBbXVxuICB9XG5cbiAgX2NhbGwgKGZuKSB7XG4gICAgdGhpcy5hY3RpdmVSZXF1ZXN0cyArPSAxXG5cbiAgICBsZXQgZG9uZSA9IGZhbHNlXG5cbiAgICBsZXQgY2FuY2VsQWN0aXZlXG4gICAgdHJ5IHtcbiAgICAgIGNhbmNlbEFjdGl2ZSA9IGZuKClcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIHRoaXMuYWN0aXZlUmVxdWVzdHMgLT0gMVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIGFib3J0OiAoKSA9PiB7XG4gICAgICAgIGlmIChkb25lKSByZXR1cm5cbiAgICAgICAgZG9uZSA9IHRydWVcbiAgICAgICAgdGhpcy5hY3RpdmVSZXF1ZXN0cyAtPSAxXG4gICAgICAgIGNhbmNlbEFjdGl2ZSgpXG4gICAgICAgIHRoaXMuX3F1ZXVlTmV4dCgpXG4gICAgICB9LFxuXG4gICAgICBkb25lOiAoKSA9PiB7XG4gICAgICAgIGlmIChkb25lKSByZXR1cm5cbiAgICAgICAgZG9uZSA9IHRydWVcbiAgICAgICAgdGhpcy5hY3RpdmVSZXF1ZXN0cyAtPSAxXG4gICAgICAgIHRoaXMuX3F1ZXVlTmV4dCgpXG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgX3F1ZXVlTmV4dCAoKSB7XG4gICAgLy8gRG8gaXQgc29vbiBidXQgbm90IGltbWVkaWF0ZWx5LCB0aGlzIGFsbG93cyBjbGVhcmluZyBvdXQgdGhlIGVudGlyZSBxdWV1ZSBzeW5jaHJvbm91c2x5XG4gICAgLy8gb25lIGJ5IG9uZSB3aXRob3V0IGNvbnRpbnVvdXNseSBfYWR2YW5jaW5nXyBpdCAoYW5kIHN0YXJ0aW5nIG5ldyB0YXNrcyBiZWZvcmUgaW1tZWRpYXRlbHlcbiAgICAvLyBhYm9ydGluZyB0aGVtKVxuICAgIFByb21pc2UucmVzb2x2ZSgpLnRoZW4oKCkgPT4ge1xuICAgICAgdGhpcy5fbmV4dCgpXG4gICAgfSlcbiAgfVxuXG4gIF9uZXh0ICgpIHtcbiAgICBpZiAodGhpcy5hY3RpdmVSZXF1ZXN0cyA+PSB0aGlzLmxpbWl0KSB7XG4gICAgICByZXR1cm5cbiAgICB9XG4gICAgaWYgKHRoaXMucXVldWVkSGFuZGxlcnMubGVuZ3RoID09PSAwKSB7XG4gICAgICByZXR1cm5cbiAgICB9XG5cbiAgICAvLyBEaXNwYXRjaCB0aGUgbmV4dCByZXF1ZXN0LCBhbmQgdXBkYXRlIHRoZSBhYm9ydC9kb25lIGhhbmRsZXJzXG4gICAgLy8gc28gdGhhdCBjYW5jZWxsaW5nIGl0IGRvZXMgdGhlIFJpZ2h0IFRoaW5nIChhbmQgZG9lc24ndCBqdXN0IHRyeVxuICAgIC8vIHRvIGRlcXVldWUgYW4gYWxyZWFkeS1ydW5uaW5nIHJlcXVlc3QpLlxuICAgIGNvbnN0IG5leHQgPSB0aGlzLnF1ZXVlZEhhbmRsZXJzLnNoaWZ0KClcbiAgICBjb25zdCBoYW5kbGVyID0gdGhpcy5fY2FsbChuZXh0LmZuKVxuICAgIG5leHQuYWJvcnQgPSBoYW5kbGVyLmFib3J0XG4gICAgbmV4dC5kb25lID0gaGFuZGxlci5kb25lXG4gIH1cblxuICBfcXVldWUgKGZuKSB7XG4gICAgY29uc3QgaGFuZGxlciA9IHtcbiAgICAgIGZuLFxuICAgICAgYWJvcnQ6ICgpID0+IHtcbiAgICAgICAgdGhpcy5fZGVxdWV1ZShoYW5kbGVyKVxuICAgICAgfSxcbiAgICAgIGRvbmU6ICgpID0+IHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKCdDYW5ub3QgbWFyayBhIHF1ZXVlZCByZXF1ZXN0IGFzIGRvbmU6IHRoaXMgaW5kaWNhdGVzIGEgYnVnJylcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5xdWV1ZWRIYW5kbGVycy5wdXNoKGhhbmRsZXIpXG4gICAgcmV0dXJuIGhhbmRsZXJcbiAgfVxuXG4gIF9kZXF1ZXVlIChoYW5kbGVyKSB7XG4gICAgY29uc3QgaW5kZXggPSB0aGlzLnF1ZXVlZEhhbmRsZXJzLmluZGV4T2YoaGFuZGxlcilcbiAgICBpZiAoaW5kZXggIT09IC0xKSB7XG4gICAgICB0aGlzLnF1ZXVlZEhhbmRsZXJzLnNwbGljZShpbmRleCwgMSlcbiAgICB9XG4gIH1cblxuICBydW4gKGZuKSB7XG4gICAgaWYgKHRoaXMuYWN0aXZlUmVxdWVzdHMgPCB0aGlzLmxpbWl0KSB7XG4gICAgICByZXR1cm4gdGhpcy5fY2FsbChmbilcbiAgICB9XG4gICAgcmV0dXJuIHRoaXMuX3F1ZXVlKGZuKVxuICB9XG5cbiAgd3JhcFByb21pc2VGdW5jdGlvbiAoZm4pIHtcbiAgICByZXR1cm4gKC4uLmFyZ3MpID0+IG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgIGNvbnN0IHF1ZXVlZFJlcXVlc3QgPSB0aGlzLnJ1bigoKSA9PiB7XG4gICAgICAgIGxldCBjYW5jZWxFcnJvclxuICAgICAgICBsZXQgcHJvbWlzZVxuICAgICAgICB0cnkge1xuICAgICAgICAgIHByb21pc2UgPSBQcm9taXNlLnJlc29sdmUoZm4oLi4uYXJncykpXG4gICAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICAgIHByb21pc2UgPSBQcm9taXNlLnJlamVjdChlcnIpXG4gICAgICAgIH1cblxuICAgICAgICBwcm9taXNlLnRoZW4oKHJlc3VsdCkgPT4ge1xuICAgICAgICAgIGlmIChjYW5jZWxFcnJvcikge1xuICAgICAgICAgICAgcmVqZWN0KGNhbmNlbEVycm9yKVxuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBxdWV1ZWRSZXF1ZXN0LmRvbmUoKVxuICAgICAgICAgICAgcmVzb2x2ZShyZXN1bHQpXG4gICAgICAgICAgfVxuICAgICAgICB9LCAoZXJyKSA9PiB7XG4gICAgICAgICAgaWYgKGNhbmNlbEVycm9yKSB7XG4gICAgICAgICAgICByZWplY3QoY2FuY2VsRXJyb3IpXG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHF1ZXVlZFJlcXVlc3QuZG9uZSgpXG4gICAgICAgICAgICByZWplY3QoZXJyKVxuICAgICAgICAgIH1cbiAgICAgICAgfSlcblxuICAgICAgICByZXR1cm4gKCkgPT4ge1xuICAgICAgICAgIGNhbmNlbEVycm9yID0gbmV3IEVycm9yKCdDYW5jZWxsZWQnKVxuICAgICAgICB9XG4gICAgICB9KVxuICAgIH0pXG4gIH1cbn1cbiIsImNvbnN0IGhhcyA9IHJlcXVpcmUoJy4vaGFzUHJvcGVydHknKVxuXG4vKipcbiAqIFRyYW5zbGF0ZXMgc3RyaW5ncyB3aXRoIGludGVycG9sYXRpb24gJiBwbHVyYWxpemF0aW9uIHN1cHBvcnQuXG4gKiBFeHRlbnNpYmxlIHdpdGggY3VzdG9tIGRpY3Rpb25hcmllcyBhbmQgcGx1cmFsaXphdGlvbiBmdW5jdGlvbnMuXG4gKlxuICogQm9ycm93cyBoZWF2aWx5IGZyb20gYW5kIGluc3BpcmVkIGJ5IFBvbHlnbG90IGh0dHBzOi8vZ2l0aHViLmNvbS9haXJibmIvcG9seWdsb3QuanMsXG4gKiBiYXNpY2FsbHkgYSBzdHJpcHBlZC1kb3duIHZlcnNpb24gb2YgaXQuIERpZmZlcmVuY2VzOiBwbHVyYWxpemF0aW9uIGZ1bmN0aW9ucyBhcmUgbm90IGhhcmRjb2RlZFxuICogYW5kIGNhbiBiZSBlYXNpbHkgYWRkZWQgYW1vbmcgd2l0aCBkaWN0aW9uYXJpZXMsIG5lc3RlZCBvYmplY3RzIGFyZSB1c2VkIGZvciBwbHVyYWxpemF0aW9uXG4gKiBhcyBvcHBvc2VkIHRvIGB8fHx8YCBkZWxpbWV0ZXJcbiAqXG4gKiBVc2FnZSBleGFtcGxlOiBgdHJhbnNsYXRvci50cmFuc2xhdGUoJ2ZpbGVzX2Nob3NlbicsIHtzbWFydF9jb3VudDogM30pYFxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGNsYXNzIFRyYW5zbGF0b3Ige1xuICAvKipcbiAgICogQHBhcmFtIHtvYmplY3R8QXJyYXk8b2JqZWN0Pn0gbG9jYWxlcyAtIGxvY2FsZSBvciBsaXN0IG9mIGxvY2FsZXMuXG4gICAqL1xuICBjb25zdHJ1Y3RvciAobG9jYWxlcykge1xuICAgIHRoaXMubG9jYWxlID0ge1xuICAgICAgc3RyaW5nczoge30sXG4gICAgICBwbHVyYWxpemU6IGZ1bmN0aW9uIChuKSB7XG4gICAgICAgIGlmIChuID09PSAxKSB7XG4gICAgICAgICAgcmV0dXJuIDBcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gMVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmIChBcnJheS5pc0FycmF5KGxvY2FsZXMpKSB7XG4gICAgICBsb2NhbGVzLmZvckVhY2goKGxvY2FsZSkgPT4gdGhpcy5fYXBwbHkobG9jYWxlKSlcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5fYXBwbHkobG9jYWxlcylcbiAgICB9XG4gIH1cblxuICBfYXBwbHkgKGxvY2FsZSkge1xuICAgIGlmICghbG9jYWxlIHx8ICFsb2NhbGUuc3RyaW5ncykge1xuICAgICAgcmV0dXJuXG4gICAgfVxuXG4gICAgY29uc3QgcHJldkxvY2FsZSA9IHRoaXMubG9jYWxlXG4gICAgdGhpcy5sb2NhbGUgPSBPYmplY3QuYXNzaWduKHt9LCBwcmV2TG9jYWxlLCB7XG4gICAgICBzdHJpbmdzOiBPYmplY3QuYXNzaWduKHt9LCBwcmV2TG9jYWxlLnN0cmluZ3MsIGxvY2FsZS5zdHJpbmdzKVxuICAgIH0pXG4gICAgdGhpcy5sb2NhbGUucGx1cmFsaXplID0gbG9jYWxlLnBsdXJhbGl6ZSB8fCBwcmV2TG9jYWxlLnBsdXJhbGl6ZVxuICB9XG5cbiAgLyoqXG4gICAqIFRha2VzIGEgc3RyaW5nIHdpdGggcGxhY2Vob2xkZXIgdmFyaWFibGVzIGxpa2UgYCV7c21hcnRfY291bnR9IGZpbGUgc2VsZWN0ZWRgXG4gICAqIGFuZCByZXBsYWNlcyBpdCB3aXRoIHZhbHVlcyBmcm9tIG9wdGlvbnMgYHtzbWFydF9jb3VudDogNX1gXG4gICAqXG4gICAqIEBsaWNlbnNlIGh0dHBzOi8vZ2l0aHViLmNvbS9haXJibmIvcG9seWdsb3QuanMvYmxvYi9tYXN0ZXIvTElDRU5TRVxuICAgKiB0YWtlbiBmcm9tIGh0dHBzOi8vZ2l0aHViLmNvbS9haXJibmIvcG9seWdsb3QuanMvYmxvYi9tYXN0ZXIvbGliL3BvbHlnbG90LmpzI0wyOTlcbiAgICpcbiAgICogQHBhcmFtIHtzdHJpbmd9IHBocmFzZSB0aGF0IG5lZWRzIGludGVycG9sYXRpb24sIHdpdGggcGxhY2Vob2xkZXJzXG4gICAqIEBwYXJhbSB7b2JqZWN0fSBvcHRpb25zIHdpdGggdmFsdWVzIHRoYXQgd2lsbCBiZSB1c2VkIHRvIHJlcGxhY2UgcGxhY2Vob2xkZXJzXG4gICAqIEByZXR1cm5zIHtzdHJpbmd9IGludGVycG9sYXRlZFxuICAgKi9cbiAgaW50ZXJwb2xhdGUgKHBocmFzZSwgb3B0aW9ucykge1xuICAgIGNvbnN0IHsgc3BsaXQsIHJlcGxhY2UgfSA9IFN0cmluZy5wcm90b3R5cGVcbiAgICBjb25zdCBkb2xsYXJSZWdleCA9IC9cXCQvZ1xuICAgIGNvbnN0IGRvbGxhckJpbGxzWWFsbCA9ICckJCQkJ1xuICAgIGxldCBpbnRlcnBvbGF0ZWQgPSBbcGhyYXNlXVxuXG4gICAgZm9yIChjb25zdCBhcmcgaW4gb3B0aW9ucykge1xuICAgICAgaWYgKGFyZyAhPT0gJ18nICYmIGhhcyhvcHRpb25zLCBhcmcpKSB7XG4gICAgICAgIC8vIEVuc3VyZSByZXBsYWNlbWVudCB2YWx1ZSBpcyBlc2NhcGVkIHRvIHByZXZlbnQgc3BlY2lhbCAkLXByZWZpeGVkXG4gICAgICAgIC8vIHJlZ2V4IHJlcGxhY2UgdG9rZW5zLiB0aGUgXCIkJCQkXCIgaXMgbmVlZGVkIGJlY2F1c2UgZWFjaCBcIiRcIiBuZWVkcyB0b1xuICAgICAgICAvLyBiZSBlc2NhcGVkIHdpdGggXCIkXCIgaXRzZWxmLCBhbmQgd2UgbmVlZCB0d28gaW4gdGhlIHJlc3VsdGluZyBvdXRwdXQuXG4gICAgICAgIHZhciByZXBsYWNlbWVudCA9IG9wdGlvbnNbYXJnXVxuICAgICAgICBpZiAodHlwZW9mIHJlcGxhY2VtZW50ID09PSAnc3RyaW5nJykge1xuICAgICAgICAgIHJlcGxhY2VtZW50ID0gcmVwbGFjZS5jYWxsKG9wdGlvbnNbYXJnXSwgZG9sbGFyUmVnZXgsIGRvbGxhckJpbGxzWWFsbClcbiAgICAgICAgfVxuICAgICAgICAvLyBXZSBjcmVhdGUgYSBuZXcgYFJlZ0V4cGAgZWFjaCB0aW1lIGluc3RlYWQgb2YgdXNpbmcgYSBtb3JlLWVmZmljaWVudFxuICAgICAgICAvLyBzdHJpbmcgcmVwbGFjZSBzbyB0aGF0IHRoZSBzYW1lIGFyZ3VtZW50IGNhbiBiZSByZXBsYWNlZCBtdWx0aXBsZSB0aW1lc1xuICAgICAgICAvLyBpbiB0aGUgc2FtZSBwaHJhc2UuXG4gICAgICAgIGludGVycG9sYXRlZCA9IGluc2VydFJlcGxhY2VtZW50KGludGVycG9sYXRlZCwgbmV3IFJlZ0V4cCgnJVxcXFx7JyArIGFyZyArICdcXFxcfScsICdnJyksIHJlcGxhY2VtZW50KVxuICAgICAgfVxuICAgIH1cblxuICAgIHJldHVybiBpbnRlcnBvbGF0ZWRcblxuICAgIGZ1bmN0aW9uIGluc2VydFJlcGxhY2VtZW50IChzb3VyY2UsIHJ4LCByZXBsYWNlbWVudCkge1xuICAgICAgY29uc3QgbmV3UGFydHMgPSBbXVxuICAgICAgc291cmNlLmZvckVhY2goKGNodW5rKSA9PiB7XG4gICAgICAgIHNwbGl0LmNhbGwoY2h1bmssIHJ4KS5mb3JFYWNoKChyYXcsIGksIGxpc3QpID0+IHtcbiAgICAgICAgICBpZiAocmF3ICE9PSAnJykge1xuICAgICAgICAgICAgbmV3UGFydHMucHVzaChyYXcpXG4gICAgICAgICAgfVxuXG4gICAgICAgICAgLy8gSW50ZXJsYWNlIHdpdGggdGhlIGByZXBsYWNlbWVudGAgdmFsdWVcbiAgICAgICAgICBpZiAoaSA8IGxpc3QubGVuZ3RoIC0gMSkge1xuICAgICAgICAgICAgbmV3UGFydHMucHVzaChyZXBsYWNlbWVudClcbiAgICAgICAgICB9XG4gICAgICAgIH0pXG4gICAgICB9KVxuICAgICAgcmV0dXJuIG5ld1BhcnRzXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFB1YmxpYyB0cmFuc2xhdGUgbWV0aG9kXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBrZXlcbiAgICogQHBhcmFtIHtvYmplY3R9IG9wdGlvbnMgd2l0aCB2YWx1ZXMgdGhhdCB3aWxsIGJlIHVzZWQgbGF0ZXIgdG8gcmVwbGFjZSBwbGFjZWhvbGRlcnMgaW4gc3RyaW5nXG4gICAqIEByZXR1cm5zIHtzdHJpbmd9IHRyYW5zbGF0ZWQgKGFuZCBpbnRlcnBvbGF0ZWQpXG4gICAqL1xuICB0cmFuc2xhdGUgKGtleSwgb3B0aW9ucykge1xuICAgIHJldHVybiB0aGlzLnRyYW5zbGF0ZUFycmF5KGtleSwgb3B0aW9ucykuam9pbignJylcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgYSB0cmFuc2xhdGlvbiBhbmQgcmV0dXJuIHRoZSB0cmFuc2xhdGVkIGFuZCBpbnRlcnBvbGF0ZWQgcGFydHMgYXMgYW4gYXJyYXkuXG4gICAqXG4gICAqIEBwYXJhbSB7c3RyaW5nfSBrZXlcbiAgICogQHBhcmFtIHtvYmplY3R9IG9wdGlvbnMgd2l0aCB2YWx1ZXMgdGhhdCB3aWxsIGJlIHVzZWQgdG8gcmVwbGFjZSBwbGFjZWhvbGRlcnNcbiAgICogQHJldHVybnMge0FycmF5fSBUaGUgdHJhbnNsYXRlZCBhbmQgaW50ZXJwb2xhdGVkIHBhcnRzLCBpbiBvcmRlci5cbiAgICovXG4gIHRyYW5zbGF0ZUFycmF5IChrZXksIG9wdGlvbnMpIHtcbiAgICBpZiAob3B0aW9ucyAmJiB0eXBlb2Ygb3B0aW9ucy5zbWFydF9jb3VudCAhPT0gJ3VuZGVmaW5lZCcpIHtcbiAgICAgIHZhciBwbHVyYWwgPSB0aGlzLmxvY2FsZS5wbHVyYWxpemUob3B0aW9ucy5zbWFydF9jb3VudClcbiAgICAgIHJldHVybiB0aGlzLmludGVycG9sYXRlKHRoaXMubG9jYWxlLnN0cmluZ3Nba2V5XVtwbHVyYWxdLCBvcHRpb25zKVxuICAgIH1cblxuICAgIHJldHVybiB0aGlzLmludGVycG9sYXRlKHRoaXMubG9jYWxlLnN0cmluZ3Nba2V5XSwgb3B0aW9ucylcbiAgfVxufVxuIiwiY29uc3QgdGhyb3R0bGUgPSByZXF1aXJlKCdsb2Rhc2gudGhyb3R0bGUnKVxuXG5mdW5jdGlvbiBfZW1pdFNvY2tldFByb2dyZXNzICh1cGxvYWRlciwgcHJvZ3Jlc3NEYXRhLCBmaWxlKSB7XG4gIGNvbnN0IHsgcHJvZ3Jlc3MsIGJ5dGVzVXBsb2FkZWQsIGJ5dGVzVG90YWwgfSA9IHByb2dyZXNzRGF0YVxuICBpZiAocHJvZ3Jlc3MpIHtcbiAgICB1cGxvYWRlci51cHB5LmxvZyhgVXBsb2FkIHByb2dyZXNzOiAke3Byb2dyZXNzfWApXG4gICAgdXBsb2FkZXIudXBweS5lbWl0KCd1cGxvYWQtcHJvZ3Jlc3MnLCBmaWxlLCB7XG4gICAgICB1cGxvYWRlcixcbiAgICAgIGJ5dGVzVXBsb2FkZWQ6IGJ5dGVzVXBsb2FkZWQsXG4gICAgICBieXRlc1RvdGFsOiBieXRlc1RvdGFsXG4gICAgfSlcbiAgfVxufVxuXG5tb2R1bGUuZXhwb3J0cyA9IHRocm90dGxlKF9lbWl0U29ja2V0UHJvZ3Jlc3MsIDMwMCwge1xuICBsZWFkaW5nOiB0cnVlLFxuICB0cmFpbGluZzogdHJ1ZVxufSlcbiIsImNvbnN0IGlzRE9NRWxlbWVudCA9IHJlcXVpcmUoJy4vaXNET01FbGVtZW50JylcblxuLyoqXG4gKiBGaW5kIGEgRE9NIGVsZW1lbnQuXG4gKlxuICogQHBhcmFtIHtOb2RlfHN0cmluZ30gZWxlbWVudFxuICogQHJldHVybnMge05vZGV8bnVsbH1cbiAqL1xubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBmaW5kRE9NRWxlbWVudCAoZWxlbWVudCwgY29udGV4dCA9IGRvY3VtZW50KSB7XG4gIGlmICh0eXBlb2YgZWxlbWVudCA9PT0gJ3N0cmluZycpIHtcbiAgICByZXR1cm4gY29udGV4dC5xdWVyeVNlbGVjdG9yKGVsZW1lbnQpXG4gIH1cblxuICBpZiAodHlwZW9mIGVsZW1lbnQgPT09ICdvYmplY3QnICYmIGlzRE9NRWxlbWVudChlbGVtZW50KSkge1xuICAgIHJldHVybiBlbGVtZW50XG4gIH1cbn1cbiIsIi8qKlxuICogVGFrZXMgYSBmaWxlIG9iamVjdCBhbmQgdHVybnMgaXQgaW50byBmaWxlSUQsIGJ5IGNvbnZlcnRpbmcgZmlsZS5uYW1lIHRvIGxvd2VyY2FzZSxcbiAqIHJlbW92aW5nIGV4dHJhIGNoYXJhY3RlcnMgYW5kIGFkZGluZyB0eXBlLCBzaXplIGFuZCBsYXN0TW9kaWZpZWRcbiAqXG4gKiBAcGFyYW0ge29iamVjdH0gZmlsZVxuICogQHJldHVybnMge3N0cmluZ30gdGhlIGZpbGVJRFxuICpcbiAqL1xubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBnZW5lcmF0ZUZpbGVJRCAoZmlsZSkge1xuICAvLyBmaWx0ZXIgaXMgbmVlZGVkIHRvIG5vdCBqb2luIGVtcHR5IHZhbHVlcyB3aXRoIGAtYFxuICByZXR1cm4gW1xuICAgICd1cHB5JyxcbiAgICBmaWxlLm5hbWUgPyBlbmNvZGVGaWxlbmFtZShmaWxlLm5hbWUudG9Mb3dlckNhc2UoKSkgOiAnJyxcbiAgICBmaWxlLnR5cGUsXG4gICAgZmlsZS5tZXRhICYmIGZpbGUubWV0YS5yZWxhdGl2ZVBhdGggPyBlbmNvZGVGaWxlbmFtZShmaWxlLm1ldGEucmVsYXRpdmVQYXRoLnRvTG93ZXJDYXNlKCkpIDogJycsXG4gICAgZmlsZS5kYXRhLnNpemUsXG4gICAgZmlsZS5kYXRhLmxhc3RNb2RpZmllZFxuICBdLmZpbHRlcih2YWwgPT4gdmFsKS5qb2luKCctJylcbn1cblxuZnVuY3Rpb24gZW5jb2RlRmlsZW5hbWUgKG5hbWUpIHtcbiAgbGV0IHN1ZmZpeCA9ICcnXG4gIHJldHVybiBuYW1lLnJlcGxhY2UoL1teQS1aMC05XS9pZywgKGNoYXJhY3RlcikgPT4ge1xuICAgIHN1ZmZpeCArPSAnLScgKyBlbmNvZGVDaGFyYWN0ZXIoY2hhcmFjdGVyKVxuICAgIHJldHVybiAnLydcbiAgfSkgKyBzdWZmaXhcbn1cblxuZnVuY3Rpb24gZW5jb2RlQ2hhcmFjdGVyIChjaGFyYWN0ZXIpIHtcbiAgcmV0dXJuIGNoYXJhY3Rlci5jaGFyQ29kZUF0KDApLnRvU3RyaW5nKDMyKVxufVxuIiwiY29uc3Qgd2Via2l0R2V0QXNFbnRyeUFwaSA9IHJlcXVpcmUoJy4vdXRpbHMvd2Via2l0R2V0QXNFbnRyeUFwaS9pbmRleCcpXG5jb25zdCBmYWxsYmFja0FwaSA9IHJlcXVpcmUoJy4vdXRpbHMvZmFsbGJhY2tBcGknKVxuXG4vKipcbiAqIFJldHVybnMgYSBwcm9taXNlIHRoYXQgcmVzb2x2ZXMgdG8gdGhlIGFycmF5IG9mIGRyb3BwZWQgZmlsZXMgKGlmIGEgZm9sZGVyIGlzIGRyb3BwZWQsIGFuZCBicm93c2VyIHN1cHBvcnRzIGZvbGRlciBwYXJzaW5nIC0gcHJvbWlzZSByZXNvbHZlcyB0byB0aGUgZmxhdCBhcnJheSBvZiBhbGwgZmlsZXMgaW4gYWxsIGRpcmVjdG9yaWVzKS5cbiAqIEVhY2ggZmlsZSBoYXMgLnJlbGF0aXZlUGF0aCBwcm9wIGFwcGVuZGVkIHRvIGl0IChlLmcuIFwiL2RvY3MvUHJhZ3VlL3RpY2tldF9mcm9tX3ByYWd1ZV90b191ZmEucGRmXCIpIGlmIGJyb3dzZXIgc3VwcG9ydHMgaXQuIE90aGVyd2lzZSBpdCdzIHVuZGVmaW5lZC5cbiAqXG4gKiBAcGFyYW0ge0RhdGFUcmFuc2Zlcn0gZGF0YVRyYW5zZmVyXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBsb2dEcm9wRXJyb3IgLSBhIGZ1bmN0aW9uIHRoYXQncyBjYWxsZWQgZXZlcnkgdGltZSBzb21lIGZvbGRlciBvciBzb21lIGZpbGUgZXJyb3Igb3V0IChlLmcuIGJlY2F1c2Ugb2YgdGhlIGZvbGRlciBuYW1lIGJlaW5nIHRvbyBsb25nIG9uIFdpbmRvd3MpLiBOb3RpY2UgdGhhdCByZXN1bHRpbmcgcHJvbWlzZSB3aWxsIGFsd2F5cyBiZSByZXNvbHZlZCBhbnl3YXkuXG4gKlxuICogQHJldHVybnMge1Byb21pc2V9IC0gQXJyYXk8RmlsZT5cbiAqL1xubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBnZXREcm9wcGVkRmlsZXMgKGRhdGFUcmFuc2ZlciwgeyBsb2dEcm9wRXJyb3IgPSAoKSA9PiB7fSB9ID0ge30pIHtcbiAgLy8gR2V0IGFsbCBmaWxlcyBmcm9tIGFsbCBzdWJkaXJzLiBXb3JrcyAoYXQgbGVhc3QpIGluIENocm9tZSwgTW96aWxsYSwgYW5kIFNhZmFyaVxuICBpZiAoZGF0YVRyYW5zZmVyLml0ZW1zICYmIGRhdGFUcmFuc2Zlci5pdGVtc1swXSAmJiAnd2Via2l0R2V0QXNFbnRyeScgaW4gZGF0YVRyYW5zZmVyLml0ZW1zWzBdKSB7XG4gICAgcmV0dXJuIHdlYmtpdEdldEFzRW50cnlBcGkoZGF0YVRyYW5zZmVyLCBsb2dEcm9wRXJyb3IpXG4gIC8vIE90aGVyd2lzZSBqdXN0IHJldHVybiBhbGwgZmlyc3Qtb3JkZXIgZmlsZXNcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gZmFsbGJhY2tBcGkoZGF0YVRyYW5zZmVyKVxuICB9XG59XG4iLCJjb25zdCB0b0FycmF5ID0gcmVxdWlyZSgnLi4vLi4vdG9BcnJheScpXG5cbi8vIC5maWxlcyBmYWxsYmFjaywgc2hvdWxkIGJlIGltcGxlbWVudGVkIGluIGFueSBicm93c2VyXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGZhbGxiYWNrQXBpIChkYXRhVHJhbnNmZXIpIHtcbiAgY29uc3QgZmlsZXMgPSB0b0FycmF5KGRhdGFUcmFuc2Zlci5maWxlcylcbiAgcmV0dXJuIFByb21pc2UucmVzb2x2ZShmaWxlcylcbn1cbiIsIi8qKlxuICogUmVjdXJzaXZlIGZ1bmN0aW9uLCBjYWxscyB0aGUgb3JpZ2luYWwgY2FsbGJhY2soKSB3aGVuIHRoZSBkaXJlY3RvcnkgaXMgZW50aXJlbHkgcGFyc2VkLlxuICpcbiAqIEBwYXJhbSB7RmlsZVN5c3RlbURpcmVjdG9yeVJlYWRlcn0gZGlyZWN0b3J5UmVhZGVyXG4gKiBAcGFyYW0ge0FycmF5fSBvbGRFbnRyaWVzXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBsb2dEcm9wRXJyb3JcbiAqIEBwYXJhbSB7RnVuY3Rpb259IGNhbGxiYWNrIC0gY2FsbGVkIHdpdGggKFsgYWxsIGZpbGVzIGFuZCBkaXJlY3RvcmllcyBpbiB0aGF0IGRpcmVjdG9yeVJlYWRlciBdKVxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGdldEZpbGVzQW5kRGlyZWN0b3JpZXNGcm9tRGlyZWN0b3J5IChkaXJlY3RvcnlSZWFkZXIsIG9sZEVudHJpZXMsIGxvZ0Ryb3BFcnJvciwgeyBvblN1Y2Nlc3MgfSkge1xuICBkaXJlY3RvcnlSZWFkZXIucmVhZEVudHJpZXMoXG4gICAgKGVudHJpZXMpID0+IHtcbiAgICAgIGNvbnN0IG5ld0VudHJpZXMgPSBbLi4ub2xkRW50cmllcywgLi4uZW50cmllc11cbiAgICAgIC8vIEFjY29yZGluZyB0byB0aGUgRmlsZVN5c3RlbSBBUEkgc3BlYywgZ2V0RmlsZXNBbmREaXJlY3Rvcmllc0Zyb21EaXJlY3RvcnkoKSBtdXN0IGJlIGNhbGxlZCB1bnRpbCBpdCBjYWxscyB0aGUgb25TdWNjZXNzIHdpdGggYW4gZW1wdHkgYXJyYXkuXG4gICAgICBpZiAoZW50cmllcy5sZW5ndGgpIHtcbiAgICAgICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgICAgZ2V0RmlsZXNBbmREaXJlY3Rvcmllc0Zyb21EaXJlY3RvcnkoZGlyZWN0b3J5UmVhZGVyLCBuZXdFbnRyaWVzLCBsb2dEcm9wRXJyb3IsIHsgb25TdWNjZXNzIH0pXG4gICAgICAgIH0sIDApXG4gICAgICAvLyBEb25lIGl0ZXJhdGluZyB0aGlzIHBhcnRpY3VsYXIgZGlyZWN0b3J5XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBvblN1Y2Nlc3MobmV3RW50cmllcylcbiAgICAgIH1cbiAgICB9LFxuICAgIC8vIE1ha2Ugc3VyZSB3ZSByZXNvbHZlIG9uIGVycm9yIGFueXdheSwgaXQncyBmaW5lIGlmIG9ubHkgb25lIGRpcmVjdG9yeSBjb3VsZG4ndCBiZSBwYXJzZWQhXG4gICAgKGVycm9yKSA9PiB7XG4gICAgICBsb2dEcm9wRXJyb3IoZXJyb3IpXG4gICAgICBvblN1Y2Nlc3Mob2xkRW50cmllcylcbiAgICB9XG4gIClcbn1cbiIsIi8qKlxuICogR2V0IHRoZSByZWxhdGl2ZSBwYXRoIGZyb20gdGhlIEZpbGVFbnRyeSNmdWxsUGF0aCwgYmVjYXVzZSBGaWxlI3dlYmtpdFJlbGF0aXZlUGF0aCBpcyBhbHdheXMgJycsIGF0IGxlYXN0IG9uRHJvcC5cbiAqXG4gKiBAcGFyYW0ge0ZpbGVFbnRyeX0gZmlsZUVudHJ5XG4gKlxuICogQHJldHVybnMge3N0cmluZ3xudWxsfSAtIGlmIGZpbGUgaXMgbm90IGluIGEgZm9sZGVyIC0gcmV0dXJuIG51bGwgKHRoaXMgaXMgdG8gYmUgY29uc2lzdGVudCB3aXRoIC5yZWxhdGl2ZVBhdGgtcyBvZiBmaWxlcyBzZWxlY3RlZCBmcm9tIE15IERldmljZSkuIElmIGZpbGUgaXMgaW4gYSBmb2xkZXIgLSByZXR1cm4gaXRzIGZ1bGxQYXRoLCBlLmcuICcvc2ltcHNvbnMvaGkuanBlZycuXG4gKi9cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gZ2V0UmVsYXRpdmVQYXRoIChmaWxlRW50cnkpIHtcbiAgLy8gZmlsZUVudHJ5LmZ1bGxQYXRoIC0gXCIvc2ltcHNvbnMvaGkuanBlZ1wiIG9yIHVuZGVmaW5lZCAoZm9yIGJyb3dzZXJzIHRoYXQgZG9uJ3Qgc3VwcG9ydCBpdClcbiAgLy8gZmlsZUVudHJ5Lm5hbWUgLSBcImhpLmpwZWdcIlxuICBpZiAoIWZpbGVFbnRyeS5mdWxsUGF0aCB8fCBmaWxlRW50cnkuZnVsbFBhdGggPT09ICcvJyArIGZpbGVFbnRyeS5uYW1lKSB7XG4gICAgcmV0dXJuIG51bGxcbiAgfSBlbHNlIHtcbiAgICByZXR1cm4gZmlsZUVudHJ5LmZ1bGxQYXRoXG4gIH1cbn1cbiIsImNvbnN0IHRvQXJyYXkgPSByZXF1aXJlKCcuLi8uLi8uLi90b0FycmF5JylcbmNvbnN0IGdldFJlbGF0aXZlUGF0aCA9IHJlcXVpcmUoJy4vZ2V0UmVsYXRpdmVQYXRoJylcbmNvbnN0IGdldEZpbGVzQW5kRGlyZWN0b3JpZXNGcm9tRGlyZWN0b3J5ID0gcmVxdWlyZSgnLi9nZXRGaWxlc0FuZERpcmVjdG9yaWVzRnJvbURpcmVjdG9yeScpXG5cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gd2Via2l0R2V0QXNFbnRyeUFwaSAoZGF0YVRyYW5zZmVyLCBsb2dEcm9wRXJyb3IpIHtcbiAgY29uc3QgZmlsZXMgPSBbXVxuXG4gIGNvbnN0IHJvb3RQcm9taXNlcyA9IFtdXG5cbiAgLyoqXG4gICAqIFJldHVybnMgYSByZXNvbHZlZCBwcm9taXNlLCB3aGVuIDpmaWxlcyBhcnJheSBpcyBlbmhhbmNlZFxuICAgKlxuICAgKiBAcGFyYW0geyhGaWxlU3lzdGVtRmlsZUVudHJ5fEZpbGVTeXN0ZW1EaXJlY3RvcnlFbnRyeSl9IGVudHJ5XG4gICAqIEByZXR1cm5zIHtQcm9taXNlfSAtIGVtcHR5IHByb21pc2UgdGhhdCByZXNvbHZlcyB3aGVuIDpmaWxlcyBpcyBlbmhhbmNlZCB3aXRoIGEgZmlsZVxuICAgKi9cbiAgY29uc3QgY3JlYXRlUHJvbWlzZVRvQWRkRmlsZU9yUGFyc2VEaXJlY3RvcnkgPSAoZW50cnkpID0+XG4gICAgbmV3IFByb21pc2UoKHJlc29sdmUpID0+IHtcbiAgICAgIC8vIFRoaXMgaXMgYSBiYXNlIGNhbGxcbiAgICAgIGlmIChlbnRyeS5pc0ZpbGUpIHtcbiAgICAgICAgLy8gQ3JlYXRlcyBhIG5ldyBGaWxlIG9iamVjdCB3aGljaCBjYW4gYmUgdXNlZCB0byByZWFkIHRoZSBmaWxlLlxuICAgICAgICBlbnRyeS5maWxlKFxuICAgICAgICAgIChmaWxlKSA9PiB7XG4gICAgICAgICAgICBmaWxlLnJlbGF0aXZlUGF0aCA9IGdldFJlbGF0aXZlUGF0aChlbnRyeSlcbiAgICAgICAgICAgIGZpbGVzLnB1c2goZmlsZSlcbiAgICAgICAgICAgIHJlc29sdmUoKVxuICAgICAgICAgIH0sXG4gICAgICAgICAgLy8gTWFrZSBzdXJlIHdlIHJlc29sdmUgb24gZXJyb3IgYW55d2F5LCBpdCdzIGZpbmUgaWYgb25seSBvbmUgZmlsZSBjb3VsZG4ndCBiZSByZWFkIVxuICAgICAgICAgIChlcnJvcikgPT4ge1xuICAgICAgICAgICAgbG9nRHJvcEVycm9yKGVycm9yKVxuICAgICAgICAgICAgcmVzb2x2ZSgpXG4gICAgICAgICAgfVxuICAgICAgICApXG4gICAgICAvLyBUaGlzIGlzIGEgcmVjdXJzaXZlIGNhbGxcbiAgICAgIH0gZWxzZSBpZiAoZW50cnkuaXNEaXJlY3RvcnkpIHtcbiAgICAgICAgY29uc3QgZGlyZWN0b3J5UmVhZGVyID0gZW50cnkuY3JlYXRlUmVhZGVyKClcbiAgICAgICAgZ2V0RmlsZXNBbmREaXJlY3Rvcmllc0Zyb21EaXJlY3RvcnkoZGlyZWN0b3J5UmVhZGVyLCBbXSwgbG9nRHJvcEVycm9yLCB7XG4gICAgICAgICAgb25TdWNjZXNzOiAoZW50cmllcykgPT4ge1xuICAgICAgICAgICAgY29uc3QgcHJvbWlzZXMgPSBlbnRyaWVzLm1hcCgoZW50cnkpID0+IGNyZWF0ZVByb21pc2VUb0FkZEZpbGVPclBhcnNlRGlyZWN0b3J5KGVudHJ5KSlcbiAgICAgICAgICAgIFByb21pc2UuYWxsKHByb21pc2VzKS50aGVuKCgpID0+IHJlc29sdmUoKSlcbiAgICAgICAgICB9XG4gICAgICAgIH0pXG4gICAgICB9XG4gICAgfSlcblxuICAvLyBGb3IgZWFjaCBkcm9wcGVkIGl0ZW0sIC0gbWFrZSBzdXJlIGl0J3MgYSBmaWxlL2RpcmVjdG9yeSwgYW5kIHN0YXJ0IGRlZXBlbmluZyBpbiFcbiAgdG9BcnJheShkYXRhVHJhbnNmZXIuaXRlbXMpXG4gICAgLmZvckVhY2goKGl0ZW0pID0+IHtcbiAgICAgIGNvbnN0IGVudHJ5ID0gaXRlbS53ZWJraXRHZXRBc0VudHJ5KClcbiAgICAgIC8vIDplbnRyeSBjYW4gYmUgbnVsbCB3aGVuIHdlIGRyb3AgdGhlIHVybCBlLmcuXG4gICAgICBpZiAoZW50cnkpIHtcbiAgICAgICAgcm9vdFByb21pc2VzLnB1c2goY3JlYXRlUHJvbWlzZVRvQWRkRmlsZU9yUGFyc2VEaXJlY3RvcnkoZW50cnkpKVxuICAgICAgfVxuICAgIH0pXG5cbiAgcmV0dXJuIFByb21pc2UuYWxsKHJvb3RQcm9taXNlcylcbiAgICAudGhlbigoKSA9PiBmaWxlcylcbn1cbiIsIi8qKlxuICogVGFrZXMgYSBmdWxsIGZpbGVuYW1lIHN0cmluZyBhbmQgcmV0dXJucyBhbiBvYmplY3Qge25hbWUsIGV4dGVuc2lvbn1cbiAqXG4gKiBAcGFyYW0ge3N0cmluZ30gZnVsbEZpbGVOYW1lXG4gKiBAcmV0dXJucyB7b2JqZWN0fSB7bmFtZSwgZXh0ZW5zaW9ufVxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGdldEZpbGVOYW1lQW5kRXh0ZW5zaW9uIChmdWxsRmlsZU5hbWUpIHtcbiAgdmFyIHJlID0gLyg/OlxcLihbXi5dKykpPyQvXG4gIHZhciBmaWxlRXh0ID0gcmUuZXhlYyhmdWxsRmlsZU5hbWUpWzFdXG4gIHZhciBmaWxlTmFtZSA9IGZ1bGxGaWxlTmFtZS5yZXBsYWNlKCcuJyArIGZpbGVFeHQsICcnKVxuICByZXR1cm4ge1xuICAgIG5hbWU6IGZpbGVOYW1lLFxuICAgIGV4dGVuc2lvbjogZmlsZUV4dFxuICB9XG59XG4iLCJjb25zdCBnZXRGaWxlTmFtZUFuZEV4dGVuc2lvbiA9IHJlcXVpcmUoJy4vZ2V0RmlsZU5hbWVBbmRFeHRlbnNpb24nKVxuY29uc3QgbWltZVR5cGVzID0gcmVxdWlyZSgnLi9taW1lVHlwZXMnKVxuXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGdldEZpbGVUeXBlIChmaWxlKSB7XG4gIGxldCBmaWxlRXh0ZW5zaW9uID0gZmlsZS5uYW1lID8gZ2V0RmlsZU5hbWVBbmRFeHRlbnNpb24oZmlsZS5uYW1lKS5leHRlbnNpb24gOiBudWxsXG4gIGZpbGVFeHRlbnNpb24gPSBmaWxlRXh0ZW5zaW9uID8gZmlsZUV4dGVuc2lvbi50b0xvd2VyQ2FzZSgpIDogbnVsbFxuXG4gIGlmIChmaWxlLnR5cGUpIHtcbiAgICAvLyBpZiBtaW1lIHR5cGUgaXMgc2V0IGluIHRoZSBmaWxlIG9iamVjdCBhbHJlYWR5LCB1c2UgdGhhdFxuICAgIHJldHVybiBmaWxlLnR5cGVcbiAgfSBlbHNlIGlmIChmaWxlRXh0ZW5zaW9uICYmIG1pbWVUeXBlc1tmaWxlRXh0ZW5zaW9uXSkge1xuICAgIC8vIGVsc2UsIHNlZSBpZiB3ZSBjYW4gbWFwIGV4dGVuc2lvbiB0byBhIG1pbWUgdHlwZVxuICAgIHJldHVybiBtaW1lVHlwZXNbZmlsZUV4dGVuc2lvbl1cbiAgfSBlbHNlIHtcbiAgICAvLyBpZiBhbGwgZmFpbHMsIGZhbGwgYmFjayB0byBhIGdlbmVyaWMgYnl0ZSBzdHJlYW0gdHlwZVxuICAgIHJldHVybiAnYXBwbGljYXRpb24vb2N0ZXQtc3RyZWFtJ1xuICB9XG59XG4iLCJtb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGdldFNvY2tldEhvc3QgKHVybCkge1xuICAvLyBnZXQgdGhlIGhvc3QgZG9tYWluXG4gIHZhciByZWdleCA9IC9eKD86aHR0cHM/OlxcL1xcL3xcXC9cXC8pPyg/OlteQFxcbl0rQCk/KD86d3d3XFwuKT8oW15cXG5dKykvaVxuICB2YXIgaG9zdCA9IHJlZ2V4LmV4ZWModXJsKVsxXVxuICB2YXIgc29ja2V0UHJvdG9jb2wgPSAvXmh0dHA6XFwvXFwvL2kudGVzdCh1cmwpID8gJ3dzJyA6ICd3c3MnXG5cbiAgcmV0dXJuIGAke3NvY2tldFByb3RvY29sfTovLyR7aG9zdH1gXG59XG4iLCIvKipcbiAqIFJldHVybnMgYSB0aW1lc3RhbXAgaW4gdGhlIGZvcm1hdCBvZiBgaG91cnM6bWludXRlczpzZWNvbmRzYFxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGdldFRpbWVTdGFtcCAoKSB7XG4gIHZhciBkYXRlID0gbmV3IERhdGUoKVxuICB2YXIgaG91cnMgPSBwYWQoZGF0ZS5nZXRIb3VycygpLnRvU3RyaW5nKCkpXG4gIHZhciBtaW51dGVzID0gcGFkKGRhdGUuZ2V0TWludXRlcygpLnRvU3RyaW5nKCkpXG4gIHZhciBzZWNvbmRzID0gcGFkKGRhdGUuZ2V0U2Vjb25kcygpLnRvU3RyaW5nKCkpXG4gIHJldHVybiBob3VycyArICc6JyArIG1pbnV0ZXMgKyAnOicgKyBzZWNvbmRzXG59XG5cbi8qKlxuICogQWRkcyB6ZXJvIHRvIHN0cmluZ3Mgc2hvcnRlciB0aGFuIHR3byBjaGFyYWN0ZXJzXG4gKi9cbmZ1bmN0aW9uIHBhZCAoc3RyKSB7XG4gIHJldHVybiBzdHIubGVuZ3RoICE9PSAyID8gMCArIHN0ciA6IHN0clxufVxuIiwibW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBoYXMgKG9iamVjdCwga2V5KSB7XG4gIHJldHVybiBPYmplY3QucHJvdG90eXBlLmhhc093blByb3BlcnR5LmNhbGwob2JqZWN0LCBrZXkpXG59XG4iLCIvKipcbiAqIENoZWNrIGlmIGFuIG9iamVjdCBpcyBhIERPTSBlbGVtZW50LiBEdWNrLXR5cGluZyBiYXNlZCBvbiBgbm9kZVR5cGVgLlxuICpcbiAqIEBwYXJhbSB7Kn0gb2JqXG4gKi9cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gaXNET01FbGVtZW50IChvYmopIHtcbiAgcmV0dXJuIG9iaiAmJiB0eXBlb2Ygb2JqID09PSAnb2JqZWN0JyAmJiBvYmoubm9kZVR5cGUgPT09IE5vZGUuRUxFTUVOVF9OT0RFXG59XG4iLCIvKipcbiAqIENoZWNrcyBpZiB0aGUgYnJvd3NlciBzdXBwb3J0cyBEcmFnICYgRHJvcCAobm90IHN1cHBvcnRlZCBvbiBtb2JpbGUgZGV2aWNlcywgZm9yIGV4YW1wbGUpLlxuICpcbiAqIEByZXR1cm5zIHtib29sZWFufVxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGlzRHJhZ0Ryb3BTdXBwb3J0ZWQgKCkge1xuICBjb25zdCBkaXYgPSBkb2N1bWVudC5jcmVhdGVFbGVtZW50KCdkaXYnKVxuXG4gIGlmICghKCdkcmFnZ2FibGUnIGluIGRpdikgfHwgISgnb25kcmFnc3RhcnQnIGluIGRpdiAmJiAnb25kcm9wJyBpbiBkaXYpKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cblxuICBpZiAoISgnRm9ybURhdGEnIGluIHdpbmRvdykpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuXG4gIGlmICghKCdGaWxlUmVhZGVyJyBpbiB3aW5kb3cpKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cblxuICByZXR1cm4gdHJ1ZVxufVxuIiwiLy8gX19fV2h5IG5vdCBhZGQgdGhlIG1pbWUtdHlwZXMgcGFja2FnZT9cbi8vICAgIEl0J3MgMTkuN2tCIGd6aXBwZWQsIGFuZCB3ZSBvbmx5IG5lZWQgbWltZSB0eXBlcyBmb3Igd2VsbC1rbm93biBleHRlbnNpb25zIChmb3IgZmlsZSBwcmV2aWV3cykuXG4vLyBfX19XaGVyZSB0byB0YWtlIG5ldyBleHRlbnNpb25zIGZyb20/XG4vLyAgICBodHRwczovL2dpdGh1Yi5jb20vanNodHRwL21pbWUtZGIvYmxvYi9tYXN0ZXIvZGIuanNvblxuXG5tb2R1bGUuZXhwb3J0cyA9IHtcbiAgbWQ6ICd0ZXh0L21hcmtkb3duJyxcbiAgbWFya2Rvd246ICd0ZXh0L21hcmtkb3duJyxcbiAgbXA0OiAndmlkZW8vbXA0JyxcbiAgbXAzOiAnYXVkaW8vbXAzJyxcbiAgc3ZnOiAnaW1hZ2Uvc3ZnK3htbCcsXG4gIGpwZzogJ2ltYWdlL2pwZWcnLFxuICBwbmc6ICdpbWFnZS9wbmcnLFxuICBnaWY6ICdpbWFnZS9naWYnLFxuICBoZWljOiAnaW1hZ2UvaGVpYycsXG4gIGhlaWY6ICdpbWFnZS9oZWlmJyxcbiAgeWFtbDogJ3RleHQveWFtbCcsXG4gIHltbDogJ3RleHQveWFtbCcsXG4gIGNzdjogJ3RleHQvY3N2JyxcbiAgYXZpOiAndmlkZW8veC1tc3ZpZGVvJyxcbiAgbWtzOiAndmlkZW8veC1tYXRyb3NrYScsXG4gIG1rdjogJ3ZpZGVvL3gtbWF0cm9za2EnLFxuICBtb3Y6ICd2aWRlby9xdWlja3RpbWUnLFxuICBkb2M6ICdhcHBsaWNhdGlvbi9tc3dvcmQnLFxuICBkb2NtOiAnYXBwbGljYXRpb24vdm5kLm1zLXdvcmQuZG9jdW1lbnQubWFjcm9lbmFibGVkLjEyJyxcbiAgZG9jeDogJ2FwcGxpY2F0aW9uL3ZuZC5vcGVueG1sZm9ybWF0cy1vZmZpY2Vkb2N1bWVudC53b3JkcHJvY2Vzc2luZ21sLmRvY3VtZW50JyxcbiAgZG90OiAnYXBwbGljYXRpb24vbXN3b3JkJyxcbiAgZG90bTogJ2FwcGxpY2F0aW9uL3ZuZC5tcy13b3JkLnRlbXBsYXRlLm1hY3JvZW5hYmxlZC4xMicsXG4gIGRvdHg6ICdhcHBsaWNhdGlvbi92bmQub3BlbnhtbGZvcm1hdHMtb2ZmaWNlZG9jdW1lbnQud29yZHByb2Nlc3NpbmdtbC50ZW1wbGF0ZScsXG4gIHhsYTogJ2FwcGxpY2F0aW9uL3ZuZC5tcy1leGNlbCcsXG4gIHhsYW06ICdhcHBsaWNhdGlvbi92bmQubXMtZXhjZWwuYWRkaW4ubWFjcm9lbmFibGVkLjEyJyxcbiAgeGxjOiAnYXBwbGljYXRpb24vdm5kLm1zLWV4Y2VsJyxcbiAgeGxmOiAnYXBwbGljYXRpb24veC14bGlmZit4bWwnLFxuICB4bG06ICdhcHBsaWNhdGlvbi92bmQubXMtZXhjZWwnLFxuICB4bHM6ICdhcHBsaWNhdGlvbi92bmQubXMtZXhjZWwnLFxuICB4bHNiOiAnYXBwbGljYXRpb24vdm5kLm1zLWV4Y2VsLnNoZWV0LmJpbmFyeS5tYWNyb2VuYWJsZWQuMTInLFxuICB4bHNtOiAnYXBwbGljYXRpb24vdm5kLm1zLWV4Y2VsLnNoZWV0Lm1hY3JvZW5hYmxlZC4xMicsXG4gIHhsc3g6ICdhcHBsaWNhdGlvbi92bmQub3BlbnhtbGZvcm1hdHMtb2ZmaWNlZG9jdW1lbnQuc3ByZWFkc2hlZXRtbC5zaGVldCcsXG4gIHhsdDogJ2FwcGxpY2F0aW9uL3ZuZC5tcy1leGNlbCcsXG4gIHhsdG06ICdhcHBsaWNhdGlvbi92bmQubXMtZXhjZWwudGVtcGxhdGUubWFjcm9lbmFibGVkLjEyJyxcbiAgeGx0eDogJ2FwcGxpY2F0aW9uL3ZuZC5vcGVueG1sZm9ybWF0cy1vZmZpY2Vkb2N1bWVudC5zcHJlYWRzaGVldG1sLnRlbXBsYXRlJyxcbiAgeGx3OiAnYXBwbGljYXRpb24vdm5kLm1zLWV4Y2VsJyxcbiAgdHh0OiAndGV4dC9wbGFpbicsXG4gIHRleHQ6ICd0ZXh0L3BsYWluJyxcbiAgY29uZjogJ3RleHQvcGxhaW4nLFxuICBsb2c6ICd0ZXh0L3BsYWluJyxcbiAgcGRmOiAnYXBwbGljYXRpb24vcGRmJ1xufVxuIiwiLy8gQWRhcHRlZCBmcm9tIGh0dHBzOi8vZ2l0aHViLmNvbS9GbGV0L3ByZXR0aWVyLWJ5dGVzL1xuLy8gQ2hhbmdpbmcgMTAwMCBieXRlcyB0byAxMDI0LCBzbyB3ZSBjYW4ga2VlcCB1cHBlcmNhc2UgS0IgdnMga0Jcbi8vIElTQyBMaWNlbnNlIChjKSBEYW4gRmxldHRyZSBodHRwczovL2dpdGh1Yi5jb20vRmxldC9wcmV0dGllci1ieXRlcy9ibG9iL21hc3Rlci9MSUNFTlNFXG5cbm1vZHVsZS5leHBvcnRzID0gcHJldHRpZXJCeXRlc1xuXG5mdW5jdGlvbiBwcmV0dGllckJ5dGVzIChudW0pIHtcbiAgaWYgKHR5cGVvZiBudW0gIT09ICdudW1iZXInIHx8IGlzTmFOKG51bSkpIHtcbiAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdFeHBlY3RlZCBhIG51bWJlciwgZ290ICcgKyB0eXBlb2YgbnVtKVxuICB9XG5cbiAgdmFyIG5lZyA9IG51bSA8IDBcbiAgdmFyIHVuaXRzID0gWydCJywgJ0tCJywgJ01CJywgJ0dCJywgJ1RCJywgJ1BCJywgJ0VCJywgJ1pCJywgJ1lCJ11cblxuICBpZiAobmVnKSB7XG4gICAgbnVtID0gLW51bVxuICB9XG5cbiAgaWYgKG51bSA8IDEpIHtcbiAgICByZXR1cm4gKG5lZyA/ICctJyA6ICcnKSArIG51bSArICcgQidcbiAgfVxuXG4gIHZhciBleHBvbmVudCA9IE1hdGgubWluKE1hdGguZmxvb3IoTWF0aC5sb2cobnVtKSAvIE1hdGgubG9nKDEwMjQpKSwgdW5pdHMubGVuZ3RoIC0gMSlcbiAgbnVtID0gTnVtYmVyKG51bSAvIE1hdGgucG93KDEwMjQsIGV4cG9uZW50KSlcbiAgdmFyIHVuaXQgPSB1bml0c1tleHBvbmVudF1cblxuICBpZiAobnVtID49IDEwIHx8IG51bSAlIDEgPT09IDApIHtcbiAgICAvLyBEbyBub3Qgc2hvdyBkZWNpbWFscyB3aGVuIHRoZSBudW1iZXIgaXMgdHdvLWRpZ2l0LCBvciBpZiB0aGUgbnVtYmVyIGhhcyBub1xuICAgIC8vIGRlY2ltYWwgY29tcG9uZW50LlxuICAgIHJldHVybiAobmVnID8gJy0nIDogJycpICsgbnVtLnRvRml4ZWQoMCkgKyAnICcgKyB1bml0XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIChuZWcgPyAnLScgOiAnJykgKyBudW0udG9GaXhlZCgxKSArICcgJyArIHVuaXRcbiAgfVxufVxuIiwibW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBzZXR0bGUgKHByb21pc2VzKSB7XG4gIGNvbnN0IHJlc29sdXRpb25zID0gW11cbiAgY29uc3QgcmVqZWN0aW9ucyA9IFtdXG4gIGZ1bmN0aW9uIHJlc29sdmVkICh2YWx1ZSkge1xuICAgIHJlc29sdXRpb25zLnB1c2godmFsdWUpXG4gIH1cbiAgZnVuY3Rpb24gcmVqZWN0ZWQgKGVycm9yKSB7XG4gICAgcmVqZWN0aW9ucy5wdXNoKGVycm9yKVxuICB9XG5cbiAgY29uc3Qgd2FpdCA9IFByb21pc2UuYWxsKFxuICAgIHByb21pc2VzLm1hcCgocHJvbWlzZSkgPT4gcHJvbWlzZS50aGVuKHJlc29sdmVkLCByZWplY3RlZCkpXG4gIClcblxuICByZXR1cm4gd2FpdC50aGVuKCgpID0+IHtcbiAgICByZXR1cm4ge1xuICAgICAgc3VjY2Vzc2Z1bDogcmVzb2x1dGlvbnMsXG4gICAgICBmYWlsZWQ6IHJlamVjdGlvbnNcbiAgICB9XG4gIH0pXG59XG4iLCIvKipcbiAqIENvbnZlcnRzIGxpc3QgaW50byBhcnJheVxuICovXG5tb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIHRvQXJyYXkgKGxpc3QpIHtcbiAgcmV0dXJuIEFycmF5LnByb3RvdHlwZS5zbGljZS5jYWxsKGxpc3QgfHwgW10sIDApXG59XG4iLCJyZXF1aXJlKCdlczYtcHJvbWlzZS9hdXRvJylcbnJlcXVpcmUoJ3doYXR3Zy1mZXRjaCcpXG5jb25zdCBVcHB5ID0gcmVxdWlyZSgnQHVwcHkvY29yZScpXG5jb25zdCBEcmFnRHJvcCA9IHJlcXVpcmUoJ0B1cHB5L2RyYWctZHJvcCcpXG5jb25zdCBQcm9ncmVzc0JhciA9IHJlcXVpcmUoJ0B1cHB5L3Byb2dyZXNzLWJhcicpXG5jb25zdCBUdXMgPSByZXF1aXJlKCdAdXBweS90dXMnKVxuXG4vLyBGdW5jdGlvbiBmb3IgZGlzcGxheWluZyB1cGxvYWRlZCBmaWxlc1xuY29uc3Qgb25VcGxvYWRTdWNjZXNzID0gKGVsRm9yVXBsb2FkZWRGaWxlcykgPT5cbiAgKGZpbGUsIHJlc3BvbnNlKSA9PiB7XG4gICAgY29uc3QgdXJsID0gcmVzcG9uc2UudXBsb2FkVVJMXG4gICAgY29uc3QgZmlsZU5hbWUgPSBmaWxlLm5hbWVcblxuICAgIGRvY3VtZW50LnF1ZXJ5U2VsZWN0b3IoZWxGb3JVcGxvYWRlZEZpbGVzKS5pbm5lckhUTUwgKz1cbiAgICAgIGA8bGk+PGEgaHJlZj1cIiR7dXJsfVwiIHRhcmdldD1cIl9ibGFua1wiPiR7ZmlsZU5hbWV9PC9hPjwvbGk+YFxuICB9XG5cbmNvbnN0IHVwcHlPbmUgPSBuZXcgVXBweSh7IGRlYnVnOiB0cnVlLCBhdXRvUHJvY2VlZDogdHJ1ZSB9KVxudXBweU9uZVxuICAudXNlKERyYWdEcm9wLCB7IHRhcmdldDogJy5leGFtcGxlLW9uZSAuZm9yLURyYWdEcm9wJyB9KVxuICAudXNlKFR1cywgeyBlbmRwb2ludDogJ2h0dHBzOi8vbWFzdGVyLnR1cy5pby9maWxlcy8nIH0pXG4gIC51c2UoUHJvZ3Jlc3NCYXIsIHsgdGFyZ2V0OiAnLmV4YW1wbGUtb25lIC5mb3ItUHJvZ3Jlc3NCYXInLCBoaWRlQWZ0ZXJGaW5pc2g6IGZhbHNlIH0pXG4gIC5vbigndXBsb2FkLXN1Y2Nlc3MnLCBvblVwbG9hZFN1Y2Nlc3MoJy5leGFtcGxlLW9uZSAudXBsb2FkZWQtZmlsZXMgb2wnKSlcblxuY29uc3QgdXBweVR3byA9IG5ldyBVcHB5KHsgZGVidWc6IHRydWUsIGF1dG9Qcm9jZWVkOiBmYWxzZSB9KVxudXBweVR3b1xuICAudXNlKERyYWdEcm9wLCB7IHRhcmdldDogJy5leGFtcGxlLXR3byAuZm9yLURyYWdEcm9wJyB9KVxuICAudXNlKFR1cywgeyBlbmRwb2ludDogJ2h0dHBzOi8vbWFzdGVyLnR1cy5pby9maWxlcy8nIH0pXG4gIC51c2UoUHJvZ3Jlc3NCYXIsIHsgdGFyZ2V0OiAnLmV4YW1wbGUtdHdvIC5mb3ItUHJvZ3Jlc3NCYXInLCBoaWRlQWZ0ZXJGaW5pc2g6IGZhbHNlIH0pXG4gIC5vbigndXBsb2FkLXN1Y2Nlc3MnLCBvblVwbG9hZFN1Y2Nlc3MoJy5leGFtcGxlLXR3byAudXBsb2FkZWQtZmlsZXMgb2wnKSlcblxuY29uc3QgdXBsb2FkQnRuID0gZG9jdW1lbnQucXVlcnlTZWxlY3RvcignLmV4YW1wbGUtdHdvIGJ1dHRvbi51cGxvYWQtYnV0dG9uJylcbnVwbG9hZEJ0bi5hZGRFdmVudExpc3RlbmVyKCdjbGljaycsIGZ1bmN0aW9uICgpIHtcbiAgdXBweVR3by51cGxvYWQoKVxufSlcbiJdfQ==
