var mutex = require('level-mutex')
  , levelup = require('levelup')
  , bytewise = require('bytewise')
  , events = require('events')
  , util = require('util')
  , sleep = require('sleep-ref')

  , noop = function () {}
  , decode = bytewise.decode
  , encode = bytewise.encode
  ;

function Store (filename, opts) {
  opts = opts || {}
  opts.keyEncoding = 'binary'
  opts.valueEncoding = 'json'
  this.lev = levelup(filename, opts)
  this.databases = {}
  this.mutex = mutex(this.lev)
}
Store.prototype.get = function (name, cb) {
  if (this.databases[name]) return cb(null, this.databases[name])
  var self = this
  var opts =
    { start: bytewise.encode([name, 0, null])
    , end: bytewise.encode([name, 0, {}])
    }

  this.mutex.peekLast(opts, function (e, key) {
    if (!self.databases[name]) {
      if (e) {
        self.databases[name] = new Database(self, name, 0)
      } else {
        var key = bytewise.decode(key)
          , seq = key[2]
          ;
        if (typeof seq !== 'number') throw new Error("Invalid sequence: "+seq)
        self.databases[name] = new Database(self, name, seq)
      }
    }
    cb(null, self.databases[name])
  })
}

function Database (store, name, seq) {
  this.store = store
  this.name = name
  this.mutex = mutex(store.lev)
  this.seq = seq
}
util.inherits(Database, events.EventEmitter)
Database.prototype.put = function (key, value, cb) {
  var self = this
  this.seq += 1
  var seq = this.seq
  if (typeof seq !== 'number') throw new Error('Invalid sequence.')
  this.mutex.put(bytewise.encode([this.name, 0, seq, false]), key, noop)
  this.mutex.put(bytewise.encode([this.name, 1, key, seq, false]), value, function (e) {
    if (e) return cb(e)
    cb(null, seq)
    self.emit('entry', {seq:seq, id:key, data:value})
  })
}
Database.prototype.add = function (key, value, cb) {
  var self = this
  self.get(key, function (e) {
    if (e) return self.put(key, value, cb)
    cb(null)
  })
}

Database.prototype.compact = function (cb) {
  var self = this
  var rangeOpts =
    { start: bytewise.encode([this.name, 1, {}])
    , end: bytewise.encode([this.name, 1, null])
    , reverse: true
    }

  var sequences = self.mutex.lev.createReadStream(rangeOpts)
    , id = null
    , seqs = []
    , deletes = []
    ;
  sequences.on('data', function (row) {
    var key = bytewise.decode(row.key)
      , _id = key[2]
      , seq = key[3]
      , deleted = key[4]
      ;
    if (id !== _id) {
      id = _id
    } else {
      deletes.push(bytewise.encode([self.name, 0, seq, deleted]))
      deletes.push(row.key)
    }
  })
  sequences.on('end', function () {
    deletes.forEach(function (entry) {
      self.mutex.del(entry, noop)
    })
    if (deletes.length === 0) return cb(null)
    else self.mutex.afterWrite(cb)
  })
}
Database.prototype.each = function (fn) {
  var self = this
  var rangeOpts =
    { start: bytewise.encode([this.name, 1, null])
    , end: bytewise.encode([this.name, 1, {}])
    }

  var sequences = self.mutex.lev.createReadStream(rangeOpts)
    , id = null
    , first = null
    ;
  sequences.on('data', function (row) {
    var key = bytewise.decode(row.key)
      , _id = key[2]
      , seq = key[3]
      , deleted = key[4]
      , entry = {}
    entry.seq = seq
    entry.deleted = deleted
    entry.id = _id
    entry.data = row.value

    if (first === null) {
      first = [_id, entry.data, entry]
    }
    if (id !== _id) {
      id = _id
      fn(first[0], first[1], first[2])
      first = null
    }
  })
  sequences.on('end', function () {
    if (first) fn(first[0], first[1], first[2])
  })
  return sequences
}
Database.prototype.keys = function (fn) {
  var self = this
  var rangeOpts =
    { start: bytewise.encode([this.name, 1, null])
    , end: bytewise.encode([this.name, 1, {}])
    , values: false
    }

  var sequences = self.mutex.lev.createReadStream(rangeOpts)
    , id = null
    , first = null
    ;
  sequences.on('data', function (row) {
    var key = bytewise.decode(row)
      , _id = key[2]
      , seq = key[3]
      , deleted = key[4]
      , entry = {}
    entry.seq = seq
    entry.deleted = deleted
    entry.id = _id

    if (first === null) {
      first = [_id, entry]
    }
    if (id !== _id) {
      id = _id
      fn(first[0], first[1])
      first = null
    }
  })
  sequences.on('end', function () {
    if (first) fn(first[0], first[1])
  })
  return sequences
}

Database.prototype.del = function (key, value, cb) {
  this.seq += 1
  var seq = this.seq
  this.mutex.put(bytewise.encode([this.name, 0, seq, true]), key, noop)
  this.mutex.put(bytewise.encode([this.name, 1, key, seq, true], value), function (e) {
    if (e) return cb(e)
    cb(null, seq)
    self.emit('entry', {seq:seq, id:key, data:value, deleted:true})
  })
}
Database.prototype.get = function (key, cb) {
  var opts =
    { start: bytewise.encode([this.name, 1, key, null])
    , end:bytewise.encode([this.name, 1, key, {}])
    }
  this.mutex.peekLast(opts, function (e, key, value) {
    if (e) return cb(new Error('not found.'))
    cb(null, value)
  })
}
Database.prototype.nextSequence = function (opts, cb) {
  opts.since = opts.since || -1
  var pending = []
    , self = this
    , onEntry = pending.push.bind(pending)
    , rangeOpts =
      { start: bytewise.encode([this.name, 0, (opts.since || -1) + 1])
      , end: bytewise.encode([this.name, 0, {}])
      }
    , ee = new events.EventEmitter()
    ;

  self.mutex.peekFirst(rangeOpts, function (e, rawkey, value) {
    if (e) return cb(2)
    var key = decode(rawkey)
    var entry =
      { seq: key[2]
      , deleted: key[3]
      , id: value
      }
    if (entry.deleted) {
      cb(null, entry)
    } else {
      self.mutex.get(encode([self.name, 1, entry.id, entry.seq, entry.deleted]), function (e, value) {
        if (e) return cb(e)
        entry.data = value
        cb(null, entry)
      })
    }
  })
}

Database.prototype.getSequences = function (opts, cb) {
  opts.since = opts.since || 0
  opts.limit = opts.limit || -1
  var pending = []
    , self = this
    , onEntry = pending.push.bind(pending)
    , rangeOpts =
      { start: bytewise.encode([this.name, 0, opts.since || 0])
      , end: bytewise.encode([this.name, 0, {}])
      , limit: opts.limit
      }
    , ee = new events.EventEmitter()
    ;

  this.on('entry', onEntry)
  var sequences = this.mutex.lev.createReadStream(rangeOpts)

  sequences.on('data', function (change) {
    change.key = decode(change.key)
    var entry =
      { id: change.value
      , seq: change.key[2]
      , deleted: change.key[3]
      }
    if (opts.include_data) {
      // even if it was deleted we do a get to insure correct ordering by relying on the mutex
      self.mutex.get(encode([self.name, 1, entry.id, entry.seq, entry.deleted]), function (e, value) {
        if (!entry.deleted) entry.data = value
        ee.emit('entry', entry)
      })
    } else {
      ee.emit('entry', entry)
    }
  })
  sequences.on('end', function () {
    // hack: get something from the mutex to insure we're after any data gets
    self.mutex.get(encode([self.name]), function () {
      pending.forEach(function (entry) {
        if (!opts.include_data) {
          entry = _.clone(entry)
          delete entry.data
        }
        if (opts.since < entry.seq) ee.emit('entry')
      })
      self.removeListener('entry', onEntry)

      if (opts.continuous) {
        // TODO: continuous once it is defined.
      } else {
        ee.emit('end')
      }
    })
  })
  return ee
}
Database.prototype.pull = function (url, opts, cb) {
  var self = this
  if (!cb && typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  if (typeof opts.continuous === 'undefined') opts.continuous = false
  if (typeof opts.since === 'undefined') opts.since = null
  if (typeof opts.save === 'undefined') opts.save = true

  function _run () {
    var s = sleep.client(url, opts)
    s.on('entry', function (entry) {
      self.put(entry.id, entry.data, function (e) {
        if (e) return cb(e) // probably need something smarter here
      })
    })
    s.on('end', function () {
      cb(null)
    })
  }

  if (opts.save && opts.since === null) {
    self.mutex.get(encode([self.name, 2, url]), function (e, since) {
      if (e) since = 0
      opts.since = since
      _run()
    })
  } else {
    _run()
  }
}

module.exports = function (filename) {return new Store(filename)}