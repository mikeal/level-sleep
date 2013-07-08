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
  this.mutex.peekLast(opts, function (e, key, seq) {
    if (!self.databases[name]) {
      if (e) {
        self.databases[name] = new Database(self, name, 0)
      } else {
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
  this.mutex.put(bytewise.encode([this.name, 0, seq, false]), key, noop)
  this.mutex.put(bytewise.encode([this.name, 1, key, seq, false]), value, function (e) {
    if (e) return cb(e)
    cb(null, seq)
    self.emit('entry', {seq:seq, id:key, data:value})
  })
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
Database.prototype.getSequences = function (opts, cb) {
  opts.since = opts.since || 0
  var ee = new events.EventEmitter()
    , pending = []
    , self = this
    , onEntry = pending.push.bind(pending)
    , rangeOpts =
      { start: bytewise.encode([this.name, 0, opts.since || 0])
      , end: bytewise.encode([this.name, 0, {}])
      }
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
      // TODO: continuous once it is defined.
      ee.emit('end')
    })
  })
  return ee
}

module.exports = function (filename) {return new Store(filename)}