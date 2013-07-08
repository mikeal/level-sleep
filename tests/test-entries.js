var lsleep = require('../')
  , noop = function () {}
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , ok = require('okdone')
  , assert = require('assert')
  ;

var d = cleanup(function (error) {
  rimraf.sync(__dirname+'/testdb')
  if (error) process.exit(1)
  ok.done()
})

var s = lsleep(__dirname+'/testdb')
s.get('test', function (e, db) {
  db.put('id1', 1, noop)
  db.put('id2', 2, noop)
  db.put('id1', 3, function () {
    db.get('id1', function (e, data) {
      assert.equal(data, 3)
      ok('overwrite')
      var dict = {}
        , entries = db.getSequences({})
        ;
      entries.on('entry', function (entry) { dict[entry.id] = entry})
      entries.on('end', function () {
        assert.equal(Object.keys(dict).length, 2)
        assert.ok(!dict['id1'].data)
        ok('entries no data')

        dict = {}
          , entries = db.getSequences({include_data:true})
          ;
        entries.on('entry', function (entry) { dict[entry.id] = entry})
        entries.on('end', function () {
          assert.equal(Object.keys(dict).length, 2)
          assert.equal(dict['id1'].data, 3)
          ok('entries w/ data')

          d.cleanup()
        })
      })
    })
  })
})