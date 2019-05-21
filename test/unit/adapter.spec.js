var proxyquire = require('proxyquire').noCallThru();;
var expect = require("chai").expect;

var mssqlStub = {};
var sqlStub = {
  updateCriteria: function () {
    return '';
  },
  serializeOptions: function () {
    return 'where clause';
  },
  selectQuery: function () {
    return '';
  }
};
var utilsStub = {
  convertDates: function () {
    return;
  },
  prepareValue: function (data) {
    return data;
  },
}

var adapter = proxyquire('../../', { mssql: mssqlStub, './sql.js': sqlStub, './utils': utilsStub });

mssqlStub.ConnectionPool = function () {
  return {
    connect: function () {
      return {
        then: function (cb) {
          cb();
        }
      }
    },
    close: function () {

    },
  };
}
mssqlStub.Request = function () {
  return {
    query: function (statement, cb) {
      return cb(null, {
        recordset: [
          { id: 1, first: 1, second: 2 },
          { id: 2, first: 2, second: 2 }
        ]
      });
    }
  }
}

describe('adapter', function () {
  var collectionDefinition = {};
  before(function () {
    collectionDefinition.meta = { junctionTable: true };
    collectionDefinition.definition = {};
    adapter.registerConnection({ identity: 'db', pool: {} }, { coll: collectionDefinition }, function () { });
  });

  describe('update', function () {
    it('should not return an error if there is a proper where clause', function (done) {
      var connection = 'db', collection = 'coll', options = { where: {} }
      sqlStub.serializeOptions = function () {
        return 'WHERE clause';
      };
      adapter.update(connection, collection, options, { sue: 1234, bob: null }, function (err, values) {
        done(err);
      });
    });
    it('should return error a where clause is not generated', function (done) {
      var connection = 'db', collection = 'coll', options = { where: {} }
      sqlStub.serializeOptions = function () {
        return '';
      };
      adapter.update(connection, collection, options, { sue: 1234, bob: null }, function (err, values) {
        if (err) {
          expect(err.message.indexOf('where clause is required')).to.equal(0);
          done();
        } else {
          done(new Error('did not throw error :('));
        }
      });
    });

  })
})
