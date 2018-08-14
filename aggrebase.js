var _ = require('lodash');
var mysql = require('mysql');
var ZongJi = require('zongji');

function updateAnalytics(dbConnection, query, params) {
  dbConnection.query(query, params, function(error, results, fields) {
    if (error) {
      if (error.sqlMessage) {
        console.error(`Failed to execute query ${error.sql}\nError: ${error.sqlMessage}`);
      } else {
        throw error;
      }
    } else {
      // TODO don't prepare the query twice
      console.info("Executed: " + mysql.format(query, params));
    }
  });
}

function rowHandler(getQuery) {
  return function (analytics, evt) {
    for (var row of evt.rows) {
      var q = getQuery(row);
      if (q) {
        updateAnalytics(analytics, q.query, q.params);
      }
    }
  };
}

//TODO This is almost identical as rowHandler
function rowUpdateHandler(getQuery) {
  return function (analytics, evt) {
    for (var update of evt.rows) {
      var q = getQuery(update.before, update.after);
      if (q) {
        updateAnalytics(analytics, q.query, q.params);
      }
    }
  };
}

function demap(obj) {
  var k = _.keys(obj)[0];
  return [k, obj[k]];
}

function columnChanged(before, after) {
  return function (col) {
    var a = after[col], b = before[col];
    if (a instanceof Date) {
      a = a.getTime();
      b = b.getTime();
    }
    return a != b;
  };
}

function view(tableMap, key, valueColumns) {
  var [viewName, sourceTable] = demap(tableMap);
  var columnNames = _.concat([key], valueColumns);
  return { handlers: { [sourceTable]: {
    writerows: rowHandler(function (row) {
      var values = _.at(row, columnNames);
      return {
        query: "INSERT INTO ?? (??) VALUES (?)",
        params: [viewName, columnNames, values]
      };
    }),
    updaterows: rowUpdateHandler(function (before, after) {
      var changedColumns = _.filter(valueColumns, columnChanged(before, after));
      if (!_.isEmpty(changedColumns)) {
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, _.pick(after, changedColumns), key, before[key]]
        };
      }
    }),
    deleterows: rowHandler(function (row) {
      return {
        query: "DELETE FROM ?? WHERE ??=?",
        params: [viewName, key, row[key]]
      };
    })
  }}};
}

function joinedView(tableMap, keyMap, valueColumns, options = {}) {
  var [viewName, sourceTable] = demap(tableMap);
  var [viewKey, joinKey] = demap(keyMap);
  return { handlers: { [sourceTable]: {
    writerows: rowHandler(function (row) {
      if (!options.filter || options.filter(row)) {
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, _.pick(row, valueColumns), viewKey, row[joinKey]]
        };
      }
    }),
    updaterows: rowUpdateHandler(function (before, after) {
      if (!options.filter || options.filter(after)) {
        var changedColumns = _.filter(valueColumns, function (col) { return before[col] != after[col]; });
        if (!_.isEmpty(changedColumns)) {
          return {
            query: "UPDATE ?? SET ? WHERE ??=?",
            params: [viewName, _.pick(after, changedColumns), viewKey, before[joinKey]]
          };
        }
      }
    }),
    deleterows: rowHandler(function (row) {
      if (!options.filter || options.filter(row)) {
        // Create an object with valueColumns as properties, each set to undefined
        var nullRow = _.reduce(valueColumns, (o, c) => _.set(o, c), {});
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, nullRow, viewKey, row[joinKey]]
        };
      }
    })
  }}};
}

function computedView(tableMap, keyMap, compute) {
  var [viewName, sourceTable] = demap(tableMap);
  var [viewKey, joinKey] = keyMap instanceof Object ? demap(keyMap) : [keyMap, keyMap];
  return { handlers: { [sourceTable]: {
    writerows: rowHandler(function (row) {
      var viewRow = compute(undefined, row);
      if (viewRow) {
        if (viewKey == joinKey) {
          var [columns, values] = _.unzip(_.entries(viewRow));
          return {
            query: "INSERT INTO ?? (??) VALUES (?)",
            params: [viewName, columns, values]
          };
        } else {
          return {
            query: "UPDATE ?? SET ? WHERE ??=?",
            params: [viewName, viewRow, viewKey, row[joinKey]]
          };
        }
      }
    }),
    updaterows: rowUpdateHandler(function (before, after) {
      var viewRow = compute(before, after);
      if (viewRow) {
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, viewRow, viewKey, before[joinKey]]
        };
      }
    }),
    deleterows: rowHandler(function (row) {
      var viewRow = compute(row, undefined);
      if (viewRow) {
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, viewRow, viewKey, before[joinKey]]
        };
      } else if (viewKey == joinKey) {
        return {
          query: "DELETE FROM ?? WHERE ??=?",
          params: [viewName, viewKey, row[joinKey]]
        };
      }
    })
  }}};
}

function Aggrebase(config) {
  this.config = config;
  this.analytics = mysql.createConnection(config.destConnection);

  this.initQueries = [];
  this.handlers = {};
}

Aggrebase.prototype.add = function(viewConfig) {
  if (viewConfig.initQuery) {
    this.initQueries.push(viewConfig.initQuery);
  }
  if (viewConfig.handlers) {
    _.merge(this.handlers, viewConfig.handlers);
  }
}

Aggrebase.prototype.init = function() {
  var self = this;
  self.analytics.connect();
  self.analytics.beginTransaction(function(error) {
    if (error) { throw error; }
    function checkError(error) {
      if (error) {
        return self.analytics.rollback(function() {
          throw error;
        });
      }
    }

    self.analytics.query("SHOW MASTER STATUS", function (error, results) {
      checkError(error);

      var startOptions = { from: {
        binlogName: results[0].File,
        binlogNextPos: results[0].Position
      }};
      _.map(_.initial(self.initQueries), function (q) {
        console.log("Executing: " + q);
        self.analytics.query(q, checkError);
      });

      var lastQ = _.last(self.initQueries);
      console.log("Executing: " + lastQ);
      self.analytics.query(lastQ, function (error) {
        checkError(error);
        self.analytics.commit(function (err) {
          checkError(err);
          self.start(startOptions);
        });
      });
    });
  });
}

Aggrebase.prototype.start = function(options) {
  var options = options || {};
  var startOptions = _.merge({
      includeEvents: ['rotate', 'tablemap', 'writerows', 'updaterows', 'deleterows'],
    }, 
    options.from || {startAtEnd: true}
  );

  if (options.from) {
    console.info(`Starting from ${startOptions.binlogName}:${startOptions.binlogNextPos}`);
  } else {
    console.info("Starting from latest position");
  }
  
  if (!this.analytics._connectCalled) {
    this.analytics.connect();
  }

  var self = this;
  this.zongji = new ZongJi(this.config.srcConnection);
  this.zongji.on('binlog', function(evt) {
    if (evt.tableMap) {
      var table = evt.tableMap[evt.tableId].tableName;
      var evtName = evt.getEventName();
      if (table in self.handlers && evtName in self.handlers[table]) {
        self.handlers[table][evtName](self.analytics, evt);
      }
    }
  });
  this.zongji.start(startOptions);
}

Aggrebase.prototype.stop = function() {
  this.zongji.stop();
  this.analytics.end();
}

Aggrebase.view = view;
Aggrebase.joinedView = joinedView;
Aggrebase.computedView = computedView;

module.exports = Aggrebase;
