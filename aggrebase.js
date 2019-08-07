var _ = require('lodash');
var mysql = require('mysql');
var ZongJi = require('zongji');

function updateDestination(dbConnection, query, params) {
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
  return function (destConnection, evt) {
    for (var row of evt.rows) {
      var q = getQuery(row);
      if (q) {
        updateDestination(destConnection, q.query, q.params);
      }
    }
  };
}

//TODO This is almost identical as rowHandler
function rowUpdateHandler(getQuery) {
  return function (destConnection, evt) {
    for (var update of evt.rows) {
      var q = getQuery(update.before, update.after);
      if (q) {
        updateDestination(destConnection, q.query, q.params);
      }
    }
  };
}

function mergeHandlers(currentVal, inVal, _k, _d, _s, stack) {
  if (stack.size === 2) {
    if (currentVal === undefined) {
      return _.isArray(inVal) ? inVal : [inVal];
    } else {
      return _.flatten([currentVal, inVal]);
    }
  }
}

function processEvent(evt) {
  if (evt.tableMap) {
    var db = evt.tableMap[evt.tableId].parentSchema;
    var table = evt.tableMap[evt.tableId].tableName;
    var evtName = evt.getEventName();
    // console.debug("Processing " + evtName + " on " + db + "." + table);
    if (_.has(this.handlers, [db, table, evtName])) {
      this.handlers[db][table][evtName].forEach((handler) => handler(this.destConnection, evt));
    }
  }
}

function demap(obj) {
  return _.entries(obj)[0];
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

function viewHandlers(sourceDb, sourceTable, sourceKey, viewName, viewKey, valueColumns) {
  var srcColumns = [sourceKey, ...valueColumns];
  var destColumns = [viewKey, ...valueColumns];
  return { [sourceDb]: { [sourceTable]: {
    writerows: rowHandler(function (row) {
      var values = _.at(row, srcColumns);
      return {
        query: "INSERT INTO ?? (??) VALUES (?)",
        params: [viewName, destColumns, values]
      };
    }),
    updaterows: rowUpdateHandler(function (before, after) {
      var changedColumns = _.filter(valueColumns, columnChanged(before, after));
      if (!_.isEmpty(changedColumns)) {
        return {
          query: "UPDATE ?? SET ? WHERE ??=?",
          params: [viewName, _.pick(after, changedColumns), viewKey, before[sourceKey]]
        };
      }
    }),
    deleterows: rowHandler(function (row) {
      return {
        query: "DELETE FROM ?? WHERE ??=?",
        params: [viewName, viewKey, row[sourceKey]]
      };
    })
  }}};
}

function joinedViewHandlers(sourceDb, sourceTable, joinKey, viewName, viewKey, valueColumns, options = {}) {
  return { [sourceDb]: { [sourceTable]: {
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

function computedViewHandlers(sourceDb, sourceTable, joinKey, viewName, viewKey, compute) {
  return { [sourceDb]: { [sourceTable]: {
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
  this.destConnection = mysql.createConnection(config.destConnection);

  this.initQueries = [];
  this.handlers = {};
}

Aggrebase.prototype.add = function(viewConfig) {
  if (viewConfig.init_queries) {
    this.initQueries = _.concat(this.initQueries, viewConfig.init_queries);
  }
  for (var source of viewConfig.sources) {
    if (typeof viewConfig.columns === 'function') {
      var joinKey = source.foreign_key ? source.foreign_key : viewConfig.key;
      var eventHandlers = computedViewHandlers(source.database, source.table_name, joinKey, viewConfig.name, viewConfig.key, source.columns);
    }
    else if (!source.foreign_key) {
      var eventHandlers = viewHandlers(source.database, source.table_name, viewConfig.key, viewConfig.name, viewConfig.key, source.columns);
    }
    else {
      var eventHandlers = joinedViewHandlers(source.database, source.table_name, source.foreign_key, viewConfig.name, viewConfig.key, source.columns, source.filter);
    }
    this.handlers = _.mergeWith(this.handlers, eventHandlers, mergeHandlers);
  }
}

Aggrebase.prototype.init = function() {
  var self = this;
  self.destConnection.connect();
  self.destConnection.beginTransaction(function(error) {
    if (error) { throw error; }
    function checkError(error) {
      if (error) {
        return self.destConnection.rollback(function() {
          throw error;
        });
      }
    }

    self.destConnection.query("SHOW MASTER STATUS", function (error, results) {
      checkError(error);

      var startOptions = { from: {
        binlogName: results[0].File,
        binlogNextPos: results[0].Position
      }};
      _.map(_.initial(self.initQueries), function (q) {
        console.info("Executing: " + q);
        self.destConnection.query(q, checkError);
      });

      var lastQ = _.last(self.initQueries);
      console.info("Executing: " + lastQ);
      self.destConnection.query(lastQ, function (error) {
        checkError(error);
        self.destConnection.commit(function (err) {
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
  
  if (!this.destConnection._connectCalled) {
    this.destConnection.connect();
  }

  this.zongji = new ZongJi(this.config.srcConnection);
  this.zongji.on('binlog', _.bind(processEvent, this));
  this.zongji.start(startOptions);
}

Aggrebase.prototype.stop = function() {
  this.zongji.stop();
  this.destConnection.end();
}

module.exports = Aggrebase;
