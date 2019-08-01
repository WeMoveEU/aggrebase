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

function mergeHandlers(currentVal, inVal, _k, _d, _s, stack) {
  if (stack.size === 1) {
    if (currentVal === undefined) {
      return _.isArray(inVal) ? inVal : [inVal];
    } else {
      return _.flatten([currentVal, inVal]);
    }
  }
}

function processEvent(evt) {
  if (evt.tableMap) {
    var table = evt.tableMap[evt.tableId].tableName;
    var evtName = evt.getEventName();
    // console.log("Processing " + evtName + " on " + table);
    if (_.has(this.handlers, [table, evtName])) {
      var analytics = this.analytics;
      this.handlers[table][evtName].forEach((handler) => handler(analytics, evt));
    }
  }
}

// TODO replace with object destructuration
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

// TODO implement keyMap
function view(tableMap, key, valueColumns) {
  var [viewName, sourceTable] = demap(tableMap);
  var columnNames = _.concat([key], valueColumns);
  return { [sourceTable]: {
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
  }};
}

function joinedView(tableMap, keyMap, valueColumns, options = {}) {
  var [viewName, sourceTable] = demap(tableMap);
  var [viewKey, joinKey] = demap(keyMap);
  return { [sourceTable]: {
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
  }};
}

function computedView(tableMap, keyMap, compute) {
  var [viewName, sourceTable] = demap(tableMap);
  var [viewKey, joinKey] = keyMap instanceof Object ? demap(keyMap) : [keyMap, keyMap];
  return { [sourceTable]: {
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
  }};
}

function Aggrebase(config) {
  this.config = config;
  // TODO rename this property (destination?)
  this.analytics = mysql.createConnection(config.destConnection);

  this.initQueries = [];
  this.handlers = {};
}

// TODO read database name
Aggrebase.prototype.add = function(viewConfig) {
  if (viewConfig.initQueries) {
    this.initQueries = _.concat(this.initQueries, viewConfig.initQueries);
  }
  for (var source of viewConfig.sources) {
    var tableMap = {[viewConfig.name]: source.table_name};
    if (typeof viewConfig.columns === 'function') {
      var keyMap = source.foreignKey ? {[viewConfig.key]: source.foreignKey} : viewConfig.key;
      var sourceHandlers = computedView(tableMap, keyMap, source.columns);
    }
    else if (!source.foreignKey) {
      var sourceHandlers = view(tableMap, viewConfig.key, source.columns);
    }
    else {
      var keyMap = {[viewConfig.key]: source.foreignKey};
      var sourceHandlers = joinedView(tableMap, keyMap, source.columns, source.filter);
    }
    this.handlers = _.mergeWith(this.handlers, sourceHandlers, mergeHandlers);
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
        console.info("Executing: " + q);
        self.analytics.query(q, checkError);
      });

      var lastQ = _.last(self.initQueries);
      console.info("Executing: " + lastQ);
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

  this.zongji = new ZongJi(this.config.srcConnection);
  this.zongji.on('binlog', _.bind(processEvent, this));
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
