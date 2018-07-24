// Copyright IBM Corp. 2013,2016. All Rights Reserved.
// Node module: loopback-connector-postgresql
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

/*!
 * PostgreSQL connector for LoopBack
 */
'use strict';
var SG = require('strong-globalize');
var g = SG();
var postgresql = require('pg');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var util = require('util');
var debug = require('debug')('loopback:connector:postgresql');
var debugData = require('debug')('loopback:connector:postgresql:data');
var Promise = require('bluebird');

/**
 *
 * Initialize the PostgreSQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header PostgreSQL.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!postgresql) {
    return;
  }

  var dbSettings = dataSource.settings || {};
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;

  dataSource.driver = postgresql;
  dataSource.connector = new PostgreSQL(postgresql, dbSettings);
  dataSource.connector.dataSource = dataSource;

  definePostgreSQLTypes(dataSource);

  if (callback) {
    if (dbSettings.lazyConnect) {
      process.nextTick(function() {
        callback();
      });
    } else {
      dataSource.connecting = true;
      dataSource.connector.connect(callback);
    }
  }
};

function definePostgreSQLTypes(dataSource) {
  var modelBuilder = dataSource.modelBuilder;
  var defineType = modelBuilder.defineValueType ?
    // loopback-datasource-juggler 2.x
    modelBuilder.defineValueType.bind(modelBuilder) :
    // loopback-datasource-juggler 1.x
    modelBuilder.constructor.registerType.bind(modelBuilder.constructor);

  // The Point type is inherited from jugglingdb mysql adapter.
  // LoopBack uses GeoPoint instead.
  // The Point type can be removed at some point in the future.
  defineType(function Point() {
  });
}

/**
 * PostgreSQL connector constructor
 *
 * @param {PostgreSQL} postgresql PostgreSQL node.js binding
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the PostgreSQL DB server
 * @property {Number} port The port number of the PostgreSQL DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function PostgreSQL(postgresql, settings) {
  // this.name = 'postgresql';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'postgresql', settings);
  this.clientConfig = settings;
  if (settings.url) {
    // pg-pool doesn't handle string config correctly
    this.clientConfig.connectionString = settings.url;
  }
  this.clientConfig.Promise = Promise;
  this.pg = new postgresql.Pool(this.clientConfig);

  this.settings = settings;
  debug('Settings %j', settings);
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(PostgreSQL, SqlConnector);

PostgreSQL.prototype.getDefaultSchemaName = function() {
  return 'public';
};

/**
 * Connect to PostgreSQL
 * @callback {Function} [callback] The callback after the connection is established
 */
PostgreSQL.prototype.connect = function(callback) {
  var self = this;
  self.pg.connect(function(err, client, done) {
    self.client = client;
    process.nextTick(done);
    callback && callback(err, client);
  });
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @param {Object} [options] Options object
 * @callback {Function} [callback] The callback after the SQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[]) data The result from the SQL
 */
PostgreSQL.prototype.executeSQL = function(sql, params, options, callback) {
  var self = this;

  if (params && params.length > 0) {
    debug('SQL: %s\nParameters: %j', sql, params);
  } else {
    debug('SQL: %s', sql);
  }

  function executeWithConnection(connection, done) {
    connection.query(sql, params, function(err, data) {
      // if(err) console.error(err);
      if (err) debug(err);
      if (data) debugData('%j', data);
      if (done) {
        process.nextTick(function() {
          // Release the connection in next tick
          done(err);
        });
      }
      var result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = {affectedRows: data.rowCount, count: data.rowCount};

            if (data.rows)
              result.rows = data.rows;

            break;
          default:
            result = data.rows;
        }
      }
      callback(err ? err : null, result);
    });
  }

  var transaction = options.transaction;
  if (transaction && transaction.connector === this) {
    if (!transaction.connection) {
      return process.nextTick(function() {
        callback(new Error(g.f('Connection does not exist')));
      });
    }
    if (transaction.txId !== transaction.connection.txId) {
      return process.nextTick(function() {
        callback(new Error(g.f('Transaction is not active')));
      });
    }
    debug('Execute SQL within a transaction');
    // Do not release the connection
    executeWithConnection(transaction.connection, null);
  } else {
    self.pg.connect(function(err, connection, done) {
      if (err) return callback(err);
      executeWithConnection(connection, done);
    });
  }
};

PostgreSQL.prototype._modifyOrCreate = function(model, data, options, fields, cb) {
  var sql = new ParameterizedSQL('INSERT INTO ' + this.tableEscaped(model));
  var columnValues = fields.columnValues;
  var fieldNames = fields.names;
  if (fieldNames.length) {
    sql.merge('(' + fieldNames.join(',') + ')', '');
    var values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    sql.merge(values);
  } else {
    sql.merge(this.buildInsertDefaultValues(model, data, options));
}

sql.merge('ON DUPLICATE KEY UPDATE');
  var setValues = [];
  for (var i = 0, n = fields.names.length; i < n; i++) {
    if (!fields.properties[i].id) {
      setValues.push(new ParameterizedSQL(fields.names[i] + '=' +
          columnValues[i].sql, columnValues[i].params));
    }
  }

  sql.merge(ParameterizedSQL.join(setValues, ','));

  this.execute(sql.sql, sql.params, options, function(err, info) {
    if (!err && info && info.insertId) {
      data.id = info.insertId;
    }
    var meta = {};
    // When using the INSERT ... ON DUPLICATE KEY UPDATE statement,
    // the returned value is as follows:
    // 1 for each successful INSERT.
    // 2 for each successful UPDATE.
    // 1 also for UPDATE with same values, so we cannot accurately
    // report if we have a new instance.
    meta.isNewInstance = undefined;
    cb(err, data, meta);
  });
};

PostgreSQL.prototype.save =
PostgreSQL.prototype.updateOrCreate = function(model, data, options, cb) {
  var fields =  this.buildFields(model, data);
  this._modifyOrCreate(model, data, options, fields, cb);
};

PostgreSQL.prototype.getInsertedId = function(model, info) {
  var insertedId = info && typeof info.insertId === 'number' ?
    info.insertId : undefined;
  return insertedId;
};

PostgreSQL.prototype.buildInsertReturning = function(model, data, options) {
  var idColumnNames = [];
  var idNames = this.idNames(model);
  for (var i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

PostgreSQL.prototype.buildInsertDefaultValues = function(model, data, options) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// postgresql 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-postgresql/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 PostgreSQL.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

PostgreSQL.prototype.fromColumnValue = function(prop, val) {
  if (val == null) {
    return val;
  }
  var type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
      val === 't' || val === '1');
    }
  } else if (prop && type === 'GeoPoint' || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      var point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1],
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now pg driver converts point to {x: lng, y: lat}
      return {
        lng: val.x,
        lat: val.y,
      };
    } else {
      return val;
    }
  } else {
    return val;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
PostgreSQL.prototype.dbName = function(name) {
  if (!name) {
    return name;
  }
  // PostgreSQL default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  var escaped = '"';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  var hasBackslash = false;
  var escaped = '\'';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*
 * Check if a value is attempting to use nested json keys
 * @param {String} property The property being queried from where clause
 * @returns {Boolean} True of the property contains dots for nested json
 */
function isNested(property) {
  return property.split('.').length > 1;
}

/*
 * Overwrite the loopback-connector column escape
 * to allow querying nested json keys
 * @param {String} model The model name
 * @param {String} property The property name
 * @returns {String} The escaped column name, or column with nested keys for deep json columns
 */
PostgreSQL.prototype.columnEscaped = function(model, property) {
  if (isNested(property)) {
    // Convert column to PostgreSQL json style query: "model"->>'val'
    var self = this;
    return property
      .split('.')
      .map(function(val, idx) { return (idx === 0 ? self.columnEscaped(model, val) : escapeLiteral(val)); })
      .reduce(function(prev, next, idx, arr) {
        return idx == 0 ? next : idx < arr.length - 1 ? prev + '->' + next : prev + '->>' + next;
      });
  } else {
    return this.escapeName(this.column(model, property));
  }
};

/*!
 * Escape the name for PostgreSQL DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
PostgreSQL.prototype.escapeName = function(name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

PostgreSQL.prototype.escapeValue = function(value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  // Can't send functions, objects, arrays
  if (typeof value === 'object' || typeof value === 'function') {
    return null;
  }
  return value;
};

PostgreSQL.prototype.tableEscaped = function(model) {
  var schema = this.schema(model) || 'public';
  return this.escapeName(schema) + '.' +
    this.escapeName(this.table(model));
};

function buildLimit(limit, offset) {
  var clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

PostgreSQL.prototype._buildLimit = function(model, limit, offset) {
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  return 'LIMIT ' + (offset ? (offset + ',' + limit) : limit);
};

PostgreSQL.prototype.applyPagination = function(model, stmt, filter) {
  var limitClause = this._buildLimit(model, filter.limit, filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

PostgreSQL.prototype.buildExpression = function(columnName, operator,
    operatorValue, propertyDefinition) {
  switch (operator) {
    case 'like':
      return new ParameterizedSQL(columnName + "::TEXT LIKE ? ESCAPE E'\\\\'",
          [operatorValue]);
    case 'ilike':
      return new ParameterizedSQL(columnName + "::TEXT ILIKE ? ESCAPE E'\\\\'",
          [operatorValue]);
    case 'nlike':
      return new ParameterizedSQL(columnName + "::TEXT NOT LIKE ? ESCAPE E'\\\\'",
          [operatorValue]);
    case 'nilike':
      return new ParameterizedSQL(columnName + "::TEXT NOT ILIKE ? ESCAPE E'\\\\'",
          [operatorValue]);
    case 'regexp':
      if (operatorValue.global)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`m`}} flag');

      var regexOperator = operatorValue.ignoreCase ? ' ~* ?' : ' ~ ?';
      return new ParameterizedSQL(columnName + regexOperator,
          [operatorValue.source]);
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper('buildExpression', columnName, operator,
          operatorValue, propertyDefinition);
  }
};

PostgreSQL.prototype.buildSelect = function(model, filter, options) {
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
    g.warn(JSON.stringify(idNames));
  }
  var tableAlias = getTableAlias(model);
  var selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter, tableAlias) +
    ' FROM ' + this.tableEscaped(model) + ' ' + tableAlias + ' '
  );
  if (filter) {
    let aliases = {};
    if (filter.where) {
      var joins = this.buildJoins(model, filter.where, tableAlias);
      if (joins.joinStmt.sql) {
        aliases = joins.aliases;
        selectStmt.sql = selectStmt.sql.replace('SELECT', 'SELECT DISTINCT ' +
          tableAlias + `.${idNames[0]}, `);
        selectStmt.merge(joins.joinStmt);
      }
      var whereStmt = this.buildWhere(model, filter.where, tableAlias, aliases);
      selectStmt.merge(whereStmt);
    }

    if (filter.order) {
      var order = this.buildOrderBy(model, filter.order, aliases);
      selectStmt.merge(order.orderBy);
      if (order.columnNames) {
        selectStmt.sql = selectStmt.sql.replace('FROM', ', ' +
          order.columnNames + ' FROM ');
      }
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter);
    }
  }
  selectStmt.sql = selectStmt.sql;
  return this.parameterize(selectStmt);
};

PostgreSQL.prototype.buildJoins = function(model, where, tableAlias) {
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var aliases = {};
  var joinStmts = [];
  for (var key in where) {
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var stmt = new ParameterizedSQL('', []);
      var branches = [];
      var branchParams = [];
      var clauses = where[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self.buildJoins(model, clauses[i], tableAlias, aliases);
          aliases = Object.assign(aliases, stmtForClause.aliases);
          if (stmtForClause.joinStmt.sql) {
            branchParams = branchParams.concat(stmtForClause.joinStmt.params);
            branches.push(stmtForClause.joinStmt.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' '),
          params: branchParams,
        });
        joinStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    var p = props[key];
    if (p == null) {
      let relations = self.getModelDefinition(model).settings.relations;
      if (relations && relations[key]) {
        let relation = relations[key];
        /* I think model should be taken from relation so it actually yields the child table alias. MySQL author uses here just model, which yields parent's another alias. */ 
        let childTableAlias = getTableAlias(relation.model);
        aliases[relation.model] = childTableAlias;
        let idNames = this.idNames(model);
        let childIdNames = this.idNames(relation.model);
        let foreignKey = relation.foreignKey != '' ? relation.foreignKey :
          relation.model.toLowerCase() + 'Id';
        let type = relation.type;
        let joinThrough, joinOn;
        if (type == 'belongsTo') {
          joinOn = tableAlias + '.' + foreignKey + ' = ' + childTableAlias + `.${childIdNames[0]}`;
        } else if (type == 'hasMany' && relation.through) {
          const throughModel = this.tableEscaped(relation.through);
          const throughModelAlias = getTableAlias(model);
          aliases[relation.through] = throughModelAlias;
          let parentForeignKey = relation.foreignKey ||
            lowerCaseFirstLetter(model) + `${idNames[0]}`;
          let childForeignKey = relation.keyThrough ||
            lowerCaseFirstLetter(relation.model) + 'Id';
          joinThrough = `LEFT JOIN ${throughModel} ${throughModelAlias} 
            ON ${throughModelAlias}.${parentForeignKey}` +
            ` = ${tableAlias}.${idNames[0]}`;
          joinOn = throughModelAlias + '.' + childForeignKey +
            ' = ' + childTableAlias + `.${idNames[0]}`;
        } else {
          joinOn = childTableAlias + '.' + foreignKey + ' = ' + tableAlias + `.${idNames[0]}`;
        }
        const joinTable = this.tableEscaped(relation.model);
        let join = new ParameterizedSQL(
          `LEFT JOIN ${joinTable} ${childTableAlias} ON ${joinOn}`, []);
        if (joinThrough) {
          join = new ParameterizedSQL(joinThrough, []).merge(join);
        }
        var recursiveResult = self.buildJoins(relation.model, where[key],
          childTableAlias);
        join.merge(recursiveResult.joinStmt);
        aliases = Object.assign(aliases, recursiveResult.aliases);
        joinStmts.push(join);
      } else {
        // Unknown property, ignore it
        debug('Unknown property %s is skipped for model %s', key, model);
      }
    }
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = joinStmts.length; k < s; k++) {
    sqls.push(joinStmts[k].sql);
    params = params.concat(joinStmts[k].params);
  }
  var joinStmt = new ParameterizedSQL({
    sql: sqls.join(' '),
    params: params,
  });
  var result = {
    aliases: aliases,
    joinStmt: joinStmt,
  };
  return result;
};

PostgreSQL.prototype.buildOrderBy = function(model, order, aliases){
  if (!order) {
    return '';
  }
  var self = this;
  if (typeof order === 'string') {
    order = [order];
  }
  var clauses = [];
  var columnNames = [];
  const n = order.length;
  for (var i = 0; i < n; i++) {
    var sign = '';
    var t  = order[i].split(/[\s,]+/);
    if (t[0].indexOf('-') === 0 ) {
      sign = '-';
      t[0] = t[0].substr(1);
    }
    if (t.length === 1) {
      clauses.push(sign + self.columnEscaped(model, order[i]));
    } else {
      var key = t[0];
      if (key.indexOf('.') > -1) {
        const modelAndProperty = key.split('.');
        const relations = this.getModelDefinition(model).settings.relations;
        if (relations && relations[modelAndProperty[0]]) {
          var relation = relations[modelAndProperty[0]];
          const alias = aliases[relation.model];
          if (alias) {
            clauses.push(sign + alias + '.' + modelAndProperty[1] + ' ' + t[1]);
            columnNames.push(alias + '.' + modelAndProperty[1] +
            ' as ' + alias + '_orderBy' + modelAndProperty[1]);
          }
        }
      } else{
        clauses.push(sign + self.columnEscaped(model, t[0]) + ' ' + t[1]);
      } 
    }
  }
  var result = {
    orderBy: clauses.length > 0 ? 'ORDER BY ' + clauses.join(',') : '',
    columnNames: columnNames.join(','),
  };
return result;
};

PostgreSQL.prototype.buildColumnNames = function(model, filter, tableAlias) {
  var fieldsFilter = filter && filter.fields;
  var cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return tableAlias ? tableAlias + '.*' : '*';
  }
  var self = this;
  var keys = Object.keys(cols);
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function(f) {
      return cols[f];
    });
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    var included = [];
    var excluded = [];
    keys.forEach(function(k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function(e) {
        var index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }
  }
  var names = keys.map(function(c) {
    const columnEscaped = self.columnEscaped(model, c);
    return tableAlias ? tableAlias + '.' + columnEscaped : columnEscaped;
  });
  return names.join(',');
};

/**
 * Disconnect from PostgreSQL
 * @param {Function} [cb] The callback function
 */
PostgreSQL.prototype.disconnect = function disconnect(cb) {
  if (this.pg) {
    debug('Disconnecting from ' + this.settings.hostname);
    var pg = this.pg;
    this.pg = null;
    pg.end();  // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

PostgreSQL.prototype.ping = function(cb) {
  this.execute('SELECT 1 AS result', [], cb);
};

PostgreSQL.prototype.getInsertedId = function(model, info) {
  var idColName = this.idColumn(model);
  var idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} where An object for the where conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
/*PostgreSQL.prototype.buildWhere = function(model, where) {
  var whereClause = this._buildWhere(model, where);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};*/
PostgreSQL.prototype.buildWhere = function(model, where, tableAlias, aliases) {
  var whereClause = this._buildWhere(model, where, tableAlias, aliases);
  if (whereClause.sql) {
    whereClause.sql = 'WHERE ' + whereClause.sql;
  }
  return whereClause;
};
/**
 * @private
 * @param model
 * @param where
 * @returns {ParameterizedSQL}
 */

PostgreSQL.prototype._buildWhere = function(model, where, tableAlias, aliases) {
  if (!where) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var whereStmts = [];
  for (var key in where) {
    var stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var branches = [];
      var branchParams = [];
      var clauses = where[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self._buildWhere(model, clauses[i], tableAlias, aliases);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: ' ( ' + branches.join(' ' + key.toUpperCase() + ' ') + ' ) ',
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    var p = props[key];
    if (p == null) {
      let relations = self.getModelDefinition(model).settings.relations;
      if (relations && relations[key]) {
        let relation = relations[key];
        let childWhere = self._buildWhere(relation.model, where[key],
          aliases[relation.model], aliases);
        whereStmts.push(childWhere);
      } else {
        debug('Unknown property %s is skipped for model %s', key, model);
      }
      continue;
    }
    /* eslint-disable one-var */
    var columnEscaped = self.columnEscaped(model, key);
    var columnName = tableAlias ? tableAlias + '.' + columnEscaped : columnEscaped;
    var expression = where[key];
    var columnValue;
    var sqlExp;
    /* eslint-enable one-var */
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && expression.constructor === Object) {
      var operator = Object.keys(expression)[0];
      // Get the expression without the operator
      expression = expression[operator];
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (var j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j]));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          var v1 = columnValue[0] === undefined ? null : columnValue[0];
          var v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if (operator === 'regexp' && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression);
      }
      sqlExp = self.buildExpression(
        columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          stmt.merge(columnName + '=').merge(columnValue);
        } else {
          stmt.merge({
            sql: columnName + '=?',
            params: [columnValue],
          });
        }
      }
    }
    whereStmts.push(stmt);
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = whereStmts.length; k < s; k++) {
    sqls.push(whereStmts[k].sql);
    params = params.concat(whereStmts[k].params);
  }
  var whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};
/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
PostgreSQL.prototype.toColumnValue = function(prop, val) {
  if (val == null) {
    // PostgreSQL complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    } else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    var iso = val.toISOString();

    // Pass in date as UTC and make sure Postgresql stores using UTC timezone
    return new ParameterizedSQL({
      sql: '?::TIMESTAMP WITH TIME ZONE',
      params: [iso],
    });
  }

  // PostgreSQL support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint' || prop.type.name === 'Point') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      // Postgres point is point(lng, lat)
      params: [val.lng, val.lat],
    });
  }

  return val;
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForIdentifier = function(key) {
  throw new Error(g.f('{{Placeholder}} for identifiers is not supported'));
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForValue = function(key) {
  return '$' + key;
};

PostgreSQL.prototype.getCountForAffectedRows = function(model, info) {
  return info && info.affectedRows;
};

PostgreSQL.prototype.count = function(model, where, options, cb) {
  if (typeof where === 'function') {
    // Backward compatibility for 1.x style signature:
    // count(model, cb, where)
    var tmp = options;
    cb = where;
    where = tmp;
  }

  var tableAlias = getTableAlias(model);

  var stmt = new ParameterizedSQL('SELECT count(*) as "cnt" FROM ' +
    this.tableEscaped(model) + ' ' + tableAlias + ' ');
  var joins = this.buildJoins(model, where, tableAlias);
  var aliases = {};
  if (joins.joinStmt.sql) {
    stmt.sql = stmt.sql.replace('count(*)', 'COUNT(DISTINCT ' + tableAlias + '.id)');
    stmt.merge(joins.joinStmt);
    aliases = joins.aliases;
  }
  stmt = stmt.merge(this.buildWhere(model, where, tableAlias, aliases));
  stmt = this.parameterize(stmt);
  this.execute(stmt.sql, stmt.params,
    function(err, res) {
      if (err) {
        return cb(err);
      }
      var c = (res && res[0] && res[0].cnt) || 0;
      // Some drivers return count as a string to contain bigint
      // See https://github.com/brianc/node-postgres/pull/427
      cb(err, Number(c));
    });
};


function getTableAlias(model) {
  return model + Math.random().toString().replace('.', '').replace('-', '');
};

function lowerCaseFirstLetter(text) {
  return text.charAt(0).toLowerCase() + text.slice(1);
};

require('./discovery')(PostgreSQL);
require('./migration')(PostgreSQL);
require('./transaction')(PostgreSQL);
