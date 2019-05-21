/**
 * Utility Functions
 */

// Dependencies
var _ = require("lodash");
var moment = require("moment");

// Module Exports

var utils = (module.exports = {});

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function (value) {
  if (!value) return value;

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays and Objects as strings
  if (
    Array.isArray(value) ||
    (value.constructor && value.constructor.name === "Object")
  ) {
    try {
      value = JSON.stringify(value);
    } catch (e) {
      // just keep the value and let the db handle an error
      value = value;
    }
  }

  return value;
};

utils.buildOrderByStatement = function (criteria) {
  var queryPart = "ORDER BY ";

  // Sort through each sort attribute criteria
  _.each(criteria.sort, function (direction, attrName) {
    if (!criteria.joinMeta) {
      queryPart += "[" + attrName + "] ";
    } else {
      queryPart += "[" + criteria.joinMeta.child + "].[" + attrName + "] ";
    }

    // Basic MongoDB-style numeric sort direction
    if (direction === 1) {
      queryPart += "ASC, ";
    } else {
      queryPart += "DESC, ";
    }
  });

  // Remove trailing comma
  if (queryPart.slice(-2) === ", ") {
    queryPart = queryPart.slice(0, -2) + " ";
  }
  return queryPart;
};

/**
 * Builds a Select statement determining if Aggeregate options are needed.
 */

utils.buildSelectStatement = function (criteria, table) {
  var query = "SELECT ";

  if (
    criteria.groupBy ||
    criteria.sum ||
    criteria.average ||
    criteria.min ||
    criteria.max
  ) {
    // Append groupBy columns to select statement
    if (criteria.groupBy) {
      if (criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function (opt) {
          query += "[" + opt + "], ";
        });
      } else {
        query += "[" + criteria.groupBy + "], ";
      }
    }

    // Handle SUM
    if (criteria.sum) {
      if (criteria.sum instanceof Array) {
        criteria.sum.forEach(function (opt) {
          query += "SUM([" + opt + "]) AS [" + opt + "], ";
        });
      } else {
        query += "SUM([" + criteria.sum + "]) AS [" + criteria.sum + "], ";
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if (criteria.average instanceof Array) {
        criteria.average.forEach(function (opt) {
          query += "AVG(CAST([" + opt + "] AS FLOAT)) AS [" + opt + "], ";
        });
      } else {
        query +=
          "AVG(CAST([" +
          criteria.average +
          "] AS FLOAT)) AS [" +
          criteria.average +
          "], ";
      }
    }

    // Handle MAX
    if (criteria.max) {
      if (criteria.max instanceof Array) {
        criteria.max.forEach(function (opt) {
          query += "MAX([" + opt + "]) AS [" + opt + "], ";
        });
      } else {
        query += "MAX([" + criteria.max + "]) AS [" + criteria.max + "], ";
      }
    }

    // Handle MIN
    if (criteria.min) {
      if (criteria.min instanceof Array) {
        criteria.min.forEach(function (opt) {
          query += "MIN([" + opt + "]) AS [" + opt + "], ";
        });
      } else {
        query += "MIN([" + criteria.min + "]) AS [" + criteria.min + "], ";
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + " ";

    // Add FROM clause
    return (query += "FROM " + table + " WITH (NOLOCK) ");
  }

  //HANDLE SKIP
  if (criteria.skip) {
    var primaryKeySort = {};
    primaryKeySort[criteria.__primaryKey__] = 1;
    //@todo what to do with no primary key OR sort?
    criteria.sort = criteria.sort || primaryKeySort;
    query +=
      "ROW_NUMBER() OVER (" +
      utils.buildOrderByStatement(criteria) +
      ") AS '__rownum__', ";
  } else if (criteria.limit) {
    // SQL Server implementation of LIMIT
    query += "TOP " + criteria.limit + " ";
  }

  //  return query += '* FROM '+table ;
  if (!criteria.joinMeta) {
    var selectColumns = "*";
    if (criteria.select && criteria.select.indexOf('*') === -1) {
      selectColumns = "[" + criteria.select.join("],[") + "]";
    }

    query += selectColumns + " FROM " + table + " WITH (NOLOCK) ";
  } else {
    var joinMeta = criteria.joinMeta;
    query += " [" + joinMeta.middleLeft + "].[" + joinMeta.middleLeftKey + "]";
    var joinMetaSelectColumns = ",[" + joinMeta.child + "].";
    if (Array.isArray(criteria.joinMeta.select)) {
      joinMetaSelectColumns += criteria.joinMeta.select.join(
        ",[" + joinMeta.child + "]."
      );
    } else {
      joinMetaSelectColumns += "*";
    }
    query += joinMetaSelectColumns;
    query += " FROM [" + joinMeta.middleRight + "]" + " WITH (NOLOCK) ";
    query +=
      " JOIN [" +
      joinMeta.child +
      "] WITH (NOLOCK) ON " +
      "[" +
      joinMeta.middleRight +
      "].[" +
      joinMeta.middleRightKey +
      "] = [" +
      joinMeta.child +
      "].[" +
      joinMeta.childKey +
      "] ";

    //console.log('joinering query: ', query);
  }
  return query;
};

utils.convertDates = function (options) {
  // more info on the structure we're cleaning
  // https://sailsjs.com/documentation/concepts/models-and-orm/query-language
  var attributes = options.attributes
  var where = options.where;
  if (!where || !_.isObject(where)) {
    return;
  }
  var topLevelGroups = ['or', 'and']
  _.each(where, function (attrWhere, attrName) {
    // or/and arrays
    if (topLevelGroups.indexOf(attrName.toLowerCase()) > -1) {
      if (_.isArray(attrWhere)) {
        return _.each(attrWhere, function recurse(smallerWhere) {
          utils.convertDates({ attributes: options.attributes, where: smallerWhere })
        });
      }
      return;
    }

    // only deal with attributes that are dates in DB
    if (attributes[attrName] && attributes[attrName].type && attributes[attrName].type.indexOf('date') === 0) {
      // key pair strings
      if (_.isString(attrWhere)) {
        return where[attrName] = utils.toSqlDate(moment(attrWhere))
      }
      // in pairs
      if (_.isArray(attrWhere)) {
        return where[attrName] = _.map(attrWhere, function (individualWhere) { return utils.toSqlDate(moment(individualWhere)) })
      }
      // dates are fine, let'em through
      if (_.isDate(attrWhere)) {
        return
      }
      // modified pairs
      if (_.isObject(attrWhere)) {
        _.each(attrWhere, function (modifiedWhere, modifier) {
          if (_.isString(modifiedWhere)) {
            return attrWhere[modifier] = utils.toSqlDate(modifiedWhere)
          }
          // in modified pairs
          if (_.isArray(modifiedWhere)) {
            return attrWhere[modifier] = _.map(modifiedWhere, function (individualWhere) { return utils.toSqlDate(individualWhere) })
          }
        });
      }
    }
  });
};

utils.toSqlDate = function (date) {
  var momentDate;
  if (!date) {
    return date;
  }
  if (_.isString(date)) {
    momentDate = moment(date)
  } else if (_.isDate(date)) {
    momentDate = moment(date)
  }

  if (!momentDate) {
    throw new Error('invalid date');
  }

  return momentDate.utc().format('YYYY-MM-DD HH:mm:ss');

  // date = date.getUTCFullYear() +
  //   "-" +
  //   ("00" + (date.getUTCMonth() + 1)).slice(-2) +
  //   "-" +
  //   ("00" + date.getUTCDate()).slice(-2) +
  //   " " +
  //   ("00" + date.getUTCHours()).slice(-2) +
  //   ":" +
  //   ("00" + date.getUTCMinutes()).slice(-2) +
  //   ":" +
  //   ("00" + date.getUTCSeconds()).slice(-2);

  // return date;
}

