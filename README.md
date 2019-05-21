# Sails-MSSQL Adapter

MSSQL adapter for Sails framework and Waterline ORM. Allows you to use MSSQL via your models to store and retrieve data.  Also provides a `query()` method for a direct interface to execute raw SQL commands.

## Installation

Install from NPM.

```bash
$ npm install sails-mssql
```

## Branch Structure

| sails.js version | sails-mssql version | sails-mssql branch |
| ----------- | ----------- | ----------- |
| 1.x | 2.x | master |
| 0.12.x | 1.x | 0.12.x |

## Bugs &nbsp; [![NPM version](https://badge.fury.io/js/sails-mssql.svg)](http://npmjs.com/package/sails-mssql)

To report a bug, post on the [issues tab](https://github.com/intel/sails-mssql/issues).

## Contributing

[![NPM](https://nodei.co/npm/sails-mssql.png?downloads=true)](http://npmjs.com/package/sails-mssql)

### Running the tests

To run the tests, point this adapter at your database by specifying a [connection URL](http://sailsjs.com/documentation/reference/configuration/sails-config-datastores#?the-connection-url) and run `npm test`:

```
WATERLINE_ADAPTER_TESTS_URL=mssql://root:myc00lP4ssw0rD@localhost/adapter_tests npm test
```

> For more info, see [**Reference > Configuration > sails.config.datastores > The connection URL**](http://sailsjs.com/documentation/reference/configuration/sails-config-datastores#?the-connection-url), or [ask for help](http://sailsjs.com/support).

## License

MIT License

