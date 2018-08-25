var Promise = require('promise');
var dbEngine = require('mssql'); 
//var util = require('util');

function Database( config )
{
	this.config = config || {};
	this.connection = null;

}



Database.prototype.disconnect = function()
{
	var cnn = this.connection;
	if( !cnn )
		return Promise.resolve();

	console.log( 'disconnecting' );
	this.connection = null;
	
	return new Promise( function( resolve, reject )
	{
		//console.log('Connection closed');
		cnn.close();
		resolve();
	});
};

Database.prototype.connect = function()
{
	if( this.connection )
		return Promise.resolve( this.connection );

	var me = this;
	return new Promise( function( resolve, reject )
	{
		me.connection = new dbEngine.ConnectionPool(me.config);
		me.connection.on( 'connect', function()
		{
			console.log('Connection opened');
		});
		me.connection.on( 'close', function()
		{
			console.log('Connection closed');
			me.disconnect(); 
		});
		me.connection.on( 'error', function(err)
		{
			console.log(err);
		});
		
		me.connection.connect( function( err, msg )
		{
			if( err )
				reject( err );
			else
				resolve( me.connection );
		});
	});
};

Database.prototype.startTransaction = function()
{
	var me = this;
	return this.connect()
	.then( function( connection )
	{
		var transaction = new dbEngine.Transaction( connection );
		return new Promise( function( resolve, reject )
		{
			transaction.begin( function( err )
			{
				if( err )
					reject( err );
				else
				{
					me.transaction = transaction;
					resolve( transaction );
				}
			});
		});
	});
};

Database.prototype.runInTransaction = function( work )
{
	if( typeof(work) != 'function' )
		return Promise.reject( new Error( 'runInTransaction: arg is not a function' ) );
		
	var me = this;
	return this.startTransaction()
	.then( function()
	{
		return work();
	})
	.then( function( result )
	{
		return me.commit( result );
	}
	,function( err )
	{
		return me.rollback( err );
	});
};

Database.prototype.commit = function( result )
{
	if( !this.transaction )
		return Promise.reject( new Error('No transactions to commit') );

	var me = this;
	return new Promise( function( resolve, reject )
	{
		me.transaction.commit( function( err, recordset )
		{
			if( err )
				reject( err );
			else
			{
				me.transaction = null;
				resolve( result || recordset );
			}
		});
	});
};

Database.prototype.rollback = function( result )
{
	if( !this.transaction )
		return Promise.reject( new Error('No transactions to rollback') );

	var me = this;
	return new Promise( function( resolve, reject )
	{
		me.transaction.rollback( function( err )
		{
			if( err )
				reject( err );
			else
			{
				me.transaction = null;
				resolve( result );
			}
		});
	});
};

Database.prototype.runQuery = function( query, callback, args )
{
	var me = this;
	var parms = args || {};

	this.connect().then( function( connection )
	{
		// Query
		var columns = {};

		var request = new dbEngine.Request(connection); // or: var request = connection.request();

		request.verbose = parms.verbose == null ? false : parms.verbose;
		request.stream = parms.stream == null ? true : parms.stream;
		request.multiple = true;

		try
		{
			request.on( 'recordset', function(cols_metadata)
			{
				//console.log('recordset');
				//console.log(columns);
				columns = cols_metadata;
			});
			request.on( 'row', function(row)
			{
				//console.log('row');
				if(callback) callback( null, { row: row, columns: columns, done: false } );
			});
			request.on( 'done', function(retValue)
			{
				//console.log('done');
				if(callback) callback( null, { row: null, columns: columns, done: true } );
			});
			request.on( 'error', function(err)
			{
				console.log( 'Error: ' + err );
				if(callback) callback( err, {} );
			});
			
			if( request.stream )
				request.query(query);
			else
				request.query(query, callback);
		}
		catch( err )
		{
			console.log( err );
		}
	},
	function(err)
	{
		// ... error checks
		console.log( 'Error: ' + err );
		if(callback) callback( err );
	});
};
Database.prototype.iterate = function( query, args, ifunc )
{
	var mapf = ifunc;
	if( args != null )
	{
		if( typeof(args) == 'function' )
		{
			mapf = args;
			args = null;
		}
	}

	var me = this;
	return new Promise( function( resolve, reject )
	{
		var result = null;
		me.runQuery( query, function( err, data )
		{
			if( err || !data )
			{
				reject(err);
				return;
			}
			
			if( data.done )
			{
				resolve( result );
				return;
			}

			result = mapf( data.row );

		}, args );
	});
};
Database.prototype.execute = function( query, args, mapfunc )
{
	var mapf = mapfunc;
	if( args != null )
	{
		if( typeof(args) == 'function' )
		{
			mapf = args;
			args = null;
		}
	}

	var rows = [];
	return this.iterate( query, args, function( row )
	{
		if( mapf )
			rows.push( mapf(data.row || {}) );
		else
			rows.push( data.row || {} );

		return rows;
	})
	.then( function( val )
	{
		return (val || rows);
	});
};

Database.prototype.buildSqlSelect = function( args )
{
	if( args == null ) return null;
	if( typeof( args ) == 'string' ) return args;
	
	var a = args || {};
	
	var fields = args.fields || '*';
	var filter = args.filter || args.where;
	var order = args.order;
	var table = args.table || args.from;
	if( !table )
	{
		console.log( 'ERR: No table specified in select' );
		console.log( args );
		return null;
	}

	var limit = args.limit;
	if( !isNaN( limit ) )
		limit = 'top ' + limit + ' ';
	else
		limit = '';
	
	var sql = 'select ' + limit + fields.join(',')
			+ ' from ' + table
			;

	if( filter )
		sql += ' where ' + filter;

	if( order )
		sql += ' order by ' + order;
		
	return sql;
};

function defaultFieldMap( row )
{
	var fields = Object.keys(row);

	var field_map = {};
	for( var i in fields )
	{
		var f = fields[i];
		field_map[f] = f;
	}
	return field_map;
};


Database.prototype.select = function( args )
{
	var sql = this.buildSqlSelect( args );
	if( !sql )
		return Promise.reject( new Error('Wrong sql select') );

	var me = this;
	return new Promise( function( resolve, reject )
	{
		var field_map = args.field_map;

		//console.log(sql);
		var entries = [];
		me.runQuery( sql, function( err, data )
		{
			if( err || !data )
			{
				reject(err);
				return;
			}
			
			if( data.done )
			{
				resolve( entries );
			}
			else
			{
				var row = data.row || {};
				var entry = {};
				
				if( !field_map ) field_map = defaultFieldMap( row );
				for( var x in field_map )
				{
					var field_name = field_map[x];
					entry[x] = row[field_name];
				}
				
				entries.push( entry );
			}
		});
	});
};

Database.prototype.update = function( args )
{
	// @parm args is an object as follow
	//{
	//	table: <name of the table to update>
	//	fields: <object with fieldname: value pairs>
	//	filter: optional where condition as string
	//}
	if( !args )
		return Promise.reject( new Error( 'No args in Database.update' ) );
	
	var table = args.table;
	if( !table )
		return Promise.reject( new Error( 'No table in Database.update' ) );
	
	var fields = args.fields;
	if( !fields )
		return Promise.reject( new Error( 'No fields in Database.update' ) );

	var me = this;
	
	return new Promise( function( resolve, reject )
	{
		var sql = 'update ' + table + ' SET ';
		var set = [];
		for( var name in fields )
		{
			var value = fields[name];
			set.push( name + ' = ' + value );
		}
		
		sql += set.join(',');
		
		if( args.filter )
			sql += ' where ' + args.filter;

		//console.log(sql);
		resolve( me.execute( sql ) );
	});
};

function buildInsertStatement( args )
{
	if( !args ) return null;

	var table = args.table;
	var values = args.values;
	
	if( !table || !values )
		return null;

	var fields = null;
	try
	{
		fields = Object.keys(values);
	}
	catch(e)
	{
		console.log('wrong values argument to buildInsertStatement: not an object');
		return null;
	}

	var fvalues = [];
	for( var i=0; i < fields.length; i++ )
	{
		var fname = fields[i];
		var fval = values[fname];
		if( fval != null )
		{
			fvalues.push( fval );
		}
	}
	

	var statement = "INSERT INTO " + table + "(" + fields.join(',') + ") VALUES (" + fvalues.join(',') + ")";
		
	//console.log( statement );
	return statement;
}

function buildDeleteStatement( args )
{
	if( !args ) return null;

	var table = args.table;
	var where = args.where;

	if( !table || !where )
		return null;

	var statement = "DELETE FROM " + table + " WHERE " + where;
		
	//console.log( statement );
	return statement;
}

Database.prototype.insert = function( statement, default_result ) // MSSQL dependant
{
	var me = this;
	return new Promise( function( resolve, reject )
	{
		if( typeof(statement) == 'object' )
		{
			statement = buildInsertStatement( statement );
			if( !statement )
			{
				reject( 'Invalid insert statement' );
				return;
			}
		}
		
		var result = default_result || {};

		// MSSQL 
		var sql = statement + ";SELECT SCOPE_IDENTITY() as newid;";
			
		//console.log( sql );
		me.runQuery( sql, function( err, data )
		{
			if( err )
			{
				reject(err);
				return;
			}
			
			if( data.done )
			{
				resolve(result);
				return;
			}
				
			var row = data.row || {};
			result.newid = row['newid'] || -1;
		});
	});
};

Database.prototype.delete = function( statement )
{
	var me = this;
	return new Promise( function( resolve, reject )
	{
		if( typeof(statement) == 'object' )
		{
			statement = buildDeleteStatement( statement );
			if( !statement )
			{
				reject( 'Invalid delete statement' );
				return;
			}
		}
		
		var sql = statement;	
		//console.log( sql );

		me.runQuery( sql, function( err, data )
		{
			if( err )
			{
				reject(err);
				return;
			}

			if( data.done )
			{
				resolve(true);
				return;
			}			
		});
	});
};

function pad2(n)
{
	if( typeof(n) == 'number' )
	{
		if( n >= 0 && n < 10 )
			return '0'+n;
		else
			return ''+n;
	}
	
	if( typeof(n) == 'string' )
	{
		if( n.length == 1 )
			return '0'+n;
		if( n.length > 1 )
			return n.substr(0,2);
	}
	
	return '00';
}

function formatDate( dt )
{
	var d = dt || new Date();
	return d.getFullYear() + '-' + pad2(d.getMonth() + 1) + '-' + pad2(d.getDate());
}

function formatTime( dt )
{
	var d = dt || new Date();
	return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
}

function formatDateTime( dt )
{
	var d = dt || new Date();
	return formatDate(d) + ' ' + formatTime(d);
}

function escapeString( str ) // MSSQL DEPENDANT
{
	try {
		// Ms SQL Server ...
		var s = str.replace( /\'/g, "''" );
		s = s.replace( /\%/g, "%%" );
		return s;
	}
	catch( err ) {
		console.log( "E: DbPromise::escapeString( %s ) : %s", str, err );
		return '{err}';
	}
};

function quote(str)
{
	return "'"+escapeString(str)+"'";
}

function quoteOrNull(str)
{
	if( !str ) return 'NULL';
	return "'"+escapeString(str)+"'";
}

function begins_with(str) // MSSQL DEPENDANT
{
	return " like '"+escapeString(str)+"%'";
}

function contains(str) // MSSQL DEPENDANT
{
	return " like '%"+escapeString(str)+"%'";
}

function ifNull( v, def )
{
	return v == null ? def : v;
}

function intOrNull( v )
{
	var i = parseInt(v);
	if( isNaN(i) )
		return 'NULL';
	else
		return i;
}

function checkBoxVal( v, def, values )
{
	if( v == null )
		return def;

	var checked_value = 1;
	var unchecked_value = 0;

	if( values )
	{
		if( values.checked ) checked_value = values.checked;
		if( values.unchecked ) unchecked_value = values.unchecked;
	}
	
	if( typeof(v) == 'string' )
	{
		if( v == 'yes' || v == 'on' || v == 'true' || v == 'checked' )
			return checked_value;
		else
			return unchecked_value;
	}


	return v ? checked_value : unchecked_value;
}

function buildInsertSQL( table, values )
{
	if( !values || !values.length ) return '';

	var sql = 'INSERT INTO '+table+' (';
	var v0 = values[0];
	var keys = Object.keys(v0);
	
	var i;
	var fnames = [];
	for( i=0; i<keys.length; i++ )
	{
		fnames.push( keys[i] );
	}
	
	sql += fnames.join(',') + ' ) VALUES \n';
	
	var rows = [];
	for( var j=0; j<values.length; j++ )
	{
		var v = values[j];
		var r = [];
		for( i=0; i<fnames.length; i++ )
		{
			var fname = fnames[i];
			var fvalue = v[ fname ];
			r.push( fvalue );
		}
		rows.push( '('+r.join(',')+')' );
	}
	
	sql += rows.join(',\n');
	return sql;
}

Database.prototype.quote = quote;

exports.Database = Database;
exports.utils =
{
	formatDate: formatDate
	,formatTime: formatTime
	,formatDateTime: formatDateTime
	,escapeString: escapeString
	,quote: quote
	,quoteOrNull: quoteOrNull
	,begins_with: begins_with
	,contains: contains
	,ifNull: ifNull
	,intOrNull: intOrNull
	,buildInsertSQL: buildInsertSQL
	,checkBoxVal: checkBoxVal
};