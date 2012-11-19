var pg = require('pg');

/*
 * constructor
 */

function DbModule (config) {
	this.HOST = config.host;
	this.USER = config.user;
	this.PASS = config.pass;
	this.DB_NAME = config.dbName;
	this.RESULTS_TABLE = config.resultsTable;
}

DbModule.prototype.CALCULATION_TYPES = {
	GEOMETRY_LENGTH: 	'sum(ST_Length',
	GEOMETRY_AREA: 		'sum(ST_Area',
  FEATURE_COUNT:    'count'
}

/*
 * 
 */
DbModule.prototype.getConnection = function () {
	var connection = new pg.Client('postgres://' + this.USER + ':' + this.PASS + '@' + this.HOST + ':5432/' + this.DB_NAME);
	connection.connect();
	
	return connection;
}

/*
 * 
 */
DbModule.prototype.getProcessedCells = function (features, callback) {
	var connection = this.getConnection();
	// var whereClause = [];
	// for (var i = 0; i < features.length; i++) {
	// 	whereClause.push('(' + features[i].name + ' IS NOT NULL)');
	// }
	// console.log('SELECT cell_id FROM ' + this.RESULTS_TABLE + ' WHERE ' + whereClause.join('OR'));
	connection.query(
		// 'SELECT cell_id FROM ' + this.RESULTS_TABLE + ' WHERE ' + whereClause.join('OR'),
		'SELECT cell_id FROM ' + this.RESULTS_TABLE,
		function (error, result) {
			if (error) {
				throw new Error ("An error occured while getting processed cell IDs:  \n " + JSON.stringify(error)); 
			} else {
				var cellIds = [];
				for (var i = 0; i < result.rows.length; i++) {
					cellIds.push(result.rows[i].cell_id);
				}
				callback(cellIds);
			}
		}
	);
}

/*
 * 
 */
DbModule.prototype.getNumberOfCells = function (callback) {
	var connection = this.getConnection();
	
	connection.query(
		'SELECT count(*) osm_redaction_before;',
		function (error, result) {
			if (error) {
				throw new Error ("An error occured while getting number of cells:  \n " + JSON.stringify(error)); 
			} else {
				callback(result.rows[0].count);
			}
		}
	);
}

/*
 * 
 */
DbModule.prototype.getNextCell = function (excludes, callback) {
	var connection = this.getConnection();
	var query = 'SELECT id, ST_AsText(ST_AsText(geom)) as geom FROM cells_bw LIMIT 1;'
	if (excludes.length > 0) query = query.split('LIMIT').join('WHERE id NOT in (' + excludes.join(', ') + ') LIMIT');
	
	connection.query(
		query,
		function (error, result) {
			connection.end();

			if (error) throw new Error ("An error occured while getting cell geometry:  \n " + JSON.stringify(error.detail)); 
			else callback(result);
		}
	);
}

/*
 * 
 */
DbModule.prototype.processCell = function (cellId, cellGeom, characteristics, callback) {
	var self = this;
	var results = {};
	var pending = 0;
	var connection = this.getConnection();
	var queries = [];

	for (var i = 0; i < characteristics.length; i++) {
		pending++;
		var currentCharacteristics = characteristics[i];
		console.log(this.getFeatureSelectStatement(currentCharacteristics, cellGeom));
		queries[i] = connection.query(this.getFeatureSelectStatement(currentCharacteristics, cellGeom));

		queries[i].on('error', function (error) {
			connection.end();
			throw new Error ("An error occured while calculating characteristics:  \n " + JSON.stringify(error));
		});

		queries[i].on('end', function () {
			console.log('Pending: ' + pending);
			console.log(results);
			pending--;
			if (pending === 0) {
				console.log(results);
				self.insertValues(cellId, results);
				connection.end();
			}
		});

		queries[i].on('row', function (row) {
			for (var i = 0; i < currentCharacteristics.features.length; i++) {
				var feature = currentCharacteristics.features[i];
					if (row[feature.key] !== null && (!feature.values || feature.values.indexOf(row[feature.key]) !== -1)) {
						if (results[feature.name]) {results[feature.name] += row.value;}
						else {results[feature.name] = row.value;}
					};
			}
		});
	}
}

	/*
	 *
	 */
DbModule.prototype.insertValues = function (cellId, values) {
	var updateStatement = [];

	for (var key in values) {
		updateStatement.push(key + '=' + values[key]);
	}
	console.log('UPDATE ' + this.RESULTS_TABLE + ' SET ' + updateStatement.join(', ') + ' WHERE id = ' + cellId);
}


	// var updateStatement = 'INSERT INTO ' + this.RESULTS_TABLE +'(cell_id, ';
	// var fields = [];
	// var fieldRequests = [];
	// for (var i = 0; i < features.length; i++) {
	// 	fields.push(features[i].name);
	// 	fieldRequests.push(this.getFeatureSelectStatement(features[i], cellGeom));
	// }

	// var connection = this.getConnection();
	// connection.query(
	// 	updateStatement + fields.join(', ') + ') VALUES (' + cellId + ', ' + fieldRequests.join(', ') + ');',
	// 	function (error, result) {
	// 		connection.end();
	// 		if (error) throw new Error ("An error occured while inserting processed values:  \n " + JSON.stringify(error)); 
	// 		else callback(cellId);
	// 	}
	// );

/*
 *
 */

DbModule.prototype.getFeatureSelectStatement = function (characteristics, cellGeom) {
	var fields = [];
	var whereClause = [];

	for (var i = 0; i < characteristics.features.length; i++) {
		var feature = characteristics.features[i];
		if (fields.indexOf(feature.key) === -1) fields.push(feature.key);
		whereClause.push('(' + feature.key + ' ' + ((feature.values) ? 'IN (\'' + feature.values.join('\', \'') + '\')' : 'IS NOT NULL') + ')');
	}

	var statement = [];

	statement.push('SELECT ');
	statement.push(fields.join(', '));
	statement.push(', ' + characteristics.calculationType + '(ST_Intersection(way, GeometryFromText(\'' + cellGeom + '\', 900913))) as value');
	statement.push(' FROM ' + characteristics.sourceTable);
	statement.push(' WHERE ST_isvalid(way)=\'t\' AND ST_Intersects(way, GeometryFromText(\'' + cellGeom + '\', 900913)) AND (');
	statement.push(whereClause.join(' OR '));
	statement.push(');');

	return statement.join('');
}

/*
 * Export the module
 */ 
module.exports = DbModule;