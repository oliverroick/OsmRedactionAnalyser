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

	this.connection = this.getConnection();
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
DbModule.prototype.getProcessedCells = function (characteristics, callback) {
	var whereClause = [];
	for (var i = 0; i < characteristics.length; i++) {
		var features = characteristics[i].features;
		for (var j = 0; j < features.length; j++) {
			whereClause.push('(' + features[j].name + ' IS NOT NULL)');
		}
	}

	this.connection.query(
		'SELECT cell_id FROM ' + this.RESULTS_TABLE + ' WHERE ' + whereClause.join('OR'),
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
	this.connection.query(
		'SELECT count(*) FROM osm_redaction_before;',
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
	var query = 'SELECT id, ST_AsText(ST_AsText(geom)) as geom FROM cells_bw LIMIT 1;'
	if (excludes.length > 0) query = query.split('LIMIT').join('WHERE id NOT in (' + excludes.join(', ') + ') LIMIT');
	
	this.connection.query(
		query,
		function (error, result) {

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

	for (var i = 0; i < characteristics.length; i++) {
		pending++;
		this.connection.query(
			this.getFeatureSelectStatement(characteristics[i], cellGeom),
			function (error, result) {
				if (error) {
					throw new Error ("An error occured while calculating characteristics:  \n " + JSON.stringify(error));
				} else {
					for (var j = 0; j < result.rows.length; j++) {
						var row = result.rows[j];
						var currentCharacteristics;

						for (var i = 0; i < characteristics.length; i++) {
							if (row.calctype == characteristics[i].calculationType) {
								currentCharacteristics = characteristics[i];
								// break;
							}
						}
						for (var k = 0; k < currentCharacteristics.features.length; k++) {
							var feature = currentCharacteristics.features[k];
							if (row[feature.key] !== null && (!feature.values || feature.values.indexOf(row[feature.key]) !== -1)) {
								if (results[feature.name]) {results[feature.name] += row.value;}
								else {results[feature.name] = row.value;}
							};
						}
					}
					pending--;
					
					if (pending === 0) {
						self.insertValues(cellId, results, callback);
					}
				}
			}
		);
	}
}

/*
 *
 */
DbModule.prototype.insertValues = function (cellId, values, callback) {
	var updateStatement = [];
	for (var key in values) {
		updateStatement.push(key + '=' + values[key]);
	}

	if (updateStatement.length > 0) {
		this.connection.query(
			'UPDATE ' + this.RESULTS_TABLE + ' SET ' + updateStatement.join(', ') + ' WHERE cell_id = ' + cellId,
			function (error, result) {
				if (error) throw new Error('Error while inserting values into database: \n' + error);
				else callback(cellId);
			}
		)	
	} else {
		callback(cellId);
	}
}

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
	statement.push(', \'' + characteristics.calculationType + '\' AS calcType');
	statement.push(' FROM ' + characteristics.sourceTable);
	statement.push(' WHERE ST_isvalid(way)=\'t\' AND ST_Intersects(way, GeometryFromText(\'' + cellGeom + '\', 900913)) AND (');
	statement.push(whereClause.join(' OR '));
	statement.push(');');

	return statement.join('');
}

DbModule.prototype.closeConnection = function () {
	this.connection.end();
}

/*
 * Export the module
 */ 
module.exports = DbModule;