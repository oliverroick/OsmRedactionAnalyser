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
	var whereClause = [];
	for (var i = 0; i < features.length; i++) {
		whereClause.push('(' + features[i].name + ' IS NOT NULL)');
	}
	
	connection.query(
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
DbModule.prototype.processCell = function (cellId, cellGeom, features, callback) {
	var lengthFeatures = {};
	var fields = [];
	for (var i = 0; i < features.length; i++) {
		var feature = features[i];
		if (feature.calculationType == this.CALCULATION_TYPES.GEOMETRY_LENGTH) {
			if (lengthFeatures[feature.key]) {
				lengthFeatures[feature.key] = lengthFeatures[feature.key].concat(feature.values);
			} else {
				fields.push(feature.key);
				lengthFeatures[feature.key] = (feature.values) ? (feature.values) : true;
			}
		}
	}
	// TODO: Next up get results and evaluate
	console.log('SELECT ' + fields.join(', ') + ', ST_Intersection(way, GeometryFromText(\'' + cellGeom + '\', 900913)) FROM  planet_osm_line WHERE ' + this.getWhereClause(lengthFeatures, cellGeom, this.CALCULATION_TYPES.GEOMETRY_LENGTH));


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
} 

/*
 *
 */

DbModule.prototype.getWhereClause = function (features, cellGeom, calculationType) {
	var whereClause = [];
	for (var i in features) {
		var feature = features[i];
		whereClause.push('(' + i + ' ' + ((feature.length) ? 'IN (\'' + feature.join('\', \'') + '\')' : 'IS NOT NULL') + ')');
	}
	return 'ST_isvalid(way)=\'t\' AND ST_Intersects(way, GeometryFromText(\'' + cellGeom + '\', 900913)) AND (' + whereClause.join(' OR ') + ')';
}

/*
 * 
 */
// DbModule.prototype.getFeatureSelectStatement = function (feature, cellGeom) {
// 	var whereClause = (feature.values) ? ' IN (\'' + feature.values.join('\', \'') + '\')' : ' IS NOT NULL'

// 	var statement = [];
// 	statement.push('(SELECT ');
// 	statement.push(feature.calculationType + '(');
// 	statement.push((feature.calculationType == 'count') ? '*' :'ST_Intersection(way, GeometryFromText(\'' + cellGeom + '\', 900913))');
// 	statement.push((feature.calculationType.indexOf('(') != -1) ? '))': ')');
// 	statement.push(' FROM ' + feature.sourceTable);
// 	statement.push(' WHERE ' + feature.key + whereClause);
// 	statement.push(' AND ST_isvalid(way)=\'t\' AND ST_Intersects(way, GeometryFromText(\'' + cellGeom + '\', 900913))');
// 	statement.push(')');
// 	return statement.join('');
// }

/*
 * Export the module
 */ 
module.exports = DbModule;