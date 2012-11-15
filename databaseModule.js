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
DbModule.prototype.getProcessedCells = function (callback) {
	var connection = this.getConnection();
	
	connection.query(
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
	var updateStatement = 'INSERT INTO ' + this.RESULTS_TABLE +'(cell_id, ';
	var fields = [];
	var fieldRequests = [];
	for (var i = 0; i < features.length; i++) {
		fields.push(features[i].name);
		fieldRequests.push(this.getFeatureSelectStatement(features[i], cellGeom));
	}

	var connection = this.getConnection();
	connection.query(
		updateStatement + fields.join(', ') + ') VALUES (' + cellId + ', ' + fieldRequests.join(', ') + ');',
		function (error, result) {
			connection.end();
			if (error) throw new Error ("An error occured while inserting processed values:  \n " + JSON.stringify(error)); 
			else callback(cellId);
		}
	);
} 

/*
 * 
 */
DbModule.prototype.getFeatureSelectStatement = function (feature, cellGeom) {
	var whereClause = (feature.values) ? ' IN (\'' + feature.values.join('\', \'') + '\')' : ' IS NOT NULL'

	var statement = [];
	statement.push('(SELECT sum(');
	statement.push(feature.calculationType + '(');
	statement.push('ST_Intersection(way, GeometryFromText(\'' + cellGeom + '\', 900913))');
	statement.push('))');
	statement.push(' FROM ' + feature.sourceTable);
	statement.push(' WHERE ' + feature.key + whereClause);
	statement.push(' AND ST_isvalid(way)=\'t\' AND ST_Intersects(way, GeometryFromText(\'' + cellGeom + '\', 900913))');
	statement.push(')');
	return statement.join('');
}

/*
 * Export the module
 */ 
module.exports = DbModule;