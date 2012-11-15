/******************************************************************************
 * Calculates length and area for selected OSM feature types. Requires an 
 * Osm2Pgsql complient data base.
 *
 * @author: Oliver Roick, Chair of GISscience, Heidelberg University
 * @version: 0.1
 * @date: Nov 15, 2012
 ******************************************************************************/

// Load the modules
var database = require('./databaseModule.js');

/*
 * Contains IDS of cells that have been processed alredy
 */
var cellsProcessed = [];



var CALCULATION_TYPES = {
	GEOMETRY_LENGTH: 	'ST_Length',
	GEOMETRY_AREA: 		'ST_area'
}

var SOURCE_TABLES = {
	OSM_NODE: 		'planet_osm_point',
	OSM_LINE: 		'planet_osm_line',
	OSM_POLYGON: 	'planet_osm_polygon',
	OSM_ROADS: 		'planet_osm_roads'
}

var features = [
 	// LENGTH FEATURES
 	{
 		name: 'length_major_highways',
 		calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
 		sourceTable: SOURCE_TABLES.OSM_ROADS,
 		key: 'highway',
 		values: [
 			'motorway',
 			'motorway_link',
 			'primary',
 			'primary_link',
 			'secondary',
 			'secondary_link',
 			'tertiary',
 			'tertiary_link',
 			'trunk',
 			'trunk_link'
 		]
 	},
 	{
 		name: 'length_minor_highways',
 		calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
 		sourceTable: SOURCE_TABLES.OSM_ROADS,
 		key: 'highway',
 		values: [
 			'living_street',
 			'pedestrian',
 			'residential',
 			'track',
 			'service'
 		]
 	},
 	{
 		name: 'length_waterways',
 		calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
 		sourceTable: SOURCE_TABLES.OSM_LINE,
 		key: 'waterway'
 	},
 	{
 		name: 'length_railways',
 		calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
 		sourceTable: SOURCE_TABLES.OSM_LINE,
 		key: 'railway'
 	},
 	{
 		name: 'length_boundary',
 		calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
 		sourceTable: SOURCE_TABLES.OSM_LINE,
 		key: 'boundary'
 	},
 	// AREA FEATURES
 	{
 		name: 'area_buildings',
 		calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
 		sourceTable: SOURCE_TABLES.OSM_POLYGON,
 		key: 'building'
 	},
 	{
 		name: 'area_landuse',
 		calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
 		sourceTable: SOURCE_TABLES.OSM_POLYGON,
 		key: 'landuse'
 	},
 	{
 		name: 'area_amenity',
 		calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
 		sourceTable: SOURCE_TABLES.OSM_POLYGON,
 		key: 'amenity'
 	},
 	{
 		name: 'area_leisure',
 		calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
 		sourceTable: SOURCE_TABLES.OSM_POLYGON,
 		key: 'leisure'
 	},{
 		name: 'area_natural',
 		calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
 		sourceTable: SOURCE_TABLES.OSM_POLYGON,
 		key: '"natural"'
 	}
 ];


// instanciate modules
var db = new database(databaseConfig);

/*
 * handleNextCell
 */
function handleNextCell(result) {
	console.log('#' + cellsProcessed.length + ': Processing cell ' + result.rows[0].id);
	db.processCell(result.rows[0].id, result.rows[0].geom, features, handleCellProcessed);
}

function handleCellProcessed(id) {
	cellsProcessed.push(id);
	db.getNextCell(cellsProcessed, handleNextCell);
}

function handleProcessedCellsResult(cellIds) {
	cellsProcessed = cellIds;
	db.getNextCell(cellsProcessed, handleNextCell);
}


/*
 * Run the process
 */
db.getProcessedCells(handleProcessedCellsResult);