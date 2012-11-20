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
var numberOfCells = 0;
var startDate = new Date().getTime();

// instanciate modules
var db;

var databaseConfig = {
	host: null,
	user: null,
	pass: null,
	dbName: null,
	resultsTable: null
};

var CALCULATION_TYPES = {
	GEOMETRY_LENGTH: 	'ST_Length',
	GEOMETRY_AREA: 		'ST_Area',
  FEATURE_COUNT:    'count'
}

var SOURCE_TABLES = {
	OSM_NODE: 		'planet_osm_point',
	OSM_LINE: 		'planet_osm_line',
	OSM_POLYGON: 	'planet_osm_polygon',
	OSM_ROADS: 		'planet_osm_roads'
}

var characteristics = [
 	{
    calculationType: CALCULATION_TYPES.GEOMETRY_LENGTH,
    sourceTable: SOURCE_TABLES.OSM_LINE,
    features : [
      {
        name: 'length_major_highways',
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
        key: 'waterway'
      },
      {
        name: 'length_railways',
        key: 'railway'
      },
      {
        name: 'length_boundary',
        key: 'boundary'
      }
    ]	
 	}, {
    calculationType: CALCULATION_TYPES.GEOMETRY_AREA,
    sourceTable: SOURCE_TABLES.OSM_POLYGON,
    features: [
      {
        name: 'area_landuse',
        key: 'landuse'
      },
      {
        name: 'area_buildings',
        key: 'building'
      },
      {
        name: 'area_amenity',
        key: 'amenity'
      },
      {
        name: 'area_leisure',
        key: 'leisure'
      },{
        name: 'area_natural',
        key: '"natural"'
      }
    ]
  }
];

/*
 * handleNextCell
 */
function handleNextCell(result) {
	if (result.rows.length > 0) {
		process.stdout.clearLine();
	  process.stdout.cursorTo(0);
		process.stdout.write('Processing cell #' + result.rows[0].id + '. ' + ((cellsProcessed.length/numberOfCells) * 100).toFixed(3) + '% finished.');
		db.processCell(result.rows[0].id, result.rows[0].geom, characteristics, handleCellProcessed);	
	} else {
		console.log('\nProcessing done. Took ' + (new Date().getTime() - startDate) + ' ms');
    db.closeConnection();
    process.exit();
	}
}

function handleCellProcessed(id) {
	cellsProcessed.push(id);
	db.getNextCell(cellsProcessed, handleNextCell);
}

function handleProcessedCellsResult(cellIds) {
	cellsProcessed = cellIds;
	db.getNextCell(cellsProcessed, handleNextCell);
}

function handleCellsCountResult(count) {
  numberOfCells = count;
  db.getProcessedCells(characteristics, handleProcessedCellsResult);
}

function initProcess () {
  // initialize database connector
  db = new database(databaseConfig);

  // run the process
  db.getNumberOfCells(handleCellsCountResult);
}


function getPassword() {
  process.stdout.write('Enter password for user ' + databaseConfig.user + ' on database ' + databaseConfig.dbName +': ');
  process.stdin.resume();
  process.stdin.setEncoding('utf8');
  process.stdin.setRawMode(true);
  password = ''
  process.stdin.on('data', function (char) {
    char = char + ""

    switch (char) {
      case "\n": case "\r": case "\u0004":
        // They've finished typing their password
        process.stdin.setRawMode(false);
        console.log('\n\n');
        databaseConfig.pass = password;
        initProcess();
        stdin.pause();
        break;
      case "\u0003":
        // Ctrl C
        console.log('Cancelled');
        process.exit();
        break;
      default:
        // More passsword characters
        process.stdout.write('');
        password += char;
        break;
    }
  });
}


/*
 * Run the process
 */

var args = process.argv;
var argsObj = {};

// parsing arguments list into database config
//start index from 2. [0]=node, [1]=scriptpath
for (var i = 2;i < args.length; i++){
   //remove all leading "-" minuses
   args[i] = args[i].replace(/^-+/,"");
   if (args[i].indexOf("=") != -1){
       var argParts = args[i].split("=");
       argsObj[argParts[0]] = argParts[1];
   }
   else{
       argsObj[args[i]] = true;
  }
};

// assigning dbconfig information
databaseConfig.user = argsObj.u;
databaseConfig.host = argsObj.h;
databaseConfig.dbName = argsObj.d;
databaseConfig.resultsTable = argsObj.r;

var stdin = process.openStdin()
     , tty = require('tty');

// Get a db-user password from the console
// starts the process afterwards by calling initProcess;
getPassword();