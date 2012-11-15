# OsmRedaction Analyser

## Requirements

- [Node.JS](http://nodejs.org/) v0.8.14+
- [node-postgres](https://github.com/brianc/node-postgres)
- A working [OSM2pgsql](http://wiki.openstreetmap.org/wiki/Osm2pgsql) instance

## Usage

1. Create the value table using createTable.sql

2. Run:

	    node index.js -u=yourUserName -h=dbHost -d=dbname -r=tablename

3. Enter password of database user.

4. Wait.