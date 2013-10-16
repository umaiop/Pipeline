/*
 Export MySQL data to CSV file

 dep:
    mysql - https://github.com/felixge/node-mysql
    underscore - http://underscorejs.org/
    moment - http://momentjs.com/
    when - https://github.com/cujojs/when
    optimist - https://github.com/substack/node-optimist
 */
var fs       = require('fs');
var path     = require('path');
// dep
var _        = require('underscore');
var when     = require("when");
var moment   = require('moment');
var mysql    = require('mysql');

var today    = new Date();
var dtFormat_YMD = "YYYY-MM-DD"; // year month day format, ex. "2013-10-03"
var dtFormat_HMS = "hh:mm:ss";   // hour min sec format, ex. "2013-10-03"

var argv = require('optimist')
    .usage('Usage: $0 --rowsPerWrite [num] --multiFileOutput --startDate [date] --endDate [date] --exportDir [dir path]')
    .boolean('multiFileOutput')
    .default('rowsPerWrite', 50000)
    .default('multiFileOutput', true)
    .default('startDate', moment().format(dtFormat_YMD))
    .default('exportDir', 'export')
    .default('db_host',     '127.0.0.1')
    .default('db_port',     3306)
    .default('db_user',     'glasslab')
    .default('db_password', 'glasslab')
    .default('db_database', 'glasslab_prod')
    .alias('m', 'multiFileOutput')
    .alias('r', 'rowsPerWrite')
    .alias('s', 'startDate')
    .alias('e', 'endDate')
    .alias('x', 'exportDir')
    .alias('h', 'db_host')
    .alias('P', 'db_port')
    .alias('u', 'db_user')
    .alias('p', 'db_password')
    .alias('d', 'db_database')
    .describe('m', 'Multi File Output')
    .describe('r', 'Rows Per Write (if multiFileOutput enabled, each file will have this many rows)')
    .describe('s', 'Start Date')
    .describe('e', 'End Date')
    .describe('x', 'Export Dir')
    .describe('h', 'MySQL DB Host')
    .describe('P', 'MySQL DB Port')
    .describe('u', 'MySQL DB User')
    .describe('p', 'MySQL DB Password')
    .describe('d', 'MySQL DB Database')
    .argv;

// copy args on top of settings
var settings = { db:{} };
for(var a in argv) {
    if(a != "$0" && a != "_") {
        // convert db_* to db object
        if(a.indexOf("db_") == 0) {
            settings["db"][a.substring(3)] = argv[a];
        } else {
            settings[a] = argv[a];
        }
    }
}

// if endDate does not exist add one day to start date
if(!settings.hasOwnProperty("endDate")) {
    var startDate =  moment( settings.startDate );
    // default add one day to startDate
    settings.endDate = startDate.add('days', 1).format(dtFormat_YMD);
}

// blocking check if dir exists, if not create dir
if(!fs.existsSync(settings.exportDir)) {
    fs.mkdirSync(settings.exportDir);
}

// chain exports
exportActivityEvents(settings).then( function(result){
    console.log("Done Exporting Data to CSV!");
    // all done exit
    process.exit();
});

// Export Activity Events
function exportActivityEvents(settings) {
    var deferred = when.defer();

    exportToCsv(settings,
        deferred,
        "gl_activity_events",
        "SELECT ae.last_updated,    \n\
            ae.USER_ID,             \n\
            ae.GAME_SESSION_ID,     \n\
            ae.NAME,                \n\
            ar.ACTIVITY_ID,         \n\
            ar.course_id,           \n\
            ae.DATA                 \n\
        FROM GL_ACTIVITY_EVENTS_ARCHIVE ae,     \n\
             GL_ACTIVITY_RESULTS ar             \n\
        WHERE   ae.last_updated > '"+settings.startDate+" 00:00:00'  \n\
            AND ae.last_updated < '"+settings.endDate+" 00:00:00'    \n\
            AND ar.game_session_id = ae.game_session_id"
    );

    return deferred.promise;
}

function exportToCsv(settings, deferred, name, query) {
    var outDataString = "";
    var rowHeader     = [];
    var rowCount      = 0;
    var totalRowCount = 0;
    var fileNum       = 1;

    var date          = moment(settings.startDate);
    var filename      = name + "_" + date.format(dtFormat_YMD) + ".csv";
    var fullFilename  = settings.exportDir + path.sep + filename;
    if(!settings.multiFileOutput) {
        // create new/empty file [BLOCKING!]
        fs.writeFileSync(fullFilename, '');
    }

    console.log("Connected to Server...");
    console.log("query:", query);

    var connection = mysql.createConnection(settings.db);
    connection.connect();

    console.log("Running Query '"+name+"'...");
    var query = connection.query(query);

    query.on('error', function(err) {
        throw err;
    });

    query.on('fields', function(fields) {
        // add header
        for(var f = 0; f < fields.length; ++f) {
            rowHeader.push( fields[f].name );
        }
    });

    query.on('result', function(row) {
        // pause DB connection
        connection.pause();

        // write header
        if(rowCount == 0) {
            // write header
            var out = [];
            for(var f = 0; f < rowHeader.length; ++f) {
                out.push( rowHeader[f] );
            }
            outDataString += out.join(",")+"\n";
        }

        // build row from fields to get order
        var out = []
        for(var f = 0; f < rowHeader.length; ++f) {
            val = row[ rowHeader[f] ];

            // convert date to string
            if(_.isDate(val) ){
                val = moment(val).format(dtFormat_YMD+" "+dtFormat_HMS);
            }

            if(_.isString(val) ){
                // escape "
                //val = val.replace(/\"/g, '\\"');

                out.push( "'"+val+"'" );
            } else {
                out.push( val );
            }
        }

        outDataString += out.join(",")+"\n";
        rowCount++;
        totalRowCount++;

        if(rowCount % settings.rowsPerWrite == 0) {
            writeOutData();

            // safty catch in case rowsPerWrite is too low
            if(fileNum > 1000) {
                console.log("Too many Files!!!");
                process.exit();
            }

            // reset count for header
            if(settings.multiFileOutput) {
                rowCount = 0;
            }
            fileNum++;
            outDataString = "";
        }

        connection.resume();
    });

    query.on('end', function() {
        writeOutData();
        console.log("Total Rows %d", totalRowCount);

        deferred.resolve("all good");
    });

    function writeOutData() {
        if(settings.multiFileOutput) {
            // multi file output creates new file every data chunk, determined by rowsPerWrite
            filename = name+"_" + date.format(dtFormat_YMD) + "_" + fileNum + ".csv";
            fullFilename = settings.exportDir + path.sep + filename;

            // create new/empty file [BLOCKING!]
            fs.writeFileSync(fullFilename, '');
        }

        // write chunk to file [BLOCKING!]
        fs.appendFileSync(fullFilename, outDataString);
        console.log("Wrote %d Rows to file %s", rowCount, filename);
    }

    connection.end();
}


