var influx = require('influx');
var AWS = require('aws-sdk');

var glacier_client = new AWS.Glacier({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.REGION
});

/* Need root to delete rows */
var influx_client = influx({
  host : 'localhost',
  port : 8086,
  username : 'root',
  password : 'root',
  database : 'metrics'
});

var ROW_BATCH = 5000;
var ALL_BEFORE = (new Date()).getTime() - (60*60*24*7 * 1000);
var VAULT_NAME = ALL_BEFORE.toString(36);

glacier_client.createVault({vaultName: VAULT_NAME}, function(err) {
  if(err) { throw err; }
  getRows();
});

///////////////////////////////////////////////////////////////////

function getRows() {
  var get_query = 'select * from /collectd.*/ where time < ' + ALL_BEFORE + 'ms limit ' + ROW_BATCH + ';';

  influx_client.query(get_query, function(err, rows) {
    if(err) { throw err; }
    archive(rows);
  });
}

function archive(rows) {
  if(!rows || !rows.length) { return; }
  var furthest_batch_past = rows[0].points[rows[0].points.length-1][0];

  glacier_client.uploadArchive({vaultName: VAULT_NAME, body: JSON.stringify(rows)}, function(err) {
    if(err) { throw err; }
    deleteRows(furthest_batch_past);
  });
}

function deleteRows(after) {
  after -= 1;

  var delete_query = 'delete from /collectd.*/ where time < ' + ALL_BEFORE + 'ms and time > ' + after + 'ms;';
  influx_client.query(delete_query, function(err) {
    if(err) { throw err; }
    getRows();
  });
}
