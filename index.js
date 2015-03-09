var influx = require('influx');
var AWS = require('aws-sdk');

var glacier_client = new AWS.Glacier({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      region: process.env.REGION
    });

var influx_opts = {
      host : 'localhost',
      port : 8086,
      username : 'root',
      password : 'root',
      database : 'metrics'
    };

var influx_client = influx(influx_opts);

function archive(vault_name, last_time) {
  var archive_query = 'select * from /collectd.*/' + (last_time ? ' where time > ' + last_time + ' limit 100;' : ' limit 100');
  var delete_query = 'delete from /collectd.*/ where time < ' + last_time + ';';

  influx_client.query(archive_query, function(err, rows) {
    if(err) { throw err; }

    if(rows && rows[0] && rows[0].points && rows[0].points[0] && rows[0].points[0][0]) {
      glacier_client.uploadArchive({vaultName: vault_name, body: JSON.stringify(rows)}, function(err, data) {
        if(err) { throw err; }
        archive(value_name, rows[0].points[0][0] + 1);
      });
    } else {
      influx_client.query(delete_query, function(err, data) {
        if(err) { throw err; }
      });
    }
  });
}

var vault_name = (new Date()).getTime().toString(36);

glacier_client.createVault({vaultName: vault_name}, function(err) {
  if(err) { throw err; }
  archive(vault_name);
});

/*glacier_client.listVaults({}, function(err, data)  {
  var vault_name = (new Date()).getTime().toString(36);
  if(err) { throw err; }
  if(!data.VaultList.length) {
    glacier_client.createVault({vaultName: vault_name}, function(err) {
      if(err) { throw err; }
      archive(vault_name);
    });
  } else {
    vault_name = data.VaultList[0].VaultName;
    archive(vault_name);
  }
});
*/
