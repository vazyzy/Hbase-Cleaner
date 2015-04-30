var hbase = require('hbase');
var conf = require('./conf.json');


var client = new hbase.Client({
  'host': conf['hbase_host'],
  'port': conf['port']
});

var tableName = 'visitor.contacts';
var params = conf['params'];

var scanner = client.getScanner(tableName);
scanner.create(params, findRows);


function findRows() {
  function handler(error, cells) {
    if (error) {
      console.log(error)
    } else {
      if (cells) {
        var l = cells.length;
        var start_del_time = Date.now();
        deleteRows(cells, function(deleted) {
          console.log('\nscan:', l);
          console.log('delete:', deleted);
          console.log('time:',  (Date.now() - start_del_time) + ' ms.');
          scanner.get(handler);
        });
      } else {
        console.log('finish for: ', params);
        scanner.delete();
      }
    }
  }

  scanner.get(handler);
}

function deleteRows(cells, complete) {
 var count = 0;
  function del(cell){
    client.getRow(tableName, cell['key']).delete(function(err, a) {
      if (err) {
        console.log(err);
      } else {
        count++
      }

      if (cells.length > 0){
        del(cells.shift());
      } else {
        complete(count);
      }
    })
  }

  del(cells.shift())
}