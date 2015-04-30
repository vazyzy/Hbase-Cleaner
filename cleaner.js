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
        var start_del_time = Date.now();
        console.log('\nscan:', cells.length);
        console.log('start:', new Date(start_del_time).toString());
        deleteRows(cells, function(deleted) {
          console.log('\ndelete:', deleted);
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
 var i = 0;
  function del(cell){
    client.getRow(tableName, cell['key']).delete(function(err, a) {
      if (err) {
        console.log(err);
      } else {
        count++
      }
      i++;
      if (i < cells.length){
        del(cells[i]);
      } else {
        complete(count);
      }
    })
  }

  del(cells[i])
}