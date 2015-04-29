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
        deleteRows(cells, function() {
          console.log('delete batch:', l);
          scanner.get(handler);
        });
      } else {
        console.log('finish for period: ', params);
        scanner.delete();
      }
    }
  }

  scanner.get(handler);
}

function deleteRows(cells, complete) {

  function del(cell){
    client.getRow(tableName, cell['key']).delete(function(err, a) {
      if (cells.length > 0){
        del(cells.shift());
      } else {
        complete();
      }
    })
  }

  del(cells.shift())
}