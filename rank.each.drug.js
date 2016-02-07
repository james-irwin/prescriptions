var fs = require('fs');
var http = require('http');
var bnfpathname = '/.../bnf.to.name.json';
var bnfnames = JSON.parse(fs.readFileSync(bnfpathname));

var name_stack=[];
name_stack.push("BOTTOM");

function gather_top_of_stack(){
  var tos = name_stack.pop();
  if (tos == "BOTTOM") return;

  gather(tos);
}

function gather(bnfcode){
  // Gather the top ten places that writes this bnfcode
  var p={};
  var equery = '/_search?q=bnf:' + bnfcode +
              '&pretty=true&size=0';

  var edata =  '{"aggs": { "by_practice": { "terms": {"field": "practice", "order" : {"total_quantity":"desc" }}, "aggs": {"total_quantity": {"sum": {"field": "quantity"}}}}}}'

  // Now POST with this URL and data.

  var options = {
    hostname: 'localhost',
    port: 9200,
    path: equery,
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': edata.length
    }
  };

  var aggsresult = "";
  var req = http.request(options, function(res) {
    res.setEncoding('utf8');
    res.on('data', function (chunk) {
      aggsresult += chunk;
    });
    res.on('end', function() {
      var topwriters = JSON.parse(aggsresult).aggregations.by_practice.buckets;
      list=[];
      for (var script = 0; script < topwriters.length; script++){
        listitem=topwriters[script].key.toUpperCase();
        list.push(listitem);
      }
      p.list = list;
      if (list.length>0)
        dump(bnfcode, p);
      gather_top_of_stack();
    })
  });

  req.on('error', function(e) {
    console.log('problem with request: ' + e.message);
  });

  // write data to request body
  req.write(edata);
  req.end();
}

function dump(bnfcode, extras){
  console.log('"' + bnfcode + '": ' + JSON.stringify(extras) + ',');
}

for (code in bnfnames) {
  name_stack.push(code);
}

gather_top_of_stack();
