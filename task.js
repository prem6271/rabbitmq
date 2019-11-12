var amqp = require('amqplib');

amqp.connect('amqp://localhost').then(function(conn) {
  return conn.createChannel().then(function(ch) {
    var q = 'db_connection_queue';
    var ok = ch.assertQueue(q, {durable: true});
    return ok.then(function() {
      var msg = process.argv.slice(2)[0] ; 
      ch.sendToQueue(q, Buffer.from( JSON.stringify(JSON.parse(msg)) ), {deliveryMode: true});
      console.log(" [x] Sent '%s'", msg   );
      return ch.close();
    });
  })
  .finally(function() { conn.close(); });
})
.catch(console.warn);