const MongoClient = require('mongodb').MongoClient;
var amqp = require('amqplib');

var dbUrl = 'mongodb://localhost:27017';
var db_name ='VKnowLab';
var db;

MongoClient.connect(dbUrl +'/' + db_name,{useUnifiedTopology:true }, function(err, client ){
  db = client.db( db_name);
})

var Worker = function () {
    this.queue_name = 'db_connection_queue';
    this.rabbitMq = {};
    this.db = {};
};


Worker.prototype.connectRabbitMq = function() {

    return amqp.connect('amqp://localhost')
  
      .then(function (connection) {
        this.rabbitMq['connection'] = connection;
        return connection.createChannel()
      }.bind(this))
  
      .then(function(channel) {
        this.rabbitMq['channel'] = channel;
        return channel.assertQueue(this.queue_name, {durable: true});
      }.bind(this))

      .then(function(){
        this.rabbitMq.channel.prefetch(1); 
      }.bind(this))
};

Worker.prototype.executeQuery1 = function(msg){
  const collection = db.collection('queues');



  return collection.insertOne({msg : msg })
        .then(function(result){
          return result.ops[0]; 
        });
};

Worker.prototype.consume = function(){
    var rabbitMq = this.rabbitMq; 
    var ok = rabbitMq.channel.consume(this.queue_name, doWork, {noAck: false});
    return ok;


    function doWork(msg) {
        var body =  JSON.parse(msg.content.toString() );
        console.log(" [x] Received ", msg.content.toString()  );
        var query = body.query; 
        executeQuery[query](body)
        .then(function(result){
          console.log(' [x] Result : ' , result);
          setTimeout(function() {
            console.log('Done')
            rabbitMq.channel.ack(msg);
          }, 1000 )
        })
    }
};

var worker = new Worker();

worker.connectRabbitMq()
.then(function(){
    worker.consume();
})


var executeQuery = {
  
  find : function(msg){
    const collection = db.collection(msg.collection);
    var query = collection.find(msg.criteria).toArray() || collection.find(msg.criteria).sort(msg.sort).toArray() ;
    return query
    .then(function(results){
      return results
    });
  },

  findOne : function(msg){
    const collection = db.collection(msg.collection);
    return collection.findOne(msg.criteria)
    .then(function(result){
      return result
    });
  },
  
  insertOne :  function(msg){
    const collection = db.collection(msg.collection);
    return collection.insertOne(msg.criteria)
    .then(function(result){
      return result.ops[0]; 
    });
  },

  insertMany : function(msg){
    const collection = db.collection(msg.collection);
    return collection.insertMany(msg.criteria)
    .then(function(result){
      return result.ops 
    });
  }
}
module.exports = executeQuery; 