# spark-http-stream

spark-http-stream enables transfer Spark DataFrame over HTTP protocol. Unlike tcp streams, Kafka streams and HDFS file streams, http streams flow across distributed data center.

spark-http-stream provides:
* HttpStreamServer: a HTTP server which receives, collects and returns http streams 
* HttpStreamSource: reads messages from HttpStreamServer, acts as a Source
* HttpStreamSink: sends messages to a HttpStreamServer using POST command, acts as a Sink

also it provides:
* HttpStreamClient: a client used to communicate with HttpStreamServer, developped upon HttpClient
* HttpStreamSourceProvider: a StreamSourceProvider which creates HttpStreamSource
* HttpStreamSinkProvider: a StreamSinkProvider which creates HttpStreamSink

# HttpStreamSource, HttpStreamSink

The following code loads messages from a HttpStreamSource:

	val lines = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1");
		.option("includesTimestamp", "true")
		.load();
		
options:
* httpServletUrl: path to the servlet
* topic: the topic name of messages which you want to consume
* includesTimestamp: if each row in the loaded DataFrame includes a time stamp or not, default value is false
* timestampColumnName: name assigned to the time stamp column, default value is '_TIMESTAMP_'
* msFetchPeriod: time interval in milliseconds for message buffer check, default value is 1(1ms)

The following code outputs messages to a HttpStreamSink:

	val query = lines.writeStream
		.format(classOf[HttpStreamSinkProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1")
		.start();
		
options:
* httpServletUrl: path to the servlet
* topic: the topic name of messages which you want to produce
* maxPacketSize: max size in bytes of each message packet, if the actual DataFrame is too large, it will be splitted into serveral packets, default value is 10*1024*1024(10M)

# starts a standalone HttpStreamServer
HttpStreamServer is actually a Jetty server, it can be started using following code:

	val server = HttpStreamServer.start("/xxxx", 8080);
    
when you request 'http://localhost:8080/xxxx', the HttpStreamServer will use an ActionsHandler to 
parse your request message, perform certain action('fecthSchema', 'fetchStream', etc), and return response message.

by default, an NullActionsHandler is provided to the HttpStreamServer. It can be replaced with a MemoryBufferAsReceiver:

	server.withBuffer()
		.addListener(new ObjectArrayPrinter())
		.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1")
		.createTopic[String]("topic-2");
      
or with a KafkaAsReceiver:

	server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
		.addListener(new ObjectArrayPrinter());

