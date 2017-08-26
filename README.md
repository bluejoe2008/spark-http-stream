# spark-http-stream

spark-http-stream provides:
* HttpStreamServer: a HTTP server which receives, collects and returns http streams 
* HttpStreamSource: reads messages from HttpStreamServer, acts as a Source
* HttpStreamSink: sends messages to a HttpStreamServer using POST command, acts as a Sink

also it provides:
* HttpStreamClient: a client used to communicate with HttpStreamServer, developped upon HttpClient
* HttpStreamSourceProvider: a StreamSourceProvider which creates HttpStreamSource
* HttpStreamSinkProvider: a StreamSinkProvider which creates HttpStreamSink

# HttpStreamServer
Following code starts a HttpStreamServer on port 8080:

	val server = HttpStreamServer.start("/xxxx", 8080);
    
when you request 'http://localhost:8080/xxxx' with contents, the HttpStreamServer will use a ActionsHandler to 
parse your request contents, perform certain action, and return response.

by default, an NullActionsHandler is provided to the HttpStreamServer. It can be repalced with a MemoryBufferAsReceiver:

  server.withBuffer()
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1")
			.createTopic[String]("topic-2");
      
or with a KafkaAsReceiver:

		server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
			.addListener(new ObjectArrayPrinter());


# HttpStreamSource, HttpStreamSink

	val lines = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1");
		.option("includesTimestamp", "true")
		.load();


