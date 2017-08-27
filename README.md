# spark-http-stream

spark-http-stream transfers Spark structured stream over HTTP protocol. Unlike tcp streams, Kafka streams and HDFS file streams, http streams often flow across distributed big data clusters on the Web. This feature is very helpful to build global data processing pipeline across serveral data centers (scientific research institues, for example) who own seperated data sets.

spark-http-stream provides:
* `HttpStreamServer`: a HTTP server which receives, collects and returns http streams 
* `HttpStreamSource`: reads messages from a `HttpStreamServer`, acts as a structured streaming Source
* `HttpStreamSink`: sends messages to a `HttpStreamServer` using HTTP-POST commands, acts as a structured streaming Sink

also it provides:
* `HttpStreamClient`: a client used to communicate with a `HttpStreamServer`, developped upon HttpClient
* `HttpStreamSourceProvider`: a StreamSourceProvider which creates `HttpStreamSource`
* `HttpStreamSinkProvider`: a StreamSinkProvider which creates `HttpStreamSink`

# HttpStreamSource, HttpStreamSink

The following code loads messages from a `HttpStreamSource`:

	val lines = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1");
		.option("includesTimestamp", "true")
		.load();
		
options:
* `httpServletUrl`: path to the servlet
* `topic`: the topic name of messages which you want to consume
* `includesTimestamp`: if each row in the loaded DataFrame includes a time stamp or not, default value is false
* `timestampColumnName`: name assigned to the time stamp column, default value is '\_TIMESTAMP\_'
* `msFetchPeriod`: time interval in milliseconds for message buffer check, default value is 1(1ms)

The following code outputs messages to a `HttpStreamSink`:

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
`HttpStreamServer` is actually a Jetty server, it can be started using following code:

	val server = HttpStreamServer.start("/xxxx", 8080);
    
when you request `http://localhost:8080/xxxx`, the HttpStreamServer will use an ActionsHandler to 
parse your request message, perform certain action(`fecthSchema`, `fetchStream`, etc), and return response message.

by default, an `NullActionsHandler` is provided to the HttpStreamServer. It can be replaced with a `MemoryBufferAsReceiver`:

	server.withBuffer()
		.addListener(new ObjectArrayPrinter())
		.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1")
		.createTopic[String]("topic-2");
      
or with a `KafkaAsReceiver`:

	server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
		.addListener(new ObjectArrayPrinter());

# understanding ActionsHandler

as shown previous section, serveral kinds of `ActionsHandler` are defined in spark-http-stream:
* `NullActionsHandler`: does nothing
* `MemoryBufferAsReceiver`: maintains a local memory buffer, stores data sent from producers into buffer, and allows consumers fetch data in batch
* `KafkaAsReceiver`: forwards all received data to kafka

users can customize your own `ActionsHandler` as you will. The interface is defined like:

	trait ActionsHandler {
		def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries;
		def destroy();
	}
	
here `ActionHandlerEntries` is just an alias of PartialFunction[String, Map[String, Any]], which accepts an input argument `action: String`, and returns an output argument `responseBody: Map[String, Any]`. the `listActionHandlerEntries` method is often written like a set of `case` expression:

	override def listActionHandlerEntries(requestBody: Map[String, Any])
		: PartialFunction[String, Map[String, Any]] = {
		case "actionSendStream" ⇒ handleSendStream(requestBody);
	}

the code above says: this `ActionsHandler` only handles action `actionSendStream`, in this case, it calls `handleSendStream(requestBody)` to handle request and output its return value as response. If other action is requested, an `UnsupportedActionException` will be thrown by the HttpStreamServer. 

`ActionsHandlerFactory` is defined to tell how to create a ActionsHandler with required parameters:

	trait ActionsHandlerFactory {
		def createInstance(params: Params): ActionsHandler;
	}

# embed HttpStreamServer in Web application servers

spark-http-stream provides a servlet named `ConfigurableHttpStreamingServlet`, users can configure the servlet in web.xml:

	<servlet>
		<servlet-name>httpStreamServlet</servlet-name>
		<servlet-class>org.apache.spark.sql.execution.streaming.http.ConfigurableHttpStreamServlet</servlet-class>
		<init-param>
			<param-name>handlerFactoryName</param-name>
			<param-value>org.apache.spark.sql.execution.streaming.http.KafkaAsReceiverFactory</param-value>
		</init-param>
		<init-param>
			<param-name>bootstrapServers</param-name>
			<param-value>vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092</param-value>
		</init-param>
	</servlet>

	<servlet-mapping>
		<servlet-name>httpStreamServlet</servlet-name>
		<url-pattern>/xxxx</url-pattern>
	</servlet-mapping>
	
in the example above, a servlet of `ConfigurableHttpStreamServlet` is defined with a ActionsHandlerFactory `KafkaAsReceiverFactory`, required parameters for the `ActionsHandlerFactory` (`bootstrapServers`, for example), are defined as `init-param`.

# using HttpStreamClient

HttpStreamClient` provides a HTTP client used to communicate with a `HttpStreamServer`. It contains serveral methods:
* `sendDataFrame`: send a `DataFrame` to the server, if the `DataFrame` is too large, it will be splitted into smaller packets
* `sendRows`: send data (as `Array[Row]`) to server
* `fetchSchema`: retrieves schema of certain topic
* `fecthStream`: retrieves data (as 'Array[RowEx]') from server
* `subscribe`: subscribe a topic and retrieves a subscriberId
* `unsubscribe`: unsubscribe

Note that some methods are only available when the server is equipped with correct `ActionsHandler`. As an example, the `KafkaAsReceiver` only handles action `actionSendStream`, that means, if you called `fecthStream` and `sendDataFrame` methods of the HttpStreamClient, it works well. But it will fail and throw an `UnsupportedActionException` when you called `subscribe` method.
