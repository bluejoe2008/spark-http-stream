# spark-http-stream

spark-http-stream transfers Spark structured stream over HTTP protocol. Unlike tcp streams, Kafka streams and HDFS file streams, http streams often flow across distributed big data clusters on the Web. This feature is very helpful to build global data processing pipelines across different data centers (scientific research institues, for example) who own seperated data sets.

spark-http-stream provides:
* `HttpStreamServer`: a HTTP server which receives, collects and provides http streams 
* `HttpStreamSource`: reads messages from a `HttpStreamServer`, acts as a structured streaming `Source`
* `HttpStreamSink`: sends messages to a `HttpStreamServer` using HTTP-POST commands, acts as a structured streaming `Sink`

also spark-http-stream provides:
* `HttpStreamClient`: a client used to communicate with a `HttpStreamServer`, developped upon HttpClient
* `HttpStreamSourceProvider`: a StreamSourceProvider which creates `HttpStreamSource`
* `HttpStreamSinkProvider`: a StreamSinkProvider which creates `HttpStreamSink`

## Starts a standalone HttpStreamServer

`HttpStreamServer` is actually a Jetty server with a `HttpStreamServlet`, it can be started using following code:

	val server = HttpStreamServer.start("/xxxx", 8080);
    
When `http://localhost:8080/xxxx` is requested, the `HttpStreamServlet` will use an embeded `ActionsHandler` to 
parse request message, perform certain action(`fecthSchema`, `fetchStream`, etc), and return response message.

By default, an `NullActionsHandler` is provided. Of coz it can be replaced with a `MemoryBufferAsReceiver`:

	server.withBuffer()
		.addListener(new ObjectArrayPrinter())
		.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1")
		.createTopic[String]("topic-2");
      
or with a `KafkaAsReceiver`:

	server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
		.addListener(new ObjectArrayPrinter());

as shown above, serveral kinds of `ActionsHandler` are defined in spark-http-stream:

* `NullActionsHandler`: does nothing
* `MemoryBufferAsReceiver`: maintains a local memory buffer, stores data sent from producers into buffer, and allows consumers to fetch data in batch
* `KafkaAsReceiver`: forwards all received data to Kafka

Notes that MemoryBufferAsReceiver maintains a server-side message buffer, while KafkaAsReceiver only forwards messages to Kafka cluster.

## HttpStreamSource, HttpStreamSink

The following code shows how to load messages from a `HttpStreamSource`:

	val lines = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1");
		.option("includesTimestamp", "true")
		.load();
		
options:

* `httpServletUrl`: path to the servlet
* `topic`: topic name of messages to be consumed
* `includesTimestamp`: tells if each row in the loaded DataFrame includes a time stamp or not, default value is `false`
* `timestampColumnName`: name assigned to the time stamp column, default value is '\_TIMESTAMP\_'
* `msFetchPeriod`: time interval in milliseconds for message polling, default value is `1`(1ms)

The following code shows how to output messages to a `HttpStreamSink`:

	val query = lines.writeStream
		.format(classOf[HttpStreamSinkProvider].getName)
		.option("httpServletUrl", "http://localhost:8080/xxxx")
		.option("topic", "topic-1")
		.start();
		
options:

* httpServletUrl: path to the servlet
* topic: topic name of produced messages
* maxPacketSize: max size in bytes of each message packet, if the actual DataFrame is too large, it will be splitted into serveral packets, default value is `10*1024*1024`(10M)

Note that `HttpStreamSource` is only available when the `HttpStreamServer` is equiped with a  `MemoryBufferAsReceiver` (use `withBuffer`, as shown above). If the HttpStreamServer choose Kafka as back-end message system (use `withKafka`), it is wrong to consume data from `HttpStreamSource`, just use `KafkaSource` (see http://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) instead:

	val df = spark
		.readStream
		.format("kafka")
		.option("kafka.bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
		.option("subscribe", "topic-1")
		.load()

see https://github.com/bluejoe2008/spark-http-stream/blob/master/src/test/scala/HttpStreamSourceSinkTest.scala and https://github.com/bluejoe2008/spark-http-stream/blob/master/src/test/scala/HttpStreamKafkaTest.scala to get complete example code.

## Understanding ActionsHandler

as shown in previous section, serveral kinds of `ActionsHandler` are defined in spark-http-stream: `NullActionsHandler`, 
`MemoryBufferAsReceiver`, `KafkaAsReceiver`.

users can also customize their own `ActionsHandler` as they will. The interface looks like:

	trait ActionsHandler {
		def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries;
		def destroy();
	}
	
here `ActionHandlerEntries` is just an alias of `PartialFunction[String, Map[String, Any]]`, which accepts an input argument `action: String`, and returns an output argument `responseBody: Map[String, Any]`. the `listActionHandlerEntries` method is often written as a set of `case` expression:

	override def listActionHandlerEntries(requestBody: Map[String, Any])
		: PartialFunction[String, Map[String, Any]] = {
		case "actionSendStream" ⇒ handleSendStream(requestBody);
	}

the code shown above says: this `ActionsHandler` only handles action `actionSendStream`, in this case, it calls  the method `handleSendStream(requestBody)` to handle request and output its return value as response. If other action is requested, an `UnsupportedActionException` will be thrown by the HttpStreamServer. 

`ActionsHandlerFactory` is defined to tell how to create a ActionsHandler with required parameters:

	trait ActionsHandlerFactory {
		def createInstance(params: Params): ActionsHandler;
	}

## Embedding HttpStreamServer in Web application servers

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
	
As shown above, a servlet of `ConfigurableHttpStreamServlet` is defined with a ActionsHandlerFactory `KafkaAsReceiverFactory`, required parameters for the `ActionsHandlerFactory` (`bootstrapServers`, for example), are defined as a set of `init-param`s.

## Using HttpStreamClient

HttpStreamClient` provides a HTTP client used to communicate with a `HttpStreamServer`. It contains serveral methods:

* `sendDataFrame`: send a `DataFrame` to the server, if the `DataFrame` is too large, it will be splitted into smaller packets
* `sendRows`: send data (as `Array[Row]`) to server
* `fetchSchema`: retrieves schema of certain topic
* `fecthStream`: retrieves data (as 'Array[RowEx]') from server
* `subscribe`: subscribe a topic and retrieves a subscriberId
* `unsubscribe`: unsubscribe

Note that some methods are only available when the server is equipped with correct `ActionsHandler`. As an example, the `KafkaAsReceiver` only handles action `actionSendStream`, that means, if you called `fetchStream` and `sendDataFrame` methods of the HttpStreamClient, it works well. But it will fail and throw an `UnsupportedActionException` when you called `subscribe` method.

```
+---------------+------------------------+-----------------+
|  methods      | MemoryBufferAsReceiver | KafkaAsReceiver |
+---------------+------------------------+-----------------+
| sendDataFrame |             √          |        √        |
+---------------+------------------------+-----------------+
| sendRows      |             √          |        √        |
+---------------+------------------------+-----------------+
| fetchSchema   |             √          |        X        |
+---------------+------------------------+-----------------+
| fecthStream   |             √          |        X        |
+---------------+------------------------+-----------------+
| subscribe     |             √          |        X        |
+---------------+------------------------+-----------------+
| unsubscribe   |             √          |        X        |
+---------------+------------------------+-----------------+
```

## Tests

* `HttpStreamServerClientTest`: tests HttpStreamServer/Client
* `HttpStreamSourceSinkTest`: tests HttpStreamSource and HttpStreamSink
* `HttpStreamKafkaTest`: tests HttpStreamSink with Kafka as underlying message reveiver
* `HttpStreamDemo`: a tool helps to test HttpTextStream and HttpTextSink

steps to tests HttpStreamDemo:

1. choose machine A, run `HttpStreamDemo start-server-on 8080 /xxxx`, this starts a HTTP server which receives data from machine B
2. choose machine B, run `nc -lk 9999`
3. run `HttpStreamDemo read-from http://machine-a-host:8080/xxxx` on machine B
4. run `HttpStreamDemo write-into http://machine-a-host:8080/xxxx` on machine C
5. type some text in nc, data will be received by HttpStreamSink and then consumed as HttpStreamSource, finally displayed on console

## StreamListener

`StreamListener` works when new data is arrived and will be consumed by `ActionsHandler`:

	trait StreamListener {
		def onArrive(topic: String, objects: Array[RowEx]);
	}
	
Two kinds of `StreamListener`s are provided:

* `StreamCollector`: collects data in a local memory buffer
* `StreamPrinter`: prints data while arriving

an example messages look like this:

	++++++++topic=topic-1++++++++
	RowEx([hello1,1,true,0.1,0.1,1,49],1,0,2017-08-27 20:37:56.432)
	RowEx([hello2,2,false,0.2,0.2,2,50],1,1,2017-08-27 20:37:56.432)
	RowEx([hello3,3,true,0.3,0.3,3,51],1,2,2017-08-27 20:37:56.432)
	
## Schema, data types, RowEx

spark-http-stream only supports data types which can be recognized by Spark Encoders. These data types includes: `String`, `Boolean`, `Int`, `Long`, `Float`, `Double`, `Byte`, `Array[]`.

A row will be wrapped as a `RowEx` object on receiving. `RowEx` is a data structure richer than `Row`. It contains some members and methods:

* `originalRow`: original row
* `batchId`: batch id passed by Spark
* `offsetInBatch`: offset of this row in current batch
* `withTimestamp()`: returns a `Row` with a timestamp
* `withId()`: returns a `Row` with its id
* `extra()`: returns a triple (batchId, offsetInBatch, timestamp)

Considering an original row has values [hello1,1,true,0.1,0.1,1,49], following code show contents of mentioned structures:

* `RowEx`: ([hello1,1,true,0.1,0.1,1,49],1,0,2017-08-27 20:37:56.432)
* `originalRow`: [hello1,1,true,0.1,0.1,1,49]
* `batchId`: 1
* `offsetInBatch`: 0
* `withTimestamp()`: [hello1,1,true,0.1,0.1,1,49,2017-08-27 20:37:56.432]
* `withId()`: [hello1,1,true,0.1,0.1,1,49,1-0]
* `extra()`: (1,0,2017-08-27 20:37:56.432)

## SerDe

spark-http-stream defines a SerilizerFactory to create a SerializerInstance:

	trait SerializerFactory {
		def getSerializerInstance(serializerName: String): SerializerInstance;
	}
	
an `SerializerFactory.DEFAULT` object is provided which is able to create two kinds of serializers:

* `java`: creates a JavaSerializer
* `kryo`: creates a KryoSerializer

New kind of Serializer, `json` serializer, for example, is welcome. 

By default, `HttpStreamClient` and `HttpStreamServer` uses `kryo` serializer.

## dependencies

* `kafka-clients-0.10`: used by `KafkaAsReceiver`
* `httpclient-4.5`: HttpStreamClient uses HttpClient project
* `jetty-9.0`: HttpStreamServer is devploped upon Jetty
* `spark-2.1`: spark structued streaming libray
