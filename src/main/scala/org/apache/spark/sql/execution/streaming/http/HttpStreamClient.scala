package org.apache.spark.sql.execution.streaming.http

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.spark.serializer.KryoSerializer
import javax.net.ssl.SSLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import javax.annotation.concurrent.NotThreadSafe
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import java.io.InputStream

object HttpStreamClient {
	def connect(httpServletUrl: String) =
		new HttpStreamClient(httpServletUrl);
}

/**
 * a client used to communicate with [[HttpStreamServer]]
 * import org.apache.spark.sql.execution.streaming.http.RowExicate with HttpStreamServer
 */
private[streaming] class HttpStreamClient(httpServletUrl: String) extends Logging {

	val sslsf = new SSLConnectionSocketFactory(SSLContext.getDefault());
	val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory]()
		.register("https", sslsf)
		.register("http", new PlainConnectionSocketFactory())
		.build();

	val connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
	connectionManager.setMaxTotal(200);
	connectionManager.setDefaultMaxPerRoute(20);

	val serializer = SerializerFactory.DEFAULT.getSerializerInstance("kryo");

	/**
	 * retrieves a HttpClient object
	 */
	private def getClient = {
		val client = HttpClients.custom()
			.setConnectionManager(connectionManager)
			.build();

		client;
	}

	/**
	 * send a dataframe to server
	 * if the packet is too large, it will be split as several smaller ones
	 */
	def sendDataFrame(topic: String, batchId: Long, dataFrame: DataFrame, maxPacketSize: Int = 10 * 1024 * 1024): Int = {
		//performed on the driver node instead of worker nodes, so use local iterator
		val iter = dataFrame.toLocalIterator;
		val buffer = ArrayBuffer[Row]();
		var rows = 0;

		def flush() = {
			rows += sendRows(topic, batchId, buffer.toArray);
			buffer.clear();
		}

		while (iter.hasNext) {
			buffer += iter.next();
			val objects = buffer.toArray;
			val bytes = serializer.serialize(objects).array();
			if (bytes.length >= maxPacketSize) {
				flush();
			}
		}

		if (!buffer.isEmpty) {
			flush();
		}

		rows;
	}

	//sends requestBody to server, and parses responseBody as a Map
	//kryoSerializer is used
	private def executeRequest(requestBody: Map[String, Any]): Map[String, Any] = {
		super.logDebug(s"request: $requestBody");

		val post = new HttpPost(httpServletUrl);
		val bytes = serializer.serialize(requestBody).array();
		val builder = EntityBuilder.create()
			.setBinary(bytes)
			.setContentType(ContentType.APPLICATION_OCTET_STREAM);

		val entity = builder.build();
		post.setEntity(entity);
		val client = getClient;
		val resp = client.execute(post);

		//server side exception
		if (resp.getStatusLine.getStatusCode != 200) {
			throw new HttpStreamServerSideException(resp.getStatusLine.getReasonPhrase);
		}

		val is = resp.getEntity.getContent;
		val responseBody: Map[String, Any] = serializer.deserializeStream(is).readObject();
		is.close();
		super.logDebug(s"response: $responseBody");
		responseBody;
	}

	def fetchSchema(topic: String): StructType = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionFetchSchema",
			"topic" -> topic));

		res("schema").asInstanceOf[StructType];
	}

	def sendRows(topic: String, batchId: Long, rows: Array[Row]) = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionSendStream",
			"topic" -> topic,
			"batchId" -> batchId,
			"rows" -> rows));

		res("rowsCount").asInstanceOf[Int];
	}

	//subscribe a topic
	//returns subscriberId
	def subscribe(topic: String): (String, StructType) = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionSubscribe",
			"topic" -> topic));

		(res("subscriberId").asInstanceOf[String], res("schema").asInstanceOf[StructType]);
	}

	def unsubscribe(subscriberId: String): String = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionUnsubscribe",
			"subscriberId" -> subscriberId));

		res("subscriberId").asInstanceOf[String];
	}

	def fetchStream[T: ClassTag](subscriberId: String): Array[RowEx] = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionFetchStream",
			"subscriberId" -> subscriberId));

		res("rows").asInstanceOf[Array[RowEx]];
	}
}

class HttpStreamServerSideException(msg: String) extends RuntimeException(msg) {
}