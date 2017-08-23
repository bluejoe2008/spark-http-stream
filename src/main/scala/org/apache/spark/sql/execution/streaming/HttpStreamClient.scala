package org.apache.spark.sql.execution.streaming

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.http.HttpResponse
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import com.esotericsoftware.kryo.io.Input

import javax.net.ssl.SSLContext
import org.apache.spark.sql.types.StructType
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row
import java.io.InputStream
import java.sql.Timestamp

object HttpStreamClient {
	def connect(httpServletUrl: String, kryoSerializer: KryoSerializer) =
		new HttpStreamClient(httpServletUrl: String, kryoSerializer);
}

/**
 * a client used to send data to HTTP server
 * a connection pool is used
 */
private[streaming] class HttpStreamClient(httpServletUrl: String, kryoSerializer: KryoSerializer) {
	val sslsf = new SSLConnectionSocketFactory(SSLContext.getDefault());
	val socketFactoryRegistry = RegistryBuilder.create[ConnectionSocketFactory]()
		.register("https", sslsf)
		.register("http", new PlainConnectionSocketFactory())
		.build();

	val cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
	cm.setMaxTotal(200);
	cm.setDefaultMaxPerRoute(20);

	/**
	 * retrieves a client object
	 */
	private def getClient = {
		val client = HttpClients.custom()
			.setConnectionManager(cm)
			.build();

		client;
	}

	def sendDataset(topic: String, batchId: Long, rdd: RDD[Row], maxPacketSize: Int = 10 * 1024 * 1024): Int = {
		val iter = rdd.toLocalIterator;
		val buffer = ArrayBuffer[Row]();
		var rows = 0;

		def flush() = {
			rows += sendRows(topic, batchId, buffer.toArray);
			buffer.clear();
		}

		while (iter.hasNext) {
			buffer += iter.next();
			val objects = buffer.toArray;
			val bytes = HttpStreamUtils.serialize(kryoSerializer, objects);
			if (bytes.length >= maxPacketSize) {
				flush();
			}
		}

		if (!buffer.isEmpty) {
			flush();
		}

		rows;
	}

	private def executeRequest(requestBody: Map[String, Any]): Map[String, Any] = {
		val post = new HttpPost(httpServletUrl);
		val bytes = HttpStreamUtils.serialize(kryoSerializer, requestBody);
		val builder = EntityBuilder.create()
			.setBinary(bytes)
			.setContentType(ContentType.APPLICATION_OCTET_STREAM);

		val entity = builder.build();
		post.setEntity(entity);
		val client = getClient;
		val resp = client.execute(post);
		if (resp.getStatusLine.getStatusCode != 200) {
			throw new HttpStreamServerSideException(resp.getStatusLine.getReasonPhrase);
		}

		val is = resp.getEntity.getContent;
		val responseBody = HttpStreamUtils.deserialize(kryoSerializer, is);
		responseBody.asInstanceOf[Map[String, Any]];
	}

	def fetchSchema(topic: String):StructType={
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

		res("received").asInstanceOf[Int];
	}

	//subscribe topics
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

	def fetchStream[T: ClassTag](subscriberId: String): Array[(Timestamp, Row)] = {
		val res = executeRequest(Map[String, Any](
			"action" -> "actionFetchStream",
			"subscriberId" -> subscriberId));

		res("rows").asInstanceOf[Array[(Timestamp, Row)]];
	}
}

class HttpStreamServerSideException(msg: String) extends RuntimeException(msg) {
}