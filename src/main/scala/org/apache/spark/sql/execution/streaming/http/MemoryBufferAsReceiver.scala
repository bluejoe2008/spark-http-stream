package org.apache.spark.sql.execution.streaming.http

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Encoder
import java.sql.Timestamp
import org.apache.spark.internal.Logging

/**
 * @author bluejoe2008@gmail.com
 */
/**
 * stores data sent from producers into a local memory buffer,
 * and allows consumers fetch data in batch
 */
class MemoryBufferAsReceiver extends AbstractActionsHandler with SendStreamActionSupport with Logging {
	val consumerIdSerial = new AtomicInteger(0);

	val topicsWithSchema = collection.mutable.Map[String, StructType]();
	val subscribledTopics = collection.mutable.Map[String, String]();
	val subscribledMessages = collection.mutable.Map[String, ArrayBuffer[RowEx]]();

	/**
	 * creates a topic with schema definition
	 */
	def createTopic(topic: String, schema: StructType): this.type = {
		topicsWithSchema(topic) = schema;
		this;
	}

	/**
	 * creates a topic with schema definition, where schema is inferred from type parameter A
	 */
	def createTopic[A: Encoder](topic: String): this.type = {
		import org.apache.spark.sql.catalyst.encoders.encoderFor
		createTopic(topic, encoderFor[A].schema);
	}

	override def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries = {
		case "actionSubscribe" ⇒ handleSubscribe(requestBody);
		case "actionUnsubscribe" ⇒ handleUnsubscribe(requestBody);
		case "actionSendStream" ⇒ handleSendStream(requestBody);
		case "actionFetchStream" ⇒ handleFetchStream(requestBody);
		case "actionFetchSchema" ⇒ handleFetchSchema(requestBody);
	}

	override def onReceiveStream(topic: String, rows: Array[RowEx]) {
		assertTopicExist(topic);
		val ts = new Timestamp(System.currentTimeMillis());
		subscribledTopics.filter(_._2.equals(topic)).foreach {
			x ⇒
				subscribledMessages.synchronized {
					val buffer = subscribledMessages(x._1);
					buffer ++= rows;
				}
		}
	}

	def handleFetchSchema(requestBody: Map[String, Any]): Map[String, Any] = {
		val topic = getRequiredParam(requestBody, "topic").asInstanceOf[String];
		assertTopicExist(topic);
		Map("schema" -> topicsWithSchema(topic));
	}

	def handleFetchStream(requestBody: Map[String, Any]): Map[String, Any] = {
		val subscriberId = getValidatedSubscriberId(requestBody);
		val rows: Array[RowEx] = subscribledMessages.synchronized {
			val buffer = subscribledMessages(subscriberId);
			if (!buffer.isEmpty) {
				val clone = buffer.clone();
				buffer.clear();
				clone.toArray;
			}
			else {
				Array[RowEx]();
			}
		}

		Map("rows" -> rows, "rowsCount" -> rows.size);
	}

	private def generateSubscriberId() = {
		val nid = consumerIdSerial.addAndGet(1);
		s"subscriber-$nid";
	}

	private def assertTopicExist(topic: String) = {
		if (!topicsWithSchema.contains(topic))
			throw new TopicNotExistException(topic);
	}

	def handleSubscribe(requestBody: Map[String, Any]): Map[String, Any] = {
		val sid = generateSubscriberId;
		val topic = getRequiredParam(requestBody, "topic").asInstanceOf[String];
		assertTopicExist(topic);
		this.synchronized {
			subscribledTopics += (sid -> topic);
			subscribledMessages += (sid -> ArrayBuffer[RowEx]());
		}

		Map("subscriberId" -> sid, "topic" -> topic, "schema" -> topicsWithSchema(topic));
	}

	//get subscriber id from http request header and validate if it is registered
	private def getValidatedSubscriberId(requestBody: Map[String, Any]) = {
		val subscriberId = getRequiredParam(requestBody, "subscriberId").asInstanceOf[String];

		if (!subscribledTopics.contains(subscriberId))
			throw new InvalidSubscriberIdException(subscriberId);

		subscriberId;
	}

	def handleUnsubscribe(requestBody: Map[String, Any]): Map[String, Any] = {
		val subscriberId = getValidatedSubscriberId(requestBody);
		this.synchronized {
			subscribledTopics.remove(subscriberId);
			subscribledMessages.remove(subscriberId);
		}

		Map("subscriberId" -> subscriberId);
	}
}

class InvalidSubscriberIdException(subscriberId: String)
		extends RuntimeException(s"invalid subscriber-id: $subscriberId") {
}

class TopicNotExistException(topic: String)
		extends RuntimeException(s"topic does not exist: $topic") {
}