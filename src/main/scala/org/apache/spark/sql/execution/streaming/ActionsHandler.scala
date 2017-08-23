package org.apache.spark.sql.execution.streaming

import java.util.Properties
import org.apache.spark.sql.catalyst.encoders.encoderFor
import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Encoder

trait ActionsHandler {
	type ActionHandlerEntries = PartialFunction[String, Map[String, Any]];
	def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries;
	def destroy();
}

trait AbstractActionsHandler extends ActionsHandler {
	def getRequiredParam(requestBody: Map[String, Any], key: String): Any = {
		val opt = requestBody.get(key);
		if (opt.isEmpty) {
			throw new MissingRequiredParameterException(key);
		}

		opt.get;
	}

	override def destroy() = {
	}
}

class LazyActionsHandler extends AbstractActionsHandler {
	override def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries = new ActionHandlerEntries() {
		def apply(action: String) = Map[String, Any]();
		def isDefinedAt(action: String) = false;
	};
}

trait SendStreamActionHandler {
	def onReceiveStream(topic: String, batchId: Long, rows: Array[Row]);
	def getRequiredParam(requestBody: Map[String, Any], key: String): Any;

	val listeners = ArrayBuffer[ObjectArrayListener]();

	def addListener(listener: ObjectArrayListener): this.type = {
		listeners += listener;
		this;
	}

	protected def notifyListeners(topic: String, batchId: Long, data: Array[Row]) {
		listeners.foreach { _.onReceived(topic, batchId, data); }
	}

	def handleSendStream(requestBody: Map[String, Any]): Map[String, Any] = {
		val topic = getRequiredParam(requestBody, "topic").asInstanceOf[String];
		val batchId = getRequiredParam(requestBody, "batchId").asInstanceOf[Long];
		val rows = getRequiredParam(requestBody, "rows").asInstanceOf[Array[Row]];

		onReceiveStream(topic, batchId, rows);
		notifyListeners(topic, batchId, rows);
		Map("received" -> rows.size);
	}
}

class InvalidSubscriberIdException(subscriberId: String)
		extends RuntimeException(s"invalid subscriber-id: $subscriberId") {
}

class TopicNotExistException(topic: String)
		extends RuntimeException(s"topic does not exist: $topic") {
}

class BufferedReceiver extends AbstractActionsHandler with SendStreamActionHandler with Logging {
	var totalLineCount = 0L;
	var consumerIdSerial = 0L;

	val topicsWithSchema = collection.mutable.Map[String, StructType]();
	val consumer2TopicsMap = collection.mutable.Map[String, String]();
	val consumerMessages = collection.mutable.Map[String, ArrayBuffer[(Timestamp, Row)]]();

	def createTopic(topic: String, schema: StructType): this.type = {
		topicsWithSchema(topic) = schema;
		this;
	}

	def createTopic[A: Encoder](topic: String): this.type = {
		createTopic(topic, encoderFor[A].schema);
	}

	override def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries = {
		case "actionSubscribe" ⇒ handleSubscribe(requestBody);
		case "actionUnsubscribe" ⇒ handleUnsubscribe(requestBody);
		case "actionSendStream" ⇒ handleSendStream(requestBody);
		case "actionFetchStream" ⇒ handleFetchStream(requestBody);
		case "actionFetchSchema" ⇒ handleFetchSchema(requestBody);
	}

	override def onReceiveStream(topic: String, batchId: Long, rows: Array[Row]) {
		assertTopicExist(topic);
		val ts = new Timestamp(System.currentTimeMillis());
		consumer2TopicsMap.filter(_._2.equals(topic)).foreach {
			x ⇒
				val buffer = consumerMessages(x._1);
				buffer.synchronized {
					buffer ++= rows.map((ts, _));
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
		val buffer = consumerMessages(subscriberId);
		val rows: Array[(Timestamp, Row)] = {
			buffer.synchronized {
				if (!buffer.isEmpty) {
					val clone = buffer.clone();
					buffer.clear();
					clone.toArray;
				}
				else {
					Array[(Timestamp, Row)]();
				}
			}
		}

		Map("rows" -> rows);
	}

	private def generateSubscriberId() = {
		val nid = this.synchronized {
			consumerIdSerial += 1;
			consumerIdSerial;
		}

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
		consumer2TopicsMap += (sid -> topic);
		consumerMessages += (sid -> ArrayBuffer[(Timestamp, Row)]());
		Map("subscriberId" -> sid, "topic" -> topic, "schema" -> topicsWithSchema(topic));
	}

	//get subscriber id from http request header and validate if it is registered
	private def getValidatedSubscriberId(requestBody: Map[String, Any]) = {
		val subscriberId = getRequiredParam(requestBody, "subscriberId").asInstanceOf[String];
		if (subscriberId == null)
			throw new MissingRequiredParameterException("subscriberId");

		if (!consumer2TopicsMap.contains(subscriberId))
			throw new InvalidSubscriberIdException(subscriberId);

		subscriberId;
	}

	def handleUnsubscribe(requestBody: Map[String, Any]): Map[String, Any] = {
		val subscriberId = getValidatedSubscriberId(requestBody);
		consumer2TopicsMap.remove(subscriberId);
		consumerMessages.remove(subscriberId);
		Map("subscriberId" -> subscriberId);
	}
}

class KafkaAsReceiver(bootstrapServers: String) extends AbstractActionsHandler with SendStreamActionHandler with Logging {
	val props = new Properties();
	props.put("bootstrap.servers", bootstrapServers);
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	val producer = new KafkaProducer[String, String](props);

	override def listActionHandlerEntries(requestBody: Map[String, Any]): PartialFunction[String, Map[String, Any]] = {
		case "actionSendStream" ⇒ handleSendStream(requestBody);
	}

	override def destroy() {
		producer.close();
	}

	override def onReceiveStream(topic: String, batchId: Long, rows: Array[Row]) = {
		var index = -1;
		rows.map { x ⇒
			index += 1;
			new ProducerRecord[String, String](topic, "" + batchId + "-" + index, x.toString());
		}.foreach(producer.send);
	}
}