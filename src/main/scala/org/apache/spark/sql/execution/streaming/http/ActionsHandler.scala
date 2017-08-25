package org.apache.spark.sql.execution.streaming.http

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
import java.util.concurrent.atomic.AtomicInteger

/**
 * ActionsHandler helps HttpStreamServer processes requests and responses
 * request body and response body are formatted in a Map[String, Any]
 */
trait ActionsHandler {
	/**
	 * alias of PartialFunction[String, Map[String, Any]]
	 * input argument is `action: String`
	 * output argument is `responseBody: Map[String, Any]`
	 */
	type ActionHandlerEntries = PartialFunction[String, Map[String, Any]];
	/**
	 * outputs a list of recognized actions ('fetchStream', for example) with corresponding action handler methods ('this.handleFetchStream', for example)
	 */
	def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries;
	def destroy();
}

trait ActionsHandlerFactory {
	def createInstance(params: Params): ActionsHandler;
}

class MemoryBufferAsReceiverFactory extends ActionsHandlerFactory {
	def createInstance(params: Params) = new MemoryBufferAsReceiver();
}

class KafkaAsReceiverFactory extends ActionsHandlerFactory {
	def createInstance(params: Params) = new KafkaAsReceiver(params.getRequiredString("bootstrapServers"));
}

abstract class AbstractActionsHandler extends ActionsHandler {
	def getRequiredParam(requestBody: Map[String, Any], key: String): Any = {
		val opt = requestBody.get(key);
		if (opt.isEmpty) {
			throw new MissingRequiredRequestParameterException(key);
		}

		opt.get;
	}

	override def destroy() = {
	}
}

class NullActionsHandler extends AbstractActionsHandler {
	override def listActionHandlerEntries(requestBody: Map[String, Any]): ActionHandlerEntries = new ActionHandlerEntries() {
		def apply(action: String) = Map[String, Any]();
		//yes, do nothing
		def isDefinedAt(action: String) = false;
	};
}

//rich row with extra info: id, time stamp, ...
case class RowEx(originalRow: Row, batchId: Long, offsetInBatch: Long, timestamp: Timestamp) {
	def withTimestamp(): Row = Row.fromSeq(originalRow.toSeq :+ timestamp);
	def withId(): Row = Row.fromSeq(originalRow.toSeq :+ s"$batchId-$offsetInBatch");
	def extra: (Long, Long, Timestamp) = { (batchId, offsetInBatch, timestamp) };
}

trait SendStreamActionSupport {
	def onReceiveStream(topic: String, batchId: Long, rows: Array[Row]);
	def getRequiredParam(requestBody: Map[String, Any], key: String): Any;

	val listeners = ArrayBuffer[ObjectArrayListener]();

	def addListener(listener: ObjectArrayListener): this.type = {
		listeners += listener;
		this;
	}

	protected def notifyListeners(topic: String, batchId: Long, data: Array[Row]) {
		listeners.foreach { _.onArrive(topic, batchId, data); }
	}

	def handleSendStream(requestBody: Map[String, Any]): Map[String, Any] = {
		val topic = getRequiredParam(requestBody, "topic").asInstanceOf[String];
		val batchId = getRequiredParam(requestBody, "batchId").asInstanceOf[Long];
		val rows = getRequiredParam(requestBody, "rows").asInstanceOf[Array[Row]].map(row ⇒ Row.fromSeq(row.toSeq));

		onReceiveStream(topic, batchId, rows);
		notifyListeners(topic, batchId, rows);
		Map("rowsCount" -> rows.size);
	}
}

class InvalidSubscriberIdException(subscriberId: String)
		extends RuntimeException(s"invalid subscriber-id: $subscriberId") {
}

class TopicNotExistException(topic: String)
		extends RuntimeException(s"topic does not exist: $topic") {
}

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
		var index = 0;
		subscribledTopics.filter(_._2.equals(topic)).foreach {
			x ⇒
				subscribledMessages.synchronized {
					val buffer = subscribledMessages(x._1);
					buffer ++= rows.map(RowEx(_, batchId, index, ts));
				}

				index += 1;
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

/**
 * forwards all received data to kafka
 */
class KafkaAsReceiver(bootstrapServers: String) extends AbstractActionsHandler with SendStreamActionSupport with Logging {
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