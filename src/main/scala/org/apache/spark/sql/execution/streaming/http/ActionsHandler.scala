package org.apache.spark.sql.execution.streaming.http

import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.types.StructType
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
	def onReceiveStream(topic: String, rows: Array[RowEx]);
	def getRequiredParam(requestBody: Map[String, Any], key: String): Any;

	val listeners = ArrayBuffer[StreamListener]();

	def addListener(listener: StreamListener): this.type = {
		listeners += listener;
		this;
	}

	protected def notifyListeners(topic: String, data: Array[RowEx]) {
		listeners.foreach { _.onArrive(topic, data); }
	}

	def handleSendStream(requestBody: Map[String, Any]): Map[String, Any] = {
		val topic = getRequiredParam(requestBody, "topic").asInstanceOf[String];
		val batchId = getRequiredParam(requestBody, "batchId").asInstanceOf[Long];
		val rows = getRequiredParam(requestBody, "rows").asInstanceOf[Array[Row]];
		val ts = new Timestamp(System.currentTimeMillis());
		var index = -1;
		val rows2 = rows.map { row ⇒
			index += 1;
			RowEx(Row.fromSeq(row.toSeq), batchId, index, ts)
		}

		onReceiveStream(topic, rows2);
		notifyListeners(topic, rows2);
		Map("rowsCount" -> rows.size);
	}
}