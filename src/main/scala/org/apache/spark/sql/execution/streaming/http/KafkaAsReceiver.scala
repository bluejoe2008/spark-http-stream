package org.apache.spark.sql.execution.streaming.http

import java.util.Properties

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.spark.internal.Logging

/**
 * @author bluejoe2008@gmail.com
 */
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

	override def onReceiveStream(topic: String, rows: Array[RowEx]) = {
		var index = -1;
		for (row ← rows) {
			index += 1;
			val key = "" + row.batchId + "-" + row.offsetInBatch;
			//TODO: send an array instead of a string value?
			val value = row.originalRow(0).toString();
			val record = new ProducerRecord[String, String](topic, key, value);
			producer.send(record, new Callback() {
				def onCompletion(metadata: RecordMetadata, e: Exception) = {
					if (e != null) {
						e.printStackTrace();
						logError(e.getMessage);
					}
					else {
						val offset = metadata.offset();
						val partition = metadata.partition();
						logDebug(s"record is sent to kafka:key=$key, value=$value, partition=$partition, offset=$offset");
					}
				}
			});
		}
	}
}

class KafkaAsReceiverFactory extends ActionsHandlerFactory {
	def createInstance(params: Params) = new KafkaAsReceiver(params.getRequiredString("bootstrapServers"));
}