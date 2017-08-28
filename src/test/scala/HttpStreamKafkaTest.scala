import java.util.Arrays
import java.util.Properties

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.http.HttpStreamClient
import org.apache.spark.sql.execution.streaming.http.HttpStreamServer
import org.apache.spark.sql.execution.streaming.http.StreamPrinter
import org.junit.Assert
import org.junit.Test
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Callback
import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.clients.consumer.ConsumerRecords

class HttpStreamKafkaTest {

	val LINES2 = Array[String]("hello", "world", "bye", "world");
	val ROWS2 = LINES2.map(Row(_));

	class ConsumerThread(topic: String, groupId: String, buffer: ArrayBuffer[String]) extends Thread {
		//consumer
		val props = new Properties();
		props.put("group.id", groupId);
		props.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");

		val consumer = new KafkaConsumer[String, String](props);
		consumer.subscribe(Arrays.asList(topic));

		override def run {
			while (true) {
				val records = consumer.poll(100);
				records.foreach(record ⇒
					println("key:" + record.key() + " value=" + record.value() + " partition:" + record.partition() + " offset=" + record.offset()));
				buffer ++= records.map(_.value()).toSeq;
				Thread.sleep(100);
			}
		}
	}

	@Test
	def testKafka() {
		val propsProducer = new Properties();

		propsProducer.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		propsProducer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsProducer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		val producer = new KafkaProducer[String, String](propsProducer);

		val topic = "kafka-topic2";
		var index = -1;
		for (row ← ROWS2) {
			index += 1;
			val key = "" + index;
			//TODO: send an array instead of a string value?
			val value = row(0).toString();
			val record = new ProducerRecord[String, String](topic, key, value);
			producer.send(record, new Callback() {
				def onCompletion(metadata: RecordMetadata, e: Exception) = {
					if (metadata != null) {
						val offset = metadata.offset();
						val partition = metadata.partition();
						val topic = metadata.topic();
						println(s"record is sent to kafka: topic=$topic, key=$key, value=$value, partition=$partition, offset=$offset");
					}
				}
			});
		}

		Thread.sleep(1000);

		val buffer = ArrayBuffer[String]();
		val thread = new ConsumerThread("kafka-topic2", "g1", buffer);
		thread.start();
		Thread.sleep(10000);

		val records = buffer.toArray;
		thread.stop();
		println(records.toSeq);
		Assert.assertArrayEquals(LINES2.sorted.asInstanceOf[Array[Object]], records.sorted.asInstanceOf[Array[Object]]);
	}

	@Test
	def testHttpStreamKafka() {
		//starts a http server with a kafka receiver
		val server = HttpStreamServer.start("/xxxx", 8080);

		server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
			.addListener(new StreamPrinter());

		val client = HttpStreamClient.connect("http://localhost:8080/xxxx");

		//send ROWS2 to HttpStreamServer, and the server will forward messages to Kafka
		client.sendRows("kafka-topic1", 1, ROWS2);

		val buffer = ArrayBuffer[String]();
		val thread = new ConsumerThread("kafka-topic1", "g2", buffer);
		thread.start();
		//now, fetch records from kafka
		Thread.sleep(10000);
		val records2 = buffer.toSeq;
		thread.stop();
		println(records2);
		Assert.assertArrayEquals(LINES2.sorted.asInstanceOf[Array[Object]], records2.sorted.toArray.asInstanceOf[Array[Object]]);
		server.stop();
	}
}
