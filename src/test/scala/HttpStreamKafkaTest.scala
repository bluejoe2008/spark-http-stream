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

class HttpStreamKafkaTest {

	val LINES2 = Array[String]("hello", "world", "bye", "world");
	val ROWS2 = LINES2.map(Row(_));

	@Test
	def testKafka() {
		//kafka conf
		val props = new Properties();

		props.put("group.id", "test");
		props.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");

		//subscribe
		val consumer2 = new KafkaConsumer[String, String](props);
		consumer2.subscribe(Arrays.asList("kafka-topic2"));

		//producer
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

		Thread.sleep(5000);
		var records = consumer2.poll(5000).map(_.value()).toSeq;
		println(records);
		Assert.assertArrayEquals(LINES2.asInstanceOf[Array[Object]], records.toArray.asInstanceOf[Array[Object]]);
		println("~~~~~~~~~~~~~~~~~~~~~~");
	}

	@Test
	def testHttpStreamKafka() {
		//kafka conf
		val props = new Properties();

		props.put("group.id", "test");
		props.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");

		val consumer2 = new KafkaConsumer[String, String](props);
		consumer2.subscribe(Arrays.asList("kafka-topic1"));
		val records3 = consumer2.poll(5000).map(_.value()).toSeq;
		println(records3);

		//starts a http server with a kafka receiver
		val server = HttpStreamServer.start("/xxxx", 8080);

		server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
			.addListener(new StreamPrinter());

		val client = HttpStreamClient.connect("http://localhost:8080/xxxx");

		//send ROWS2 to HttpStreamServer, and the server will forward messages to Kafka
		client.sendRows("kafka-topic1", 1, ROWS2);

		//now, fetch records from kafka
		val records2 = consumer2.poll(5000).map(_.value()).toSeq;
		println(records2);
		Assert.assertArrayEquals(LINES2.asInstanceOf[Array[Object]], records2.toArray.asInstanceOf[Array[Object]]);
		server.stop();
	}
}
