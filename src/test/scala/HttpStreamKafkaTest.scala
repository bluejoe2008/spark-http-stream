import java.util.Arrays
import java.util.Properties

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.http.HttpStreamClient
import org.apache.spark.sql.execution.streaming.http.HttpStreamServer
import org.apache.spark.sql.execution.streaming.http.ObjectArrayPrinter
import org.junit.Assert
import org.junit.Test

class HttpStreamKafkaTest {

	val LINES2 = Array[String]("hello", "world", "bye", "world");
	val ROWS2 = LINES2.map(Row(_));

	@Test
	def testHttpStreamKafka() {
		//kafka conf
		val props = new Properties();

		props.put("group.id", "test");
		props.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");

		val consumer1 = new KafkaConsumer[String, String](props);
		consumer1.subscribe(Arrays.asList("kafka-topic1"));
		var records1: Seq[String] = null;
		do {
			records1 = consumer1.poll(5000).map(_.value()).toSeq;
			println(records1);
		} while (!records1.isEmpty)

		consumer1.close();
		props.put("auto.offset.reset", "latest");
		val consumer2 = new KafkaConsumer[String, String](props);
		consumer2.subscribe(Arrays.asList("kafka-topic1"));

		//starts a http server with a kafka receiver
		val server = HttpStreamServer.start("/xxxx", 8080);

		server.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
			.addListener(new ObjectArrayPrinter());

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
