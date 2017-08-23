import java.util.Arrays
import java.util.Properties

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ArrayBuffer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.execution.streaming.HttpStreamClient
import org.apache.spark.sql.execution.streaming.HttpStreamServer
import org.junit.Assert
import org.junit.Test
import org.apache.spark.sql.execution.streaming.ObjectArrayPrinter
import java.sql.Date
import org.apache.spark.sql.Row

class HttpStreamKafkaTest {

	val LINES2 = Array[String]("hello", "world", "bye", "world");
	val ROWS2 = LINES2.map(Row(_));

	@Test
	def testHttpStreamKafka() {
		//starts a http server with a receiver servlet
		val kryoSerializer = new KryoSerializer(new SparkConf());
		val receiver = HttpStreamServer.start(kryoSerializer, "/xxxx", 8080);

		receiver.withKafka("vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092")
			.addListener(new ObjectArrayPrinter());

		//kafka
		val props = new Properties();

		props.put("group.id", "test");
		props.put("bootstrap.servers", "vm105:9092,vm106:9092,vm107:9092,vm181:9092,vm182:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "latest");

		val consumer = new KafkaConsumer[String, String](props);
		consumer.subscribe(Arrays.asList("kafka-topic1"));

		val client = HttpStreamClient.connect("http://localhost:8080/xxxx", kryoSerializer);
		client.sendRows("kafka-topic1", 1, ROWS2);

		//fetch records from kafka
		val records = consumer.poll(1000).map(_.value()).toArray;

		Assert.assertArrayEquals(LINES2.asInstanceOf[Array[Object]], records.asInstanceOf[Array[Object]]);
		receiver.stop();
	}
}
