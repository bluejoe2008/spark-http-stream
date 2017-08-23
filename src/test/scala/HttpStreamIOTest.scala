import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.HttpStreamClient
import org.apache.spark.sql.execution.streaming.HttpStreamServer
import org.apache.spark.sql.execution.streaming.HttpStreamServerSideException
import org.apache.spark.sql.execution.streaming.ObjectArrayPrinter
import org.junit.Assert
import org.junit.Test
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.ByteType

class HttpStreamIOTest {
	val ROWS1 = Array(Row("hello1", 1, true, 0.1f, 0.1d, 1L, new Date(10000)),
		Row("hello2", 2, false, 0.2f, 0.2d, 2L, new Date(20000)),
		Row("hello3", 3, true, 0.3f, 0.3d, 3L, new Date(30000)));

	val ROWS2 = Array(Row("hello"),
		Row("world"),
		Row("bye"),
		Row("world"));

	@Test
	def testHttpStreamIO() {
		//starts a http server with a receiver servlet
		val kryoSerializer = new KryoSerializer(new SparkConf());
		val receiver = HttpStreamServer.start(kryoSerializer, "/xxxx", 8080);
		val spark = SparkSession.builder.appName("testHttpTextSink").master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		import spark.implicits._
		receiver.withBuffer()
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Date)]("topic-1")
			.createTopic[String]("topic-2");

		val client = HttpStreamClient.connect("http://localhost:8080/xxxx", kryoSerializer);
		val schema1 = client.fetchSchema("topic-1");
		Assert.assertArrayEquals(Array[Object](StringType, IntegerType, BooleanType, FloatType, DoubleType, LongType, DateType),
			schema1.fields.map(_.dataType).asInstanceOf[Array[Object]]);
		val schema2 = client.fetchSchema("topic-2");
		Assert.assertArrayEquals(Array[Object](StringType),
			schema2.fields.map(_.dataType).asInstanceOf[Array[Object]]);

		val sid1 = client.subscribe("topic-1")._1;
		val sid2 = client.subscribe("topic-2")._1;

		client.sendRows("topic-1", 1, ROWS1);

		val sid4 = client.subscribe("topic-1")._1;
		val sid5 = client.subscribe("topic-2")._1;

		client.sendRows("topic-2", 1, ROWS2);
		val fetched = client.fetchStream(sid1).map(_._2);
		Assert.assertArrayEquals(ROWS1.asInstanceOf[Array[Object]], fetched.asInstanceOf[Array[Object]]);
		//it is empty now
		Assert.assertArrayEquals(Array[Object](), client.fetchStream(sid1).map(_._2).asInstanceOf[Array[Object]]);
		Assert.assertArrayEquals(ROWS2.asInstanceOf[Array[Object]], client.fetchStream(sid2).map(_._2).asInstanceOf[Array[Object]]);
		Assert.assertArrayEquals(Array[Object](), client.fetchStream(sid4).map(_._2).asInstanceOf[Array[Object]]);
		Assert.assertArrayEquals(ROWS2.asInstanceOf[Array[Object]], client.fetchStream(sid5).map(_._2).asInstanceOf[Array[Object]]);
		Assert.assertArrayEquals(Array[Object](), client.fetchStream(sid5).map(_._2).asInstanceOf[Array[Object]]);

		client.unsubscribe(sid4);
		try {
			client.fetchStream(sid4);
			Assert.assertTrue(false);
		}
		catch {
			case e ⇒
				e.printStackTrace();
				Assert.assertEquals(classOf[HttpStreamServerSideException], e.getClass);
		}

		receiver.stop();
	}
}
