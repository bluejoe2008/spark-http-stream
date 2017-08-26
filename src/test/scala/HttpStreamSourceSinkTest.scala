import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.http.HttpStreamClient
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.junit.Assert
import org.junit.Test
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.commons.io.FileUtils
import java.io.File
import java.sql.Timestamp
import org.apache.spark.sql.execution.streaming.http.ObjectArrayCollector
import org.apache.spark.sql.execution.streaming.http.HttpStreamServer
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.MemorySink
import org.apache.spark.sql.execution.streaming.http.HttpStreamSourceProvider
import org.apache.spark.sql.execution.streaming.http.ObjectArrayPrinter
import org.apache.spark.sql.execution.streaming.http.HttpStreamSinkProvider

/**
 * tests HttpStreamSource and HttpStreamSink
 */
class HttpStreamSourceSinkTest {
	val ROWS1: Array[Row] = Array(Row("hello1", 1, true, 0.1f, 0.1d, 1L, '1'.toByte),
		Row("hello2", 2, false, 0.2f, 0.2d, 2L, '2'.toByte),
		Row("hello3", 3, true, 0.3f, 0.3d, 3L, '3'.toByte));

	@Test
	def testHttpStreamSink() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		//reads data from memory
		import spark.implicits._
		val memoryStream = new MemoryStream[(String, Int, Boolean, Float, Double, Long, Byte)](1, sqlContext);
		val schema = memoryStream.schema;
		//starts a http server
		val server = HttpStreamServer.start("/xxxx", 8080);
		//add a listener by which we can test if the sink works well
		val collector = new ObjectArrayCollector();
		server.withBuffer()
			.addListener(collector)
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1");

		//memory->map->HttpStreamSink
		val query = memoryStream.toDF().writeStream
			.format(classOf[HttpStreamSinkProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1")
			.start();

		//produces some data now
		memoryStream.addData(ROWS1.map { row ⇒
			(row(0).asInstanceOf[String], row(1).asInstanceOf[Int], row(2).asInstanceOf[Boolean],
				row(3).asInstanceOf[Float], row(4).asInstanceOf[Double], row(5).asInstanceOf[Long], row(6).asInstanceOf[Byte])
		});

		query.awaitTermination(5000);
		server.stop();

		//the collector should have got data from the sink
		val data = collector.get.map(_.originalRow);
		Assert.assertArrayEquals(ROWS1.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]], data.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]]);
	}

	def queryStream(includesTimestamp: Boolean) = {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		//reads data from memory
		import spark.implicits._
		//starts a http server with a receiver servlet
		val receiver = HttpStreamServer.start("/xxxx", 8080);
		receiver.withBuffer()
			.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1");

		//HttpStreamSource as a source stream
		val reader = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1");

		if (includesTimestamp) {
			reader.option("includesTimestamp", "true");
		}

		val lines = reader.load();
		//HttpStreamSource->map->memory
		FileUtils.deleteDirectory(new File(s"/tmp/query2"));
		val query = lines.writeStream
			.option("queryName", "query2")
			.format("memory")
			.start();

		//produces some data now
		val producer = HttpStreamClient.connect("http://localhost:8080/xxxx");
		producer.sendRows("topic-1", -1, ROWS1);

		query.awaitTermination(5000);

		//tests if memorySink get data
		val memorySink = query.asInstanceOf[StreamExecution].sink.asInstanceOf[MemorySink];
		val ds = memorySink.allData;

		receiver.stop();
		spark.stop();
		ds;
	}

	@Test
	def testHttpStreamSourceWithTimestamp() {
		val ds = queryStream(true);
		Assert.assertEquals(classOf[Timestamp], ds(0)(7).getClass());
		Assert.assertArrayEquals(ROWS1.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]]);
	}

	@Test
	def testHttpStreamSource() {
		val ds = queryStream(false);
		//yes, get correct data
		Assert.assertArrayEquals(ROWS1.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]]);
	}

	@Test
	def testSinkAndSource() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		//reads data from memory
		import spark.implicits._
		val memoryStream = new MemoryStream[(String, Int, Boolean, Float, Double, Long, Byte)](1, sqlContext);
		val schema = memoryStream.schema;

		//starts a http server with a buffer
		val receiver = HttpStreamServer.start("/xxxx", 8080);
		//add a listener by which we can test if the sink works well
		receiver.withBuffer()
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Byte)]("topic-1");

		//memory->map->HttpStreamSink
		val query1 = memoryStream.toDF().writeStream
			.format(classOf[HttpStreamSinkProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1")
			.start();

		//HttpStreamSource->map->memory
		FileUtils.deleteDirectory(new File(s"/tmp/query2"));
		val query2 = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1")
			.load()
			.writeStream
			.option("queryName", "query2")
			.format("memory")
			.start();

		//produces data now
		memoryStream.addData(ROWS1.map { row ⇒
			(row(0).asInstanceOf[String], row(1).asInstanceOf[Int], row(2).asInstanceOf[Boolean],
				row(3).asInstanceOf[Float], row(4).asInstanceOf[Double], row(5).asInstanceOf[Long], row(6).asInstanceOf[Byte])
		});

		query1.awaitTermination(5000);
		query2.awaitTermination(5000);
		receiver.stop();

		//tests if memorySink get correct data
		val memorySink = query2.asInstanceOf[StreamExecution].sink.asInstanceOf[MemorySink];
		val ds = memorySink.allData;

		Assert.assertArrayEquals(ROWS1.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.toArray).toArray.asInstanceOf[Array[Object]]);
	}
}
