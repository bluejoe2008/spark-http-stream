import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HttpStreamClient
import org.apache.spark.sql.execution.streaming.HttpStreamServer
import org.apache.spark.sql.execution.streaming.HttpStreamSinkProvider
import org.apache.spark.sql.execution.streaming.MemorySink
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.ObjectArrayCollector
import org.apache.spark.sql.execution.streaming.ObjectArrayPrinter
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
import org.apache.spark.sql.execution.streaming.HttpStreamSourceProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.commons.io.FileUtils
import java.io.File

class SparkHttpStreamTest {
	val ROWS1: Array[Row] = Array(Row("hello1", 1, true, 0.1f, 0.1d, 1L, new Date(10000)),
		Row("hello2", 2, false, 0.2f, 0.2d, 2L, new Date(20000)), Row("hello3", 3, true, 0.3f, 0.3d, 3L, new Date(30000)));

	val ROWS2: Array[Row] = Array(Row("HELLO1", 2, false), Row("HELLO2", 3, true), Row("HELLO3", 4, false));

	@Test
	def testHttpStreamSink() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		//we read data from memory
		import spark.implicits._
		val memoryStream = new MemoryStream[(String, Int, Boolean, Float, Double, Long, Date)](1, sqlContext);
		val schema = memoryStream.schema;
		//val memoryStream = new MemoryStream[String](1, sqlContext);
		val kryoSerializer = new KryoSerializer(new SparkConf());
		//starts a http server with a receiver servlet
		val receiver = HttpStreamServer.start(kryoSerializer, "/xxxx", 8080);
		//add a listener by which we can test if the sink works well
		val collector = new ObjectArrayCollector();
		receiver.withBuffer()
			.addListener(collector)
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Date)]("topic-1");

		//memory->map->HttpTextSink
		val query = memoryStream.toDF().writeStream
			.format(classOf[HttpStreamSinkProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1")
			.start();

		//produces data now
		memoryStream.addData(ROWS1.map { row ⇒
			(row(0).asInstanceOf[String], row(1).asInstanceOf[Int], row(2).asInstanceOf[Boolean],
				row(3).asInstanceOf[Float], row(4).asInstanceOf[Double], row(5).asInstanceOf[Long], row(6).asInstanceOf[Date])
		});

		query.awaitTermination(5000);
		receiver.stop();

		//the listener got data in the sink
		val data = collector.get;
		Assert.assertArrayEquals(Array[Object](StringType, IntegerType, BooleanType, FloatType, DoubleType, LongType, DateType), data(0).schema.fields.map(_.dataType).asInstanceOf[Array[Object]]);
		//Date.equals() returns false
		val d1 = ROWS1(0)(6);
		val d2 = data(0)(6);

		Assert.assertArrayEquals(ROWS1.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]], data.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]]);
	}

	var i = 100;

	def queryStream(includesTimestamp: Boolean) = {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		//we read data from memory
		import spark.implicits._
		val kryoSerializer = new KryoSerializer(new SparkConf());
		//starts a http server with a receiver servlet
		val receiver = HttpStreamServer.start(kryoSerializer, "/xxxx", 8080);
		receiver.withBuffer()
			.createTopic[(String, Int, Boolean, Float, Double, Long, Date)]("topic-1");

		//HttpTextStream as a source stream
		val reader = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1");
		if (includesTimestamp) {
			reader.option("includesTimestamp", "true");
		}

		val lines = reader.load();
		i += 1;
		//HttpTextStream->map->memory
		FileUtils.deleteDirectory(new File(s"/tmp/query$i"));
		val query = lines.writeStream
			.option("queryName", s"query$i")
			.format("memory")
			.start();

		//produces some data now
		val sender = HttpStreamClient.connect("http://localhost:8080/xxxx", kryoSerializer);
		sender.sendRows("topic-1", -1, ROWS1);

		query.awaitTermination(5000);

		//tests if memorySink get data
		val memorySink = query.asInstanceOf[StreamExecution].sink.asInstanceOf[MemorySink];
		val ds = memorySink.allData;

		receiver.stop();
		spark.close();
		ds;
	}

	@Test
	def testHttpStreamSource() {
		val ds = queryStream(false);
		//yes, get correct data
		Assert.assertArrayEquals(ROWS1.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]]);
	}

	@Test
	def testHttpStreamSourceWithTimestamp() {
		val ds = queryStream(true);
		//yes, get correct data
		Assert.assertArrayEquals(ROWS1.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.dropRight(2).toArray).toArray.asInstanceOf[Array[Object]]);
	}

	@Test
	def testSinkAndSource() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val sqlContext = spark.sqlContext;
		//we read data from memory
		import spark.implicits._
		val memoryStream = new MemoryStream[(String, Int, Boolean, Float, Double, Long, Date)](1, sqlContext);
		val schema = memoryStream.schema;

		val kryoSerializer = new KryoSerializer(new SparkConf());
		//starts a http server with a receiver servlet
		val receiver = HttpStreamServer.start(kryoSerializer, "/xxxx", 8080);
		//add a listener by which we can test if the sink works well
		receiver.withBuffer()
			.addListener(new ObjectArrayPrinter())
			.createTopic[(String, Int, Boolean, Float, Double, Long, Date)]("topic-1");

		//memory->map->HttpTextSink
		val query1 = memoryStream.toDF().writeStream
			.format(classOf[HttpStreamSinkProvider].getName)
			.option("httpServletUrl", "http://localhost:8080/xxxx")
			.option("topic", "topic-1")
			.start();

		//HttpTextStream->map->memory
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
				row(3).asInstanceOf[Float], row(4).asInstanceOf[Double], row(5).asInstanceOf[Long], row(6).asInstanceOf[Date])
		});

		query1.awaitTermination(5000);
		query2.awaitTermination(5000);
		receiver.stop();

		//tests if memorySink get data
		val memorySink = query2.asInstanceOf[StreamExecution].sink.asInstanceOf[MemorySink];
		val ds = memorySink.allData;

		Assert.assertArrayEquals(ROWS1.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]], ds.map(_.toSeq.dropRight(1).toArray).toArray.asInstanceOf[Array[Object]]);
	}
}
