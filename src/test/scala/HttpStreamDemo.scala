import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HttpStreamServer
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.ObjectArrayPrinter
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.execution.streaming.HttpStreamSourceProvider
import org.apache.spark.sql.execution.streaming.HttpStreamSinkProvider

/**
 * this Demo tests HttpTextStream and HttpTextSink:
 * 1. choose machine A, run 'HttpStreamDemo start-server-on 8080 /xxxx', this starts a HTTP server which receives data from machine B
 * 2. choose machine B, run 'nc -lk 9999'
 * 3. run 'HttpStreamDemo read-from http://machine-a-host:8080/xxxx' on machine B
 * 4. run 'HttpStreamDemo write-into http://machine-a-host:8080/xxxx' on machine C
 * 5. type some text in nc, data will be received by HttpStreamSink and then produced as HttpStreamSource, finally displayed on console
 */

object HttpStreamDemo {

	def printUsage() {
		println("USAGE:");
		val name = this.getClass.getSimpleName;
		println(s"\t$name start-server-on 8080 /xxxx");
		println(s"\t$name write-into http://localhost:8080/xxxx");
		println(s"\t$name read-from http://localhost:8080/xxxx");
	}

	def main(args: Array[String]) {
		if (args.length == 0) {
			printUsage();
		}
		else {
			args(0) match {
				case "write-into" ⇒ runAsSink(args(1));
				case "start-server-on" ⇒ runAsReceiver(args(2), args(1).toInt);
				case "read-from" ⇒ runAsSource(args(1));
				case s: String ⇒ printUsage();
			}
		}
	}

	def runAsSink(httpServletURL: String) {
		val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[4]")
			.getOrCreate();

		println(s"reading from tcp://localhost:9999");
		println(s"writing into $httpServletURL");

		val sqlContext = spark.sqlContext;

		//tcp->HttpTextSink
		val lines = spark.readStream.
			format("socket").
			option("host", "localhost").
			option("port", 9999).
			load();

		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		val query = lines.writeStream
			.format(classOf[HttpStreamSinkProvider].getName)
			.option("httpServletUrl", httpServletURL)
			.option("topic", "topic-1")
			.start();

		query.awaitTermination();
	}

	def runAsReceiver(servletPath: String, httpPort: Int) {
		val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[4]")
			.getOrCreate();

		import spark.implicits._
		val kryoSerializer = new KryoSerializer(new SparkConf());

		//starts a http server with a receiver servlet
		val receiver = HttpStreamServer.start(kryoSerializer, servletPath, httpPort);
		receiver.withBuffer().addListener(new ObjectArrayPrinter()).createTopic[String]("topic-1");
	}

	def runAsSource(httpServletURL: String) {
		val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[4]")
			.getOrCreate();

		spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/");

		//HttpTextStream->map->console
		//HttpTextStream as a source stream
		val lines = spark.readStream.format(classOf[HttpStreamSourceProvider].getName)
			.option("httpServletUrl", httpServletURL)
			.option("topic", "topic-1").load();

		import spark.implicits._
		val words = lines.as[String].flatMap(_.split(" "));
		val wordCounts = words.groupBy("value").count();

		val query = wordCounts.writeStream.
			outputMode("complete").
			format("console").
			start();

		query.awaitTermination();
	}
}