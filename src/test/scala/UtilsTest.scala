import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.InputStream
import org.apache.commons.io.IOUtils
import com.esotericsoftware.kryo.io.Input
import org.apache.spark.sql.execution.streaming.http.KryoSerializerUtils

class UtilsTest {
	@Test
	def testKryoSerDe() {
		val d1 = new Date(30000);
		val bytes = KryoSerializerUtils.serialize(d1);
		val d2 = KryoSerializerUtils.deserialize(bytes);
		Assert.assertEquals(d1, d2);

		val d3 = Map('x' -> Array("aaa", "bbb"), 'y' -> Array("ccc", "ddd"));
		println(d3);
		val bytes2 = KryoSerializerUtils.serialize(d3);
		val d4 = KryoSerializerUtils.deserialize(bytes2).asInstanceOf[Map[String, Any]];
		println(d4);
	}

	@Test
	def testEncoderSchema() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		val sqlContext = spark.sqlContext;
		import sqlContext.implicits._
		import org.apache.spark.sql.catalyst.encoders.encoderFor
		val schema1 = encoderFor[String].schema;
		val schema2 = encoderFor[(String)].schema;
		val schema3 = encoderFor[((String))].schema;

		Assert.assertEquals(schema1, schema2);
		Assert.assertEquals(schema1, schema3);
	}

	@Test
	def testDateInTuple() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();
		val sqlContext = spark.sqlContext;
		import sqlContext.implicits._

		val d1 = new Date(30000);
		val ds = sqlContext.createDataset(Seq[(Int, Date)]((1, d1)));
		val d2 = ds.collect()(0)._2;

		//NOTE: d1!=d2, maybe a bug
		println(d1.equals(d2));
	}
}
