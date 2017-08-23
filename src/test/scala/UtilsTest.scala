import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HttpStreamUtils
import org.junit.Assert
import org.junit.Test

class UtilsTest {
	@Test
	def test() {
		val d1 = new Date(30000);

		val kryoSerializer = new KryoSerializer(new SparkConf());
		val bytes = HttpStreamUtils.serialize(kryoSerializer, d1);
		val d2 = HttpStreamUtils.deserialize(kryoSerializer, bytes);
		Assert.assertEquals(d1, d2);
	}

	@Test
	def test2() {
		val spark = SparkSession.builder.master("local[4]")
			.getOrCreate();

		val d1 = new Date(30000);
		val sqlContext = spark.sqlContext;
		import sqlContext.implicits._

		val ds = sqlContext.createDataset(Seq[(Int, Date)]((1, d1)));
		val d2 = ds.collect()(0)._2;

		Assert.assertEquals(d1, d2);
	}
}
