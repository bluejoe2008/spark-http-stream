package org.apache.spark.sql.execution.streaming

import java.sql.Timestamp

import scala.collection.mutable.ListBuffer

import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

import Params.map2Params
import java.util.concurrent.atomic.AtomicBoolean

/**
 * reads data from a HttpStreamServer
 */
class HttpStreamSource(sqlContext: SQLContext, httpServletUrl: String, topic: String, includesTimestamp: Boolean, timestampColumnName: String) extends Source with Logging {
	override def schema: StructType = schema2;
	var producerOffset: LongOffset = new LongOffset(-1);
	var consumerOffset = -1;
	val streamBuffer = ListBuffer[Row]();
	val kryoSerializer = new KryoSerializer(sqlContext.sparkContext.conf);
	val client = HttpStreamClient.connect(httpServletUrl, kryoSerializer);
	val (subscriberId, schema1) = client.subscribe(topic);
	val schema2 = SchemaUtils.wrapSchema(schema1, includesTimestamp, timestampColumnName);
	val flagStop = new AtomicBoolean(false);

	//this thread move data from HttpTextReceiver to local buffer periodically
	val readerThread = new Thread(s"http-stream-reader($httpServletUrl)") {
		override def run() {
			while (!flagStop.get) {
				val list: Array[(Timestamp, Row)] =
					try {
						client.fetchStream(subscriberId);
					}
					catch {
						case e: Throwable ⇒
							val msg = e.getMessage;
							logWarning(s"failed to fetch http stream: $msg");
							Array[(Timestamp, Row)]();
					}
				this.synchronized {
					if (!list.isEmpty) {
						producerOffset += list.size;
						streamBuffer ++= list.map { t2 ⇒ (Row.fromSeq(t2._2.toSeq :+ t2._1)) };
					}
				}

				Thread.sleep(1);
			}
		}
	}

	readerThread.start();

	override def getOffset: Option[Offset] = synchronized {
		if (producerOffset.offset == -1) {
			None
		}
		else {
			Some(producerOffset)
		}
	}

	//(start, end], 0-based here
	override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
		val iStart = LongOffset.convert(start.getOrElse(LongOffset(-1))).getOrElse(LongOffset(-1)).offset;
		val iEnd = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset;
		//slice(from, to) return [from, to)
		val slice = this.synchronized {
			streamBuffer.slice(iStart.toInt - consumerOffset, iEnd.toInt - consumerOffset);
		}

		val rdd = sqlContext.sparkContext.parallelize(slice);
		val df = sqlContext.createDataFrame(rdd, schema);
		df;
	}

	override def commit(end: Offset) {
		//[0, end] lines have been eaten
		//discards this area
		val optEnd = LongOffset.convert(end);
		optEnd match {
			case Some(LongOffset(iOffset: Long)) ⇒ if (iOffset >= 0) {
				this.synchronized {
					streamBuffer.trimStart(iOffset.toInt - consumerOffset);
					consumerOffset = iOffset.toInt;
				}
			}

			case _ ⇒ throw new WrongArgumentException("end", end);
		}
	}

	override def stop() {
		client.unsubscribe(subscriberId);
		flagStop.set(true);
	}
}

class SchemaNotProvidedException extends RuntimeException {
}

class HttpStreamSourceProvider extends StreamSourceProvider with DataSourceRegister with Logging {
	override def shortName() = "httpStream";

	override def sourceSchema(
		sqlContext: SQLContext,
		schema: Option[StructType],
		providerName: String,
		parameters: Map[String, String]): (String, StructType) = {
		val kryoSerializer = new KryoSerializer(sqlContext.sparkContext.conf);
		val httpServletUrl = parameters.getRequiredString("httpServletUrl");
		val topic = parameters.getRequiredString("topic");
		val client = HttpStreamClient.connect(httpServletUrl, kryoSerializer);
		val schema = client.fetchSchema(topic);

		(parameters.getRequiredString("topic"), SchemaUtils.wrapSchema(schema, parameters.getBool("includesTimestamp", false), parameters.getString("timestampColumnName", "_TIMESTAMP_")));
	}

	override def createSource(
		sqlContext: SQLContext,
		metadataPath: String,
		schema: Option[StructType],
		providerName: String,
		parameters: Map[String, String]): Source = new HttpStreamSource(sqlContext, parameters.getRequiredString("httpServletUrl"), parameters.getRequiredString("topic"), parameters.getBool("includesTimestamp", false), parameters.getString("timestampColumnName", "_TIMESTAMP_"));
}