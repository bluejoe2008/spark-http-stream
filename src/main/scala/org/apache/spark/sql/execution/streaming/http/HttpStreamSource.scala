package org.apache.spark.sql.execution.streaming.http

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import org.apache.spark
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.StreamSourceProvider
import org.apache.spark.sql.types.StructType

import Params.map2Params

/**
 * @author bluejoe2008@gmail.com
 * reads data from a HttpStreamServer
 */
class HttpStreamSource(sqlContext: SQLContext, httpServletUrl: String, topic: String, msFetchPeriod: Int, includesTimestamp: Boolean, timestampColumnName: String) extends Source with Logging {
	override def schema: StructType = schema2;
	var producerOffset: LongOffset = new LongOffset(-1);
	var consumerOffset = -1;
	val streamBuffer = ListBuffer[RowEx]();
	val consumer = HttpStreamClient.connect(httpServletUrl);
	val (subscriberId, schema1) = consumer.subscribe(topic);
	val schema2 = SchemaUtils.buildSchema(schema1, includesTimestamp, timestampColumnName);
	val flagStop = new AtomicBoolean(false);

	//this thread move data from HttpTextReceiver to local buffer periodically
	val readerThread = new Thread(s"http-stream-reader($httpServletUrl)") {
		override def run() {
			while (!flagStop.get) {
				val list: Array[RowEx] = {
					try {
						consumer.fetchStream(subscriberId);
					}
					catch {
						case e: Throwable ⇒
							val msg = e.getMessage;
							logWarning(s"failed to fetch http stream: $msg");
							Array[RowEx]();
					}
				}

				this.synchronized {
					if (!list.isEmpty) {
						producerOffset += list.size;
						streamBuffer ++= list;
					}
				}

				Thread.sleep(msFetchPeriod);
			}
		}
	}

	readerThread.start();

	override def getOffset: Option[Offset] = {
		val po = this.synchronized { producerOffset };
		if (po.offset == -1) {
			None
		}
		else {
			Some(po)
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

		val slice2 = {
			if (includesTimestamp)
				slice.map(_.withTimestamp());
			else
				slice.map(_.originalRow);
		}

		val rdd = sqlContext.sparkContext.parallelize(slice2);
		sqlContext.createDataFrame(rdd, schema);
	}

	override def commit(end: Offset) {
		//discards [0, end] lines, since they have been consumed
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
		consumer.unsubscribe(subscriberId);
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
		val httpServletUrl = parameters.getRequiredString("httpServletUrl");
		val topic = parameters.getRequiredString("topic");
		val client = HttpStreamClient.connect(httpServletUrl);
		val schema = client.fetchSchema(topic);

		(parameters.getRequiredString("topic"),
			SchemaUtils.buildSchema(schema,
				parameters.getBool("includesTimestamp", false),
				parameters.getString("timestampColumnName", "_TIMESTAMP_")));
	}

	override def createSource(
		sqlContext: SQLContext,
		metadataPath: String,
		schema: Option[StructType],
		providerName: String,
		parameters: Map[String, String]): Source =
		new HttpStreamSource(sqlContext,
			parameters.getRequiredString("httpServletUrl"),
			parameters.getRequiredString("topic"),
			parameters.getInt("msFetchPeriod", 1),
			parameters.getBool("includesTimestamp", false),
			parameters.getString("timestampColumnName", "_TIMESTAMP_"));
}