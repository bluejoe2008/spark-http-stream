package org.apache.spark.sql.execution.streaming.http

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

/**
 * @author bluejoe2008@gmail.com
 * listens on data arrivals to HttpStreamServer
 */
trait StreamListener {
	def onArrive(topic: String, objects: Array[RowEx]);
}

/**
 * collects data in a local memory buffer
 */
class StreamCollector extends StreamListener {
	val buffer = ArrayBuffer[RowEx]();

	def onArrive(topic: String, objects: Array[RowEx]) {
		buffer ++= objects;
	}

	def get = buffer.toArray;
}

/**
 * prints data while arriving
 */
class StreamPrinter extends StreamListener {
	def onArrive(topic: String, objects: Array[RowEx]) {
		println(s"++++++++topic=$topic++++++++");
		objects.foreach(println);
	}
}
