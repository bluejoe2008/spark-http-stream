package org.apache.spark.sql.execution.streaming.http

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

/**
 * @author bluejoe2008@gmail.com
 * listens on data arrivals to HttpStreamServer
 */
trait ObjectArrayListener {
	def onArrive(topic: String, objects: Array[RowEx]);
}

/**
 * collects data in local memory buffer
 */
class ObjectArrayCollector extends ObjectArrayListener {
	val buffer = ArrayBuffer[RowEx]();

	def onArrive(topic: String, objects: Array[RowEx]) {
		buffer ++= objects;
	}

	def get = buffer.toArray;
}

/**
 * prints data while arriving
 */
class ObjectArrayPrinter extends ObjectArrayListener {
	def onArrive(topic: String, objects: Array[RowEx]) {
		println(s"++++++++topic=$topic++++++++");
		objects.foreach(println);
	}
}
