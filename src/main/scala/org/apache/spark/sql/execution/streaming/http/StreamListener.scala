package org.apache.spark.sql.execution.streaming.http

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

/**
 * @author bluejoe2008@gmail.com
 * listens on data arrivals to HttpStreamServer
 */
trait ObjectArrayListener {
	def onArrive(topic: String, batchId: Long, objects: Array[Row]);
}

/**
 * collects data in local memory buffer
 */
class ObjectArrayCollector extends ObjectArrayListener {
	val buffer = ArrayBuffer[Row]();

	def onArrive(topic: String, batchId: Long, objects: Array[Row]) {
		buffer ++= objects;
	}

	def get = buffer.toArray;
}

/**
 * prints data while arriving
 */
class ObjectArrayPrinter extends ObjectArrayListener {
	def onArrive(topic: String, batchId: Long, objects: Array[Row]) {
		println(s"++++++++topic=$topic,batch=$batchId++++++++");
		objects.foreach(println);
	}
}
