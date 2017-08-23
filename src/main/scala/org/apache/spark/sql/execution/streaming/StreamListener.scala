package org.apache.spark.sql.execution.streaming

import com.esotericsoftware.kryo.io.Input
import org.apache.spark.serializer.KryoSerializer
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.Row

trait ObjectArrayListener {
	def onReceived(topic: String, batchId: Long, objects: Array[Row]);
}

class ObjectArrayCollector extends ObjectArrayListener {
	val buffer = ArrayBuffer[Row]();

	def onReceived(topic: String, batchId: Long, objects: Array[Row]) {
		buffer ++= objects;
	}

	def get = buffer.toArray;
}

class ObjectArrayPrinter extends ObjectArrayListener {
	def onReceived(topic: String, batchId: Long, objects: Array[Row]) {
		println(s"++++++++topic=$topic,batch=$batchId++++++++");
		objects.foreach(println);
	}
}
