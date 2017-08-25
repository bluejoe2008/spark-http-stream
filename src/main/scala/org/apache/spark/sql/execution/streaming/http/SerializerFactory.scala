package org.apache.spark.sql.execution.streaming.http

import java.nio.ByteBuffer
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.serializer.SerializationStream
import java.io.OutputStream
import java.io.InputStream
import scala.reflect.ClassTag
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.serializer.KryoSerializer

/**
 * @author bluejoe2008@gmail.com
 */
object SerializerFactory {
	val DEFAULT = new SerializerFactory {
		override def getSerializerInstance(serializerName: String): SerializerInstance = {
			serializerName.toLowerCase() match {
				case "kryo" ⇒
					new KryoSerializer(new SparkConf()).newInstance();
				case "java" ⇒
					new JavaSerializer(new SparkConf()).newInstance();
				case _ ⇒ throw new InvalidSerializerNameException(serializerName);
			}
		}
	}
}

trait SerializerFactory {
	def getSerializerInstance(serializerName: String): SerializerInstance;
}