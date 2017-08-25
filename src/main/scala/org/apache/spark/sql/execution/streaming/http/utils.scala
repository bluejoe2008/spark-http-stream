package org.apache.spark.sql.execution.streaming.http

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.SparkConf
import org.apache.commons.io.IOUtils
import org.apache.spark.serializer.KryoSerializer
import java.io.InputStream
import com.esotericsoftware.kryo.io.Input
import java.io.ByteArrayOutputStream

class WrongArgumentException(name: String, value: Any)
		extends RuntimeException(s"wrong argument: $name=$value") {
}

class MissingRequiredArgumentException(map: Map[String, String], paramName: String)
		extends RuntimeException(s"missing required argument: $paramName, all parameters=$map") {
}

class InvalidSerializerNameException(serializerName: String)
		extends RuntimeException(s"invalid serializer name: $serializerName") {
}

object SchemaUtils {
	def buildSchema(schema: StructType, includesTimestamp: Boolean, timestampColumnName: String = "_TIMESTAMP_"): StructType = {
		if (!includesTimestamp)
			schema;
		else
			StructType(schema.fields.toSeq :+ StructField(timestampColumnName, TimestampType, false));
	}
}

object Params {
	/**
	 * convert a map to a Params object
	 */
	implicit def map2Params(map: Map[String, String]) = {
		new Params(map);
	}
}

/**
 * Params provides an enhanced Map which supports getInt()/getBool()... operations
 */
class Params(map: Map[String, String]) {
	def getInt(paramName: String, defaultValue: Int) = getParamWithDefault(paramName, defaultValue, { _.toInt });
	def getBool(paramName: String, defaultValue: Boolean = false) = getParamWithDefault(paramName, defaultValue, { _.toBoolean });
	def getString(paramName: String, defaultValue: String) = getParamWithDefault(paramName, defaultValue, { x ⇒ x });

	def getRequiredInt(paramName: String) = getRequiredParam(paramName, { _.toInt });
	def getRequiredBool(paramName: String) = getRequiredParam(paramName, { _.toBoolean });
	def getRequiredString(paramName: String) = getRequiredParam(paramName, { x ⇒ x });

	private def getParamWithDefault[T](paramName: String, defaultValue: T, parse: (String ⇒ T)) = {
		val opt = map.get(paramName);
		if (opt.isEmpty) {
			defaultValue;
		}
		else {
			try {
				parse(opt.get);
			}
			catch {
				case _: Throwable ⇒ defaultValue;
			}
		}
	}

	private def getRequiredParam[T](paramName: String, parse: (String ⇒ T)) = {
		val opt = map.get(paramName);
		if (opt.isEmpty) {
			throw new MissingRequiredArgumentException(map, paramName);
		}
		else {
			parse(opt.get);
		}
	}
}

/**
 * @deprecated use [[SerializerFactory]]
 */
object KryoSerializerUtils {
	val kryoSerializer = new KryoSerializer(new SparkConf());

	/**
	 * serialize an object into a byte array
	 */
	def serialize(data: Any): Array[Byte] = {
		val bao = new ByteArrayOutputStream();
		val kryo = kryoSerializer.newKryo();
		val output = kryoSerializer.newKryoOutput();
		output.setOutputStream(bao);
		kryo.writeClassAndObject(output, data);
		output.close();
		bao.toByteArray();
	}

	/**
	 * deserialize an object from an input stream
	 */
	def deserialize(is: InputStream): Any = {
		deserialize(IOUtils.toByteArray(is));
	}

	/**
	 * deserialize an object from a byte array
	 */
	def deserialize(bytes: Array[Byte]): Any = {
		val kryo = kryoSerializer.newKryo();
		val input = new Input();
		input.setBuffer(bytes);
		kryo.readClassAndObject(input);
	}
}