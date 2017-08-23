package org.apache.spark.sql.execution.streaming

import org.apache.commons.io.IOUtils
import org.apache.spark.serializer.KryoSerializer
import java.io.InputStream
import com.esotericsoftware.kryo.io.Input
import java.io.ByteArrayOutputStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType

object HttpStreamUtils {
	def serialize(kryoSerializer: KryoSerializer, data: Any): Array[Byte] = {
		val bao = new ByteArrayOutputStream();
		val kryo = kryoSerializer.newKryo();
		val output = kryoSerializer.newKryoOutput();
		output.setOutputStream(bao);
		kryo.writeClassAndObject(output, data);
		output.close();
		bao.toByteArray();
	}

	def deserialize(kryoSerializer: KryoSerializer, is: InputStream): Any = {
		deserialize(kryoSerializer, IOUtils.toByteArray(is));
	}

	def deserialize(kryoSerializer: KryoSerializer, bytes: Array[Byte]): Any = {
		val kryo = kryoSerializer.newKryo();
		val input = new Input();
		input.setBuffer(bytes);
		kryo.readClassAndObject(input);
	}
}

object Params {
	/**
	 * convert a map to a Params
	 */
	implicit def map2Params(map: Map[String, String]) = {
		new Params(map);
	}
}

class WrongArgumentException(name: String, value: Any) extends RuntimeException(s"wrong argument: $name=$value") {
}

class ArgumentNotSetException(map: Map[String, String], paramName: String) extends RuntimeException(s"argument not configured: $paramName, all parameters=$map") {
}

object SchemaUtils {
	def wrapSchema(schema: StructType, includesTimestamp: Boolean, timestampColumnName: String = "_TIMESTAMP_"): StructType = {
		if (!includesTimestamp)
			schema;
		else
			StructType(schema.fields.toSeq :+ StructField(timestampColumnName, TimestampType, false));
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
			throw new ArgumentNotSetException(map, paramName);
		}
		else {
			parse(opt.get);
		}
	}
}