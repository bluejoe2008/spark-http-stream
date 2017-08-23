package org.apache.spark.sql.execution.streaming

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.KryoSerializer
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder

import com.esotericsoftware.kryo.io.Input

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class MissingRequiredParameterException(paramName: String)
		extends RuntimeException(s"missing required request parameter: $paramName") {
}

class UnsupportedActionException(action: String)
		extends RuntimeException(s"unsupported action in HTTP request header: $action") {
}

object HttpStreamServer {
	def start(kryoSerializer: KryoSerializer, httpServletPath: String, httpPort: Int) = {
		val server = new HttpStreamServer(kryoSerializer, httpServletPath, httpPort);
		server.start;
		server;
	}
}

/**
 * a HttpStreamServer starts a HTTP server (using Jetty)
 */
private[streaming] class HttpStreamServer(val kryoSerializer: KryoSerializer, httpServletPath: String, httpPort: Int) extends Logging {
	val server = new Server(httpPort);
	val actionsHandlerServlet = new ActionsHandlerServlet(new LazyActionsHandler());

	class ActionsHandlerServlet(var handler: ActionsHandler) extends HttpServlet with Logging {
		def setActionsHandler(handler: ActionsHandler) = { this.handler = handler; }

		override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
			val requestBody = HttpStreamUtils.deserialize(kryoSerializer, request.getInputStream).asInstanceOf[Map[String, Any]];
			super.logDebug(s"request: $requestBody");

			val opt = requestBody.get("action");
			if (opt.isEmpty)
				throw new MissingRequiredParameterException("action");

			val action = opt.get.asInstanceOf[String];
			val pf = handler.listActionHandlerEntries(requestBody);
			val responseBody = {
				if (pf.isDefinedAt(action)) {
					pf.apply(action);
				}
				else {
					throw new UnsupportedActionException(action);
				}
			}

			super.logDebug(s"response: $responseBody");
			val bytes = HttpStreamUtils.serialize(kryoSerializer, responseBody);
			response.getOutputStream.write(bytes);
		}

		override def destroy() {
			handler.destroy();
		}
	}

	def start() = {
		val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);
		//add servlet
		context.addServlet(new ServletHolder(actionsHandlerServlet), httpServletPath);
		server.start();

		super.logInfo(s"start on http://localhost:$httpPort$httpServletPath");
	}

	def withActionHandler[T <: ActionsHandler](actionHandler: T): T = {
		actionsHandlerServlet.setActionsHandler(actionHandler);
		actionHandler;
	}

	def withBuffer(): BufferedReceiver = {
		withActionHandler(new BufferedReceiver());
	}

	def withKafka(bootstrapServers: String): KafkaAsReceiver = {
		withActionHandler(new KafkaAsReceiver(bootstrapServers));
	}

	def stop() = {
		actionsHandlerServlet.destroy();
		if (server != null)
			server.stop();
	}
}