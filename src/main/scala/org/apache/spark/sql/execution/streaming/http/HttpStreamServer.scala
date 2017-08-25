package org.apache.spark.sql.execution.streaming.http

import org.apache.spark.internal.Logging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletContextHandler

import javax.servlet.ServletConfig
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.eclipse.jetty.servlet.ServletHolder
import scala.collection.JavaConversions

class MissingRequiredRequestParameterException(paramName: String)
		extends RuntimeException(s"missing required request parameter: $paramName") {
}

class UnsupportedActionException(action: String)
		extends RuntimeException(s"unsupported action in HTTP request header: $action") {
}

object HttpStreamServer {
	def start(httpServletPath: String, httpPort: Int) = {
		val server = new HttpStreamServer(httpServletPath, httpPort);
		server.start;
		server;
	}
}

/**
 * configurable servlet which can be configured in your web.xml
 * example:
 * <servlet>
 *   <servlet-name>httpStreamServlet</servlet-name>
 *   <servlet-class>org.apache.spark.sql.execution.streaming.http.ConfigurableHttpStreamingServlet</servlet-class>
 *   <init-param>
 *     <param-name>handlerFactoryName</param-name>
 *     <param-value></param-value>
 *   </init-param>
 * </servlet>
 *
 * <servlet-mapping>
 *   <servlet-name>httpStreamServlet</servlet-name>
 *   <url-pattern>/servlet/stream</url-pattern>
 * </servlet-mapping>
 */
class ConfigurableHttpStreamingServlet extends AbstractHttpStreamingServlet {
	var actionsHandler: ActionsHandler = null;
	override def getActionsHandler(): ActionsHandler = actionsHandler;

	override def init(config: ServletConfig) {
		val handlerFactoryName = config.getInitParameter("handlerFactoryName");

		val factory = if (handlerFactoryName != null) {
			Class.forName(handlerFactoryName).newInstance().asInstanceOf[ActionsHandlerFactory];
		}
		else {
			new MemoryBufferAsReceiverFactory()
		};

		actionsHandler = factory.createInstance(
			JavaConversions.enumerationAsScalaIterator(config.getInitParameterNames)
				.map(name ⇒ (name, config.getInitParameter(name))).toMap);
	}
}

/**
 * responds http post requests
 */
abstract class AbstractHttpStreamingServlet extends HttpServlet with Logging {
	protected def getActionsHandler(): ActionsHandler;
	protected def getSerializerFactory(): SerializerFactory = SerializerFactory.DEFAULT;

	def getRequestHeader(request: HttpServletRequest, name: String, defaultValue: String) = {
		val value = request.getParameter(name);
		if (value != null)
			value;
		else
			defaultValue;
	}

	override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
		//1. parses HTTP request as a Map object
		//2. passes it to actionsHandler
		//3. retrieves an output Map and wrap it as HTTP response
		val serializer = getSerializerFactory().getSerializerInstance("kryo");
		val requestBody = serializer.deserializeStream(request.getInputStream).readObject().asInstanceOf[Map[String, Any]];

		val opt = requestBody.get("action");
		if (opt.isEmpty)
			throw new MissingRequiredRequestParameterException("action");

		val action = opt.get.asInstanceOf[String];
		val pf = getActionsHandler().listActionHandlerEntries(requestBody);
		val responseBody = {
			if (pf.isDefinedAt(action)) {
				pf.apply(action);
			}
			else {
				throw new UnsupportedActionException(action);
			}
		}

		serializer.serializeStream(response.getOutputStream).writeObject(responseBody);
	}

	override def destroy() {
		getActionsHandler().destroy();
	}
}

/**
 * a HttpStreamServer starts a HTTP server (using Jetty)
 */
private[streaming] class HttpStreamServer(httpServletPath: String, httpPort: Int) extends Logging {
	private val server = new Server(httpPort);
	private var actionsHandler: ActionsHandler = new NullActionsHandler();
	private val httpStreamServlet = new AbstractHttpStreamingServlet() {
		override def getActionsHandler = actionsHandler;
	};

	def start() = {
		val context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);
		//add servlet
		context.addServlet(new ServletHolder(httpStreamServlet), httpServletPath);
		server.start();

		super.logInfo(s"start on http://localhost:$httpPort$httpServletPath");
	}

	/**
	 * updates inner actionsHandler
	 */
	def withActionsHandler[T <: ActionsHandler](actionsHandler: T): T = {
		this.actionsHandler = actionsHandler;
		actionsHandler;
	}

	def withBuffer(): MemoryBufferAsReceiver = {
		withActionsHandler(new MemoryBufferAsReceiver());
	}

	def withKafka(bootstrapServers: String): KafkaAsReceiver = {
		withActionsHandler(new KafkaAsReceiver(bootstrapServers));
	}

	def stop() = {
		httpStreamServlet.destroy();
		if (server != null)
			server.stop();
	}
}