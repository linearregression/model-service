package modelservice.web

import akka.io.IO
import modelservice.core.Core
import modelservice.api.Api
import spray.can.Http

/**
 * Web
 */
trait Web {
  this: Api with Core =>
  {
    val interface = System.getProperty("http.interface") match {
      case x: String => x
      case _ => "0.0.0.0"
    }
    val port = System.getProperty("http.port") match {
      case x: String => {
        try {
          x.toInt
        } catch {
          case e: Exception => 8080
        }
      }
      case _ => 8080
    }
    IO(Http) ! Http.Bind(modelService, interface=interface, port=port)
  }
}
