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
  IO(Http) ! Http.Bind(modelService, interface = "0.0.0.0", port = 8080)
}
