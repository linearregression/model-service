package modelservice

import modelservice.api.{Api, ModelServiceActor}
import modelservice.core.InitCore
import modelservice.web.Web

object Boot extends App with InitCore with Api with Web