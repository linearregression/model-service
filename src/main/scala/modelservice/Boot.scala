package modelservice

import modelservice.api.{Api, ModelServiceActor}
import modelservice.core.{ModelServiceActors, InitCore}
import modelservice.web.Web

object Boot extends App with InitCore with Api with Web with ModelServiceActors