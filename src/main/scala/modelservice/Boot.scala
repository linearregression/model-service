package modelservice

import modelservice.api.{Api, ModelServiceActor}
import modelservice.core.prediction.PredictionActorSet
import modelservice.core.{CoreActorSet, InitCore}
import modelservice.web.Web

object Boot extends App with InitCore with Api with Web with CoreActorSet with PredictionActorSet