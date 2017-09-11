package bigglue.examples

import bigglue.computations.Mapper
import bigglue.connectors.Status.Status
import bigglue.connectors.instances.{ActorConnector, IncrTrackerJobQueue}
import bigglue.data.{I, Identifiable, BasicIdentity}
import bigglue.platforms.UnaryPlatform
import bigglue.store.DataMultiMap
import bigglue.store.instances.InMemDataMultiMap

/**
  * Created by edmundlam on 8/9/17.
  */
object Probe {

  def extractStatusMap[Input <: Identifiable[Input], Output <: Identifiable[Output]](mapper: Mapper[Input,Output]): DataMultiMap[Status,Input] = {
    mapper.platformOpt.get.asInstanceOf[UnaryPlatform[Input,Input]].getUpstreamConnector()
      .asInstanceOf[ActorConnector[Input]].innerConnector.asInstanceOf[IncrTrackerJobQueue[Input]].statusMap
  }

}
