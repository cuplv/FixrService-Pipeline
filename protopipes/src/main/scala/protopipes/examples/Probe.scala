package protopipes.examples

import protopipes.computations.Mapper
import protopipes.connectors.Status.Status
import protopipes.connectors.instances.{ActorConnector, IncrTrackerJobQueue}
import protopipes.data.{I, Identifiable, Identity}
import protopipes.platforms.UnaryPlatform
import protopipes.store.DataMultiMap
import protopipes.store.instances.InMemDataMultiMap

/**
  * Created by edmundlam on 8/9/17.
  */
object Probe {

  def extractStatusMap[Input <: Identifiable[Input], Output <: Identifiable[Output]](mapper: Mapper[Input,Output]): DataMultiMap[Status,Input] = {
    mapper.platformOpt.get.asInstanceOf[UnaryPlatform[Input,Input]].getUpstreamConnector()
      .asInstanceOf[ActorConnector[Input]].innerConnector.asInstanceOf[IncrTrackerJobQueue[Input]].statusMap
  }

}
