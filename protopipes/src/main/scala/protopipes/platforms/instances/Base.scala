package protopipes.platforms.instances

import protopipes.connectors.Connector.Id
import protopipes.configurations.{PipeConfig, PlatformBuilder}
import protopipes.computations.Mapper
import protopipes.connectors.Status
import protopipes.data.Identifiable
import protopipes.platforms.UnaryPlatform
import protopipes.store.DataStore
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class MapperPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]] extends UnaryPlatform[Input, Output] {

  var mapperOpt: Option[Mapper[Input,Output]] = None

  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder, mapper: Mapper[Input,Output]): Unit = {
    init(conf, inputMap, outputMap, builder)
    mapperOpt = Some(mapper)
  }

  override def run(): Unit = mapperOpt match {
    case Some(mapper) => getUpstreamConnector().retrieveUp() foreach {
      pair => tryComputeThenStore(mapper, pair, getOutputMap())
    }
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  override def terminate(): Unit = { }

  def tryComputeThenStore(mapper: Mapper[Input, Output], input: Input, outputMap: DataStore[Output]): List[Output] = {
    try {
      val outputs = mapper.getOp(input).map(
        output => {
          outputMap.put(output)
          output
        }
      )
      getUpstreamConnector().reportUp(Status.Done, input)
      outputs
    } catch {
      case ex:Exception => {
        // Compute exception occurred, log this in error store
        // errors.put(input.identity, GeneralErrorSummary(ex))
        List()
      }
    }
  }

}
