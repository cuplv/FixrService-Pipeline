package bigglue.platforms.instances

import bigglue.connectors.Connector.Id
import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.computations.Mapper
import bigglue.connectors.Status
import bigglue.data.Identifiable
import bigglue.platforms.UnaryPlatform
import bigglue.store.DataStore
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
