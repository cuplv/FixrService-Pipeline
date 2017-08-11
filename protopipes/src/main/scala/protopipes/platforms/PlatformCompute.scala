package protopipes.platforms

import protopipes.computations.{Mapper, PairwiseComposer}
import protopipes.connectors.Connector.Id
import protopipes.connectors.{Connector, Status}
import protopipes.data.Identifiable
import protopipes.store.DataStore

/**
  * Created by edmundlam on 8/8/17.
  */
trait ComputesMap[Input <: Identifiable[Input], Output <: Identifiable[Output]] {

  var mapperOpt: Option[Mapper[Input,Output]] = None

  def setMapper(mapper: Mapper[Input,Output]): Unit = mapperOpt = Some(mapper)

  def getMapper(): Mapper[Input, Output] = mapperOpt match {
    case Some(mapper) => mapper
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def map(upstreamConnector: Connector[Input], mapper: Mapper[Input, Output], outputMap: DataStore[Output]): Unit = {
    upstreamConnector.retrieveUp() foreach {
      input => tryComputeThenStore(upstreamConnector, mapper, input, outputMap)
    }
  }

  def tryComputeThenStore(upstreamConnector: Connector[Input], mapper: Mapper[Input, Output], input: Input, outputMap: DataStore[Output]): List[Output] = {
    try {
      val outputs = mapper.compute(input).map(
        output => {
          outputMap.put(output)
          output
        }
      )
      upstreamConnector.reportUp(Status.Done, input )
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

trait ComputesPairwiseCompose[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]] {

  var composerOpt: Option[PairwiseComposer[InputL,InputR,Output]] = None

  def setPairwiseComposer(composer: PairwiseComposer[InputL,InputR,Output]): Unit = composerOpt = Some(composer)

  def getComposer(): PairwiseComposer[InputL,InputR,Output] = composerOpt match {
    case Some(composer) => composer
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def tryComposeThenStore(pairConnector: Connector[protopipes.data.Pair[InputL,InputR]], composer: PairwiseComposer[InputL,InputR,Output],
                          pair: protopipes.data.Pair[InputL,InputR], outputMap: DataStore[Output]): Option[Output] = {
    try {
      val output = if(composer.filter(pair.left, pair.right)) Some(composer.compose(pair.left,pair.right)) else None
      pairConnector.reportUp(Status.Done, pair)
      output
    } catch {
      case ex: Exception => {
        // TODO Log error
        None
      }
    }
  }

}