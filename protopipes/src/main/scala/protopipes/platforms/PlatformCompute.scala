package protopipes.platforms

import protopipes.computations.{Mapper, PairwiseComposer, Reducer}
import protopipes.connectors.Connector.Id
import protopipes.connectors.{Connector, Status}
import protopipes.curators.{ErrorCurator, ProvenanceCurator}
import protopipes.data.{BasicIdentity, Identifiable, Identity}
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

  /*
  def map(upstreamConnector: Connector[Input], mapper: Mapper[Input, Output], outputMap: DataStore[Output]): Unit = {
    upstreamConnector.retrieveUp() foreach {
      input => tryComputeThenStore(upstreamConnector, mapper, input, outputMap)
    }
  } */

  def tryCompute(upstreamConnector: Connector[Input], mapper: Mapper[Input, Output], input: Input, outputMap: DataStore[Output]): List[Output] = {
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
        List()
      }
    }
  }

}

trait ComputesReduce[Input <: Identifiable[Input], Output <: Identifiable[Output]] {

  var reducerOpt: Option[Reducer[Input,Output]] = None

  def setReducer(reducer: Reducer[Input,Output]): Unit = reducerOpt = Some(reducer)

  def getReducer(): Reducer[Input,Output] = reducerOpt match {
    case Some(reducer) => reducer
    case None => {
      // TODO: Throw exception
      ???
    }
  }

  def tryGroup(upstreamConnector: Connector[Input], // errorCurator: ErrorCurator[Input],
               reducer: Reducer[Input,Output], input: Input): Option[Identity[Output]] = {
     try {
       val outputId = reducer.groupBy(input)
       upstreamConnector.reportUp(Status.Done, input)
       Some(outputId)
     } catch {
       case ex: Exception => {
         // errorCurator.reportError(input, ex, Some("Reducer \'groupBy\' operation failed."))
         None
       }
     }
  }

  def tryFold(upstreamConnector: Connector[Input], // errorCurator: ErrorCurator[Input], provenanceCurator: ProvenanceCurator[Input,Output],
              reducer: Reducer[Input,Output], input: Input, output: Output): Output = {
    try {
      val newOutput = reducer.fold(input, output)
      upstreamConnector.reportUp(Status.Done, input)
      // provenanceCurator.reportProvenance(input, output)
      newOutput
    } catch {
      case ex: Exception => {
        // errorCurator.reportError(input, ex, Some("Reducer \'fold\' operation failed."))
        output
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

  def tryFilterAndCompose(pairConnector: Connector[protopipes.data.Pair[InputL,InputR]], composer: PairwiseComposer[InputL,InputR,Output],
                          pair: protopipes.data.Pair[InputL,InputR], outputMap: DataStore[Output]): Option[Output] = {
    try {
      val output = if(composer.filter(pair.left, pair.right)) {
        val output = composer.compose(pair.left,pair.right)
        outputMap.put(output)
        Some(output)
      } else None
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