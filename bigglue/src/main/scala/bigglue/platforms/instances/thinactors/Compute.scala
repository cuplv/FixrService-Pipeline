package bigglue.platforms.instances.thinactors

import bigglue.connectors.Status
import bigglue.data.{Identifiable, BasicIdentity}
import bigglue.store.DataMap

import scala.util.Random

/**
  * Created by edmundlam on 8/11/17.
  */


class ThinActorMapperPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorUnaryPlatform[Input, Output] { // with ComputesMap[Input, Output] {

  /*
  override def run(): Unit = {
    val mapper = getMapper()
    val upstreamConnector = getUpstreamConnector()
    val outputMap = getOutputMap()
    getInputs() foreach {
      input => tryCompute(upstreamConnector, mapper, input, outputMap)
    }
  } */

  override def toString: String = "Thin-Actor-Mapper-Platform"

}

class ThinActorReducerPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorUnaryPlatform[Input, Output] { // } with ComputesReduce[Input,Output] {

   /*
   override def run(): Unit = {
     val reducer = getReducer()
     val upstreamConnector = getUpstreamConnector()
     val errorCurator = getErrorCurator()
     val provenanceCurator = getProvenanceCurator()
     val outputMap = getOutputMap().asInstanceOf[DataMap[Identity[Output],Output]]

     var mergals = Map.empty[Identity[Output],Seq[Input]]
     getInputs() foreach {
       input => {
         tryGroup(upstreamConnector, reducer, input) match {
           case Some(outputId) => mergals = mergals + (outputId -> (mergals.getOrElse (outputId, Seq.empty[Input]) :+ input) )
           case None => // error logged by tryGroup. Ignoring.
         }
       }
     }

     mergals.toList foreach {
       pair => {
         val output = outputMap.getOrElse(pair._1, reducer.zero)
         val newOutput = pair._2.foldRight(output) { (i,o) => tryFold(upstreamConnector, reducer, i, o) }
         outputMap.put(newOutput)
       }
     }

   } */

}

class ThinActorPairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR],Output <: Identifiable[Output]]
(name: String = ThinActorPlatform.NAME + Random.nextInt(99999)) extends ThinActorBinaryPlatform[InputL,InputR,Output] { // with ComputesPairwiseCompose[InputL,InputR,Output] {

  /*
  override def run(): Unit = {
    val composer = getComposer()
    val pairConnector = getPairConnector()
    val outputMap = getOutputMap()
    val inputs = getInputs()
    inputs._3 foreach {
      tryFilterAndCompose(pairConnector, composer, _, outputMap)
    }
    getUpstreamLConnector().reportUp(Status.Done,inputs._1)
    getUpstreamRConnector().reportUp(Status.Done,inputs._2)
  } */

}
