package bigglue.computations

import com.typesafe.config.Config
import bigglue.checkers.{BinaryChecker, UnaryChecker}
import bigglue.configurations._
import bigglue.data.Identifiable
import bigglue.exceptions.NotInitializedException
import bigglue.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import bigglue.store.DataStore

import scala.util.Random

/**
  * Created by edmundlam on 8/14/17.
  */

/**
  * This is the superclass for [[Mapper]], [[Reducer]], and [[PairwiseComposer]].
  * In short, computations take inputs in and then compute on them using some algorithm to create outputs.
  */
abstract class Computation extends ConfigBuildsPlatform {

  var platformOpt: Option[Platform] = None
  var configOption: ConfOpt = DefaultOpt
  var trueVersionOpt: Option[String] = None

  val versionOpt: Option[String] = None

  def toStep(conf: PipeConfig, step: String): PipeConfig = {
    try {
      val stepConf = conf.typeSafeConfig.getConfig(Constant.BIGGLUE).getConfig(step)
      PipeConfig.resolveOptions(conf, ConfOpt.typesafeConfig(stepConf))
    } catch{
      case e: Exception => conf
    }
  }

  def init(config: PipeConfig, platform: Platform): Unit = platformOpt match {
    case None => {
      platformOpt = Some(platform)
    }
    case Some(_) => {
      // TODO: already init-id, log warning
    }
  }

  /**
    * If the computation has been initialized, it calls [[Platform.persist]] on the platform.
    * This gives the platform the responsibility of what data to send/resend down the pipeline.
    */
  def persist(): Unit = platformOpt match{
    case Some(platform) =>
      platform.persist()
    case None => throw new NotInitializedException("Computation", "persist()", None)
  }

  def run(): Unit = platformOpt match {
    case Some(platform) => platform.run()
    case None => {
      throw new NotInitializedException("Computation", "run()", None)
    }
  }

  def terminate(): Unit ={
    platformOpt match {
      case Some(platform) => platform.terminate()
      case None => {
        // TODO: not init-ed. Log warning.
      }
    }
  }

}

abstract class UnaryComputation[Input <: Identifiable[Input], Output <: Identifiable[Output]]
    extends Computation with UnaryChecker[Input,Output] {

  var name: String = s"UnaryComputation-${Random.nextInt(99999)}"
  var unaryPlatformOpt: Option[UnaryPlatform[Input,Output]] = None

  def name(newName: String): UnaryComputation[Input, Output] = { name = newName ; this }

  def getUnaryPlatform(): UnaryPlatform[Input,Output] = unaryPlatformOpt match {
    case Some(unaryPlatform) => unaryPlatform
    case None => {
      throw new NotInitializedException("UnaryComputation", "getUnaryPlatform", None)
    }
  }

  /**
    * This connects the newly initialized platform to the Input Data Store by making it so the Input Data Store
    * sends data down to the platform.
    * @param conf The configuration file.
    * @param inputMap The Input Data Store
    * @param outputMap The Output Data Store
    * @param platform The Platform added by the [[Mapper]] or [[Reducer]] computations.
    */
  def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], platform: UnaryPlatform[Input, Output]): Unit = {
    inputMap.registerConnector(platform.getUpstreamConnector())
    unaryPlatformOpt = Some(platform)
    init(conf, platform)
  }

}

abstract class BinaryComputation[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
    extends Computation with BinaryChecker[InputL,InputR,Output] {

  var name: String = s"BinaryComputation-${Random.nextInt(99999)}"
  var binaryPlatformOpt: Option[BinaryPlatform[InputL, InputR, Output]] = None

  def name(newName: String): BinaryComputation[InputL,InputR, Output] = { name = newName ; this }

  def getBinaryPlatform(): BinaryPlatform[InputL, InputR, Output] = binaryPlatformOpt match {
    case Some(platform) => platform
    case None => {
      throw new NotInitializedException("BinaryComputation", "getBinaryPlatform", None)
    }
  }

  def init(conf: PipeConfig, inputMapL: DataStore[InputL], inputMapR: DataStore[InputR], outputMap: DataStore[Output], platform: BinaryPlatform[InputL,InputR,Output]): Unit = {
    inputMapL.registerConnector(platform.getUpstreamLConnector())
    inputMapR.registerConnector(platform.getUpstreamRConnector())
    binaryPlatformOpt = Some(platform)
    init(conf, platform)
  }

}