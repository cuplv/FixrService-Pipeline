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

abstract class Computation extends ConfigBuildsPlatform {

  var platformOpt: Option[Platform] = None
  var configOption: ConfOpt = DefaultOpt

  val versionOpt: Option[String] = None

  def init(config: PipeConfig, platform: Platform): Unit = platformOpt match {
    case None => {
      platformOpt = Some(platform)
    }
    case Some(_) => {
      // TODO: already init-id, log warning
    }
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