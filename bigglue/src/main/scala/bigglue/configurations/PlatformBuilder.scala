package bigglue.configurations

import com.typesafe.config.Config
import bigglue.connectors.Connector
import bigglue.curators.{ErrorCurator, ProvenanceCurator}
import bigglue.data.Identifiable
import bigglue.platforms._
import bigglue.platforms.instances.MapperPlatform

import scala.util.Random

/**
  * Created by edmundlam on 8/8/17.
  */


abstract class PlatformBuilder {

  def connector[Data <: Identifiable[Data]](name: String): Connector[Data]

  def provenanceCurator[Input <: Identifiable[Input], Output <: Identifiable[Output]]: ProvenanceCurator[Input,Output]

  def errorCurator[Input <: Identifiable[Input]]: ErrorCurator[Input]

  def mapperPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] // with ComputesMap[Input,Output]

  def reducerPlatform[Input <: Identifiable[Input],Output <: Identifiable[Output]](): UnaryPlatform[Input,Output] // with ComputesReduce[Input,Output]

  def pairwiseComposerPlatform[InputL <: Identifiable[InputL],InputR <: Identifiable[InputR],Output <: Identifiable[Output]]: BinaryPlatform[InputL,InputR,Output] // with ComputesPairwiseCompose[InputL,InputR,Output]

}

object PlatformBuilder {

  def load(pconf: PipeConfig): PlatformBuilder = {
     val conf = pconf.typeSafeConfig
     val protoConf = conf.getConfig(Constant.BIGGLUE)
     // println( protoConf.getString(Constant.MAPPER) )

     new PlatformBuilder {

       override def pairwiseComposerPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]: BinaryPlatform[InputL, InputR, Output] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.PAIRWISE)).getConstructors()(0)
         val args = Array[AnyRef](s"pairwise-platform-${Random.nextInt(99999)}")
         constructor.newInstance( args:_* ).asInstanceOf[BinaryPlatform[InputL,InputR,Output]]
       }

       override def reducerPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]](): UnaryPlatform[Input, Output] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.REDUCER)).getConstructors()(0)
         val args = Array[AnyRef](s"reducer-platform-${Random.nextInt(99999)}")
         constructor.newInstance( args:_* ).asInstanceOf[UnaryPlatform[Input,Output]]
       }

       override def mapperPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]](): UnaryPlatform[Input, Output] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.MAPPER)).getConstructors()(0)
         val args = Array[AnyRef](s"mapper-platform-${Random.nextInt(99999)}")
         constructor.newInstance( args:_* ).asInstanceOf[UnaryPlatform[Input,Output]]
       }

       override def connector[Data <: Identifiable[Data]](name: String): Connector[Data] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.CONNECTOR)).getConstructors()(0)
         constructor.newInstance().asInstanceOf[Connector[Data]]
       }

       override def provenanceCurator[Input <: Identifiable[Input], Output <: Identifiable[Output]]: ProvenanceCurator[Input, Output] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.PROVENANCE)).getConstructors()(0)
         constructor.newInstance().asInstanceOf[ProvenanceCurator[Input,Output]]
       }

       override def errorCurator[Input <: Identifiable[Input]]: ErrorCurator[Input] = {
         val constructor = Class.forName(protoConf.getConfig(Constant.PLATFORM).getString(Constant.ERROR)).getConstructors()(0)
         constructor.newInstance().asInstanceOf[ErrorCurator[Input]]
       }
     }
  }

}