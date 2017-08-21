package protopipes.configurations

import java.io.File
import java.util.Map.Entry
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigValue}

/**
  * Created by edmundlam on 8/15/17.
  */
abstract class ConfOpt

object DefaultOpt extends ConfOpt

case class StepLabelOpt(label: String) extends ConfOpt

case class TypesafeConfOpt(conf: Config) extends ConfOpt

case class PropertiesOpt(props: Properties) extends ConfOpt

object ConfOpt {

  def default: ConfOpt = DefaultOpt
  def label(name: String) = StepLabelOpt(name)
  def typesafeConfig(conf: Config) = TypesafeConfOpt(conf)
  def properties(props: Properties) = PropertiesOpt(props)

}

object PipeConfig {

   def newConfig(typeSafeConfOpt: Option[Config] = None, javaUtilsPropsOpt: Option[Map[PropKey,Properties]] = None): PipeConfig = {
     PipeConfig(typeSafeConfOpt.getOrElse(ConfigFactory.load()), javaUtilsPropsOpt.getOrElse(Map.empty[PropKey,Properties]))
   }

   def fromFile(fileName: String): Config = {
      ConfigFactory.parseFile(new File(fileName)).withFallback(ConfigFactory.load())
   }

   // Returns a new configuration file, with configurations in path 'stepName' set as the primary,
   // and using current configurations as fallback.
   def liftTypeSafeConfigToLabel(mainConfig: Config, stepName: String): Config = {
     if (!mainConfig.getConfig(Constant.PROTOPIPES).hasPath(stepName)) { return mainConfig }
     val stepConfig = mainConfig.getConfig(Constant.PROTOPIPES).getConfig(stepName)
     stepConfig.withFallback(mainConfig.getConfig(Constant.PROTOPIPES)).atPath(Constant.PROTOPIPES)
   }

   def resolveOptions(mainConfig: PipeConfig, option: ConfOpt): PipeConfig = option match {
     case DefaultOpt => mainConfig
     case StepLabelOpt(stepName) => mainConfig.withTypeSafeConfig(liftTypeSafeConfigToLabel(mainConfig.typeSafeConfig, stepName))
     case TypesafeConfOpt(conf) => mainConfig.withTypeSafeConfig(conf.withFallback(mainConfig.typeSafeConfig))
   }

   def resolveOptions(conf: Config, option: ConfOpt): Config = {
     resolveOptions(PipeConfig.newConfig(typeSafeConfOpt = Some(conf)), option).typeSafeConfig
   }

   def resolveConfig(props: Properties, conf: Config): Properties = {
     extractTypeSafeConfig(conf).toList foreach {
       item => props.put(item._1, item._2)
     }
     props
   }

  /*
   def resolveToTypeSafeConfig(mainConfig: PipeConfig, option: ConfOpt): Config = {
     resolveOptions( resolveToTypeSafeConfig(mainConfig), option)
   } */

   // def resolveToJavaUtilProps(mainConfig: PipeConfig, option: ConfOpt): Properties = ???

  /*
   def resolveToTypeSafeConfig(config: PipeConfig): Config = config match {
     case TypesafeConfig(conf) => conf
     case _ => {
       // TODO: Throw exception
       ???
     }
   } */

   def extractTypeSafeConfig(conf: Config): Map[String,String] = {
     var map = Map.empty[String,String]
     import scala.collection.JavaConversions._
     for(entry <- conf.entrySet().iterator()) {
       val path  = entry.getKey
       val value = entry.getValue
       map = map + (path -> value.render())
     }
     map
   }

}

case class PropKey(pipeLabel: String, propType: String)

case class PipeConfig(typeSafeConfig: Config, javaUtilsProps: Map[PropKey,Properties]) {

  def withTypeSafeConfig(conf: Config): PipeConfig = PipeConfig(conf, javaUtilsProps)

  def withJavaUtilProperties(label: String, propType: String, props: Properties): PipeConfig =
        PipeConfig(typeSafeConfig, javaUtilsProps + (PropKey(label, propType) -> props))

}

// case class TypesafeConfig(config: Config) extends PipeConfig

// case class JavaUtilPropsConfig(props: Properties) extends PipeConfig