package protopipes.configurations

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

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

   def newConfig(typeSafeConfOpt: Option[Config] = None, javaUtilsPropsOpt: Option[Map[String,Properties]] = None): PipeConfig = {
     PipeConfig(typeSafeConfOpt.getOrElse(ConfigFactory.load()), javaUtilsPropsOpt.getOrElse(Map.empty[String,Properties]))
   }

   def fromFile(fileName: String): Config = {
      ConfigFactory.parseFile(new File(fileName)).withFallback(ConfigFactory.load())
   }

   // Returns a new configuration file, with configurations in path 'stepName' set as the primary,
   // and using current configurations as fallback.
   def combineConfig(mainConfig: Config, stepName: String): Config = {
     val stepConfig = mainConfig.getConfig(Constant.PROTOPIPES).getConfig(stepName)
     stepConfig.withFallback(mainConfig.getConfig(Constant.PROTOPIPES)).atPath(Constant.PROTOPIPES)
   }

   def resolveOptions(mainConfig: PipeConfig, option: ConfOpt): PipeConfig = option match {
     case DefaultOpt => mainConfig
     case StepLabelOpt(stepName) => mainConfig.withTypeSafeConfig(combineConfig(mainConfig.typeSafeConfig, stepName))
     case TypesafeConfOpt(conf) => mainConfig.withTypeSafeConfig(conf.withFallback(mainConfig.typeSafeConfig))
   }

   def resolveOptions(conf: Config, option: ConfOpt): Config = {
     resolveOptions(PipeConfig.newConfig(typeSafeConfOpt = Some(conf)), option).typeSafeConfig
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

}

case class PipeConfig(typeSafeConfig: Config, javaUtilsProps: Map[String,Properties]) {

  def withTypeSafeConfig(conf: Config): PipeConfig = PipeConfig(conf, javaUtilsProps)

  def withJavaUtilProperties(name: String, props: Properties): PipeConfig =
        PipeConfig(typeSafeConfig, javaUtilsProps + (name -> props))

}

// case class TypesafeConfig(config: Config) extends PipeConfig

// case class JavaUtilPropsConfig(props: Properties) extends PipeConfig