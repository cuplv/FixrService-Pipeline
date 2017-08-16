package protopipes.configurations

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

/**
  * Created by edmundlam on 8/15/17.
  */
abstract class ConfOpt

object DefaultOpt extends ConfOpt

case class StepLabelOpt(label: String) extends ConfOpt

case class OverrideConfOpt(conf: Config) extends ConfOpt

object ConfOpt {

  def default: ConfOpt = DefaultOpt
  def label(name: String) = StepLabelOpt(name)
  def overrideConfig(conf: Config) = OverrideConfOpt(conf)

}

object PipeConfig {

   def fromFile(fileName: String): Config = {
      ConfigFactory.parseFile(new File(fileName)).withFallback(ConfigFactory.load())
   }

   // Returns a new configuration file, with configurations in path 'stepName' set as the primary,
   // and using current configurations as fallback.
   def combineConfig(mainConfig: Config, stepName: String): Config = {
     val stepConfig = mainConfig.getConfig(Constant.PROTOPIPES).getConfig(stepName)
     stepConfig.withFallback(mainConfig.getConfig(Constant.PROTOPIPES)).atPath(Constant.PROTOPIPES)
   }

   def resolveOptions(mainConfig: Config, option: ConfOpt): Config = option match {
     case DefaultOpt => mainConfig
     case StepLabelOpt(stepName) => combineConfig(mainConfig, stepName)
     case OverrideConfOpt(conf) => conf.withFallback(mainConfig)
   }

}
