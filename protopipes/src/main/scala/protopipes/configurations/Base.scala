package protopipes.configurations

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

/**
  * Created by edmundlam on 8/15/17.
  */
abstract class ConfigOption

object DefaultOption extends ConfigOption

case class StepNameOption(stepName: String) extends ConfigOption

case class OverrideConfigOption(conf: Config) extends ConfigOption

object ConfigOption {

  def default: ConfigOption = DefaultOption
  def stepName(name: String) = StepNameOption(name)
  def overrideConfig(conf: Config) = OverrideConfigOption(conf)

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

   def resolveOptions(mainConfig: Config, option: ConfigOption): Config = option match {
     case DefaultOption => mainConfig
     case StepNameOption(stepName) => combineConfig(mainConfig, stepName)
     case OverrideConfigOption(conf) => conf.withFallback(mainConfig)
   }

}
