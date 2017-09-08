package protopipes.configurations

import protopipes.exceptions.NotInitializedException

/**
  * Created by edmundlam on 9/5/17.
  */
trait ConfigBuilds[Builder] {

  var configOpt: Option[PipeConfig] = None

  def setConfig(pipeConfig: PipeConfig): Unit = configOpt = Some(pipeConfig)

  def getConfig(config: PipeConfig): PipeConfig = {
    if (config == null) {
      configOpt match {
        case Some(config) => config
        case None => throw new NotInitializedException("ConfigBuilds", "getConfig", None)
      }
    } else config
  }

  def constructBuilder(config: PipeConfig = null): Builder

}

trait ConfigBuildsPlatform extends ConfigBuilds[PlatformBuilder] {

  override def constructBuilder(config: PipeConfig = null): PlatformBuilder = {
    val thisConfig = getConfig(config)
    PlatformBuilder.load(thisConfig)
  }

}

trait ConfigBuildsDataStore extends ConfigBuilds[DataStoreBuilder] {

  override def constructBuilder(config: PipeConfig): DataStoreBuilder = {
    val thisConfig = getConfig(config)
    DataStoreBuilder.load(thisConfig)
  }

}

trait ConfigBuildsDataStorePlatform extends ConfigBuilds[(PlatformBuilder,DataStoreBuilder)] {

  val buildsPlatform = new ConfigBuildsPlatform {}
  val buildsDataStore = new ConfigBuildsDataStore {}

  override def setConfig(pipeConfig: PipeConfig): Unit = {
    super.setConfig(pipeConfig)
    buildsPlatform.setConfig(pipeConfig)
    buildsDataStore.setConfig(pipeConfig)
  }

  override def constructBuilder(config: PipeConfig): (PlatformBuilder, DataStoreBuilder) =
    (buildsPlatform.constructBuilder(config), buildsDataStore.constructBuilder(config))

}