package bigglue.configurations

import java.lang.reflect.Constructor

import com.typesafe.config.{Config, ConfigValueFactory, ConfigValueType}
import bigglue.data.Identifiable
import bigglue.exceptions.NotSupportedConfigFormatException
import bigglue.platforms.BinaryPlatform
import bigglue.store._

import scala.util.Random

/**
  * Created by edmundlam on 8/15/17.
  */

abstract class DataStoreBuilder {
  var confOpt: Option[Config] = None

  def map[Key,Data](name:String, template:String = Constant.MAP): DataMap[Key,Data]

  def idmap[Item <: Identifiable[Item]](name:String, template:String = Constant.IDMAP): IdDataMap[Item]

  def multimap[Key,Data](name:String, template:String = Constant.MULTIMAP): DataMultiMap[Key,Data]

  def queue[Data](name:String, template:String = Constant.QUEUE): DataQueue[Data]

  def list[Data](name:String, template:String = Constant.LIST): DataStore[Data]

  def loadAux(pconf: PipeConfig): DataStoreBuilder = {
    val conf = pconf.typeSafeConfig
    val protoConf = conf.getConfig(Constant.BIGGLUE)
    confOpt = Some(protoConf)
    this
  }

  def mkAuxStore[Store](template: String): Option[Store] = confOpt match {
    case Some(conf) => {
      // println(s"Making datastore $template")
      Some (Class.forName (conf.getConfig (Constant.DATASTORE).getString (template) ).getConstructors () (0).newInstance ().asInstanceOf[Store] )
    }
    case None => None
  }

}

object DataStoreBuilder {

  def load(pconf: PipeConfig): DataStoreBuilder = {
    val conf = pconf.typeSafeConfig
    val protoConf = conf.getConfig(Constant.BIGGLUE)

    new DataStoreBuilder {
      override def list[Data](name: String, template: String = Constant.LIST): DataStore[Data] = {
        val constructor = Class.forName(protoConf.getConfig(Constant.DATASTORE).getString(template)).getConstructors()(0)
        val ls = constructor.newInstance().asInstanceOf[DataStore[Data]]
        ls.setName(name)
        ls
      }
      override def multimap[Key, Data](name: String, template: String = Constant.MULTIMAP): DataMultiMap[Key, Data] = {
        val constructor = Class.forName(protoConf.getConfig(Constant.DATASTORE).getString(template)).getConstructors()(0)
        val mmap = constructor.newInstance().asInstanceOf[DataMultiMap[Key,Data]]
        mmap.setName(name)
        mmap
      }
      override def map[Key, Data](name: String, template: String = Constant.MAP): DataMap[Key, Data] = {
        println("Cool! " + protoConf.getConfig(Constant.DATASTORE).getString(template))
        val constructor = Class.forName(protoConf.getConfig(Constant.DATASTORE).getString(template)).getConstructors()(0)
        val map =constructor.newInstance().asInstanceOf[DataMap[Key,Data]]
        map.setName(name)
        map
      }
      override def idmap[Item <: Identifiable[Item]](name:String, template: String = Constant.IDMAP): IdDataMap[Item] = {
        val constructor = Class.forName(protoConf.getConfig(Constant.DATASTORE).getString(template)).getConstructors()(0)
        val map = constructor.newInstance().asInstanceOf[IdDataMap[Item]]
        map.setName(name)
        map
      }
      override def queue[Data](name: String, template: String = Constant.QUEUE): DataQueue[Data] = {
        val constructor = Class.forName(protoConf.getConfig(Constant.DATASTORE).getString(template)).getConstructors()(0)
        val qu = constructor.newInstance().asInstanceOf[DataQueue[Data]]
        qu.setName(name)
        qu
      }
    }.loadAux(pconf)

  }

  def main(args: Array[String]): Unit = {
    val config = PipeConfig.newConfig()

    load(config)
  }

}

