package protopipes.configurations

import com.typesafe.config.Config
import protopipes.data.Identifiable
import protopipes.platforms.BinaryPlatform
import protopipes.store._

import scala.util.Random

/**
  * Created by edmundlam on 8/15/17.
  */

abstract class DataStoreBuilder {

  def map[Key,Data](name:String, template:String = Constant.MAP): DataMap[Key,Data]

  def idmap[Item <: Identifiable[Item]](name:String, template:String = Constant.IDMAP): IdDataMap[Item]

  def multimap[Key,Data](name:String, template:String = Constant.MULTIMAP): DataMultiMap[Key,Data]

  def queue[Data](name:String, template:String = Constant.QUEUE): DataQueue[Data]

  def list[Data](name:String, template:String = Constant.LIST): DataStore[Data]

}

object DataStoreBuilder {

  def load(conf: Config): DataStoreBuilder = {
    val protoConf = conf.getConfig(Constant.PROTOPIPES)

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
    }

  }

}