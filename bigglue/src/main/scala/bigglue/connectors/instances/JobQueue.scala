package bigglue.connectors.instances

import bigglue.configurations.{PipeConfig, PlatformBuilder}
import bigglue.connectors.{Connector, Status}
import bigglue.connectors.Connector.Id
import bigglue.connectors.Status.Status
import bigglue.curators.{StandardVersionCurator, VersionCurator}
import bigglue.data.{BasicIdentity, I, Identifiable}
import bigglue.exceptions.{NotInitializedException, ProtoPipeException}
import bigglue.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import bigglue.store.instances.file.TextFileDataMap
import bigglue.store._
import bigglue.store.instances.{InMemDataMap, InMemDataMultiMap, InMemDataQueue}
import com.typesafe.config.Config

/**
  * Created by edmundlam on 8/8/17.
  */
class JobQueue[Data] extends Connector[Data] {

  var queue: DataQueue[Data] = new InMemDataQueue[Data]

  override def init(conf: PipeConfig): Unit = { }

  override def terminate(): Unit = { }

  override def signalDown(): Unit = getDownstream().signalDown()

  override def sendDown(data: Seq[Data]): Unit = {
    queue.enqueue(data)
    // getPlatform().wake()
    // getDownstream().signalDown()
    super.signalDown()
  }

  override def sendDownModified(data: Seq[Data]): Unit = sendDown(data)

  override def retrieveUp(): Seq[Data] = queue.extract()

  override def reportUp(status:Status, data: Seq[Data]): Unit = { }

  override def size(): Int = queue.size()

}

class IncrTrackerJobQueue[Data <: Identifiable[Data]] extends JobQueue[Data] {

  // var currId: Id = 0L
  // val history: DataMap[Id, Identity[Data]] = new InMemDataMap[Id, Identity[Data]]
  val statusMap: DataMultiMap[Status, Data] = new InMemDataMultiMap[Status, Data]
  //var iMapOpt: Option[DataStore[Data]] = Some(new InMemDataMap[Data, Data])
  var textMapOpt: Option[TextFileDataMap] = None
  var iMapOpt: Option[DataStore[Data]] = Some(new InMemDataMap[Data, Data])
  var ver: Option[String] = None

  def textMap: TextFileDataMap = textMapOpt match{
    case Some(x) => x
    case None => throw new ProtoPipeException(Some("Stored Map call not allowed: Platform has not been registered."))
  }

  def iMap: DataStore[Data] = iMapOpt match{
    case Some(x) => x
    case None => throw new ProtoPipeException(Some("Stored Map call not allowed: Platform has not been registered."))
  }

  /*
  def getMap(): DataMap[Identity[Data],Data] = datastoreOpt match {
    case Some(dataStore) => dataStore.asInstanceOf[DataMap[Identity[Data],Data]]
    case None => {
      // TODO: Throw exception
      ???
    }
  } */

  def getStatusMap(): DataMultiMap[Status, Data] = statusMap

  private def appendStatus(data: Seq[Data], status: Status): Unit = {
    val ids = data.foldRight(List[String]()){
      case (dat, lis) => dat.identity().getVersion() match{
        case None => dat.identity().getId() :: lis
        case Some(x) => s"${dat.identity().getId()}-#-$x" :: lis
      }
    }
    val trueStatus = ver match {
      case Some(v) => s"${status.toString} v.$v}"
      case None => status.toString
    }
    textMapOpt match{
      case Some(textMap) =>
        val (noNeedToAppend, strs) = textMap.extract().foldRight((List[String](), List[String]())){
          case (str, (x, curStrs)) =>
            val statusSplit = str.a.split(" -!- ")
            statusSplit.length match {
              case y if y == 2 =>

                val (newID, realStr) = ids.foldLeft(x, str.a) {
                  case ((nIDs, st), id) => if (id.equals(statusSplit(0))) {
                    (trueStatus.indexOf("v."), statusSplit(1).indexOf("v.")) match {
                      case (-1, -1) =>
                        (id :: nIDs, s"${statusSplit(0)} -!- $trueStatus")
                      case (xx, yy) if trueStatus.substring(xx+2).equals(statusSplit(1).substring(yy+2)) =>
                        (id :: nIDs, s"${statusSplit(0)} -!- $trueStatus")
                      case _ => (nIDs, str.a)
                    }
                  } else (nIDs, st)

                }
                (newID, realStr :: curStrs)
              case _ => (x, curStrs)
            }
        }
        val realList = ids.diff(noNeedToAppend).foldRight(strs){
          case (toApp, lis) => s"$toApp -!- $trueStatus" :: lis
        }
        textMap.put(realList.map(x => I(x)))
      case None => ()
    }
    /*data.foreach{dat =>
      val string = ver match{
        case None => status.toString
        case Some(x) => s"${status.toString}-#-$x"
      }
      dat.addEmbedded("status", string)
    }
    if (iMap.isInstanceOf[DataMap[_, _]]){
      try {
        iMap.put_(data)
      } catch{
        case e: Exception => ()
      }
    }*/
  }

  override def sendDown(data: Seq[Data]): Unit = {
    val newData = data.filterNot( d => statusMap.contains(Status.Done, d) )
    statusMap.put(Status.NotDone, newData.toSet)
    appendStatus(newData, Status.NotDone)
    super.sendDown(newData)
  }

  override def sendDownModified(data: Seq[Data]): Unit = {
    statusMap.put(Status.Modified, data.toSet)
    appendStatus(data, Status.Modified)
    super.sendDown(data)
  }

  override def registerPlatform(platform: Platform): Unit = {
    super.registerPlatform(platform)
    platform match{
      case u: UnaryPlatform[Data, _] =>
        iMapOpt = Some(u.getInputMap())
        textMapOpt = Some(new TextFileDataMap(s"${u.getInputMap().displayName()}-STATUS"))
        ver = (u.versionCuratorOpt match {
          case None => None
          case Some(version) => Some(version.thisVersion)
        }) match {
          case Some("<None>") => None
          case x => x
        }
      case b: BinaryPlatform[_, _, _] =>
        val (textMapo, iMapo) = b.upstreamLConnectorOpt match {
          case None => (Some(new TextFileDataMap(s"${b.getInputLMap().displayName()}-STATUS")), b.inputLMapOpt.asInstanceOf[Option[DataStore[Data]]])
          case _ => b.upstreamRConnectorOpt match {
            case None => (Some(new TextFileDataMap(s"${b.getInputRMap().displayName()}-STATUS")), b.inputRMapOpt.asInstanceOf[Option[DataStore[Data]]])
            case _ => throw new ProtoPipeException(Some("Connector registered to full platform!"))
          }
        }
        ver = (b.versionCuratorOpt match{
          case None => None
          case Some(version) => Some(version.thisVersion)
        }) match {
          case Some("<None>") => None
          case x => x
        }
        textMapOpt = textMapo
        iMapOpt = iMapo
    }
  }

  override def persist(dataStore: DataStore[Data]): Unit = {
    //super.persist(dataStore)
    iMapOpt = Some(dataStore)
    val ids = dataStore.all().foldRight(Map[String, (Boolean, Data)]()) {
      case (dat, lis) => dat.identity().getVersion() match {
        case None => lis+(dat.identity().getId()->(false, dat))
        case Some(x) => lis+(s"${dat.identity().getId()}-#-$x"->(false, dat))
      }
    }
    val trueIds = textMap.iterator().foldRight(ids){
      case (str, oIDs) =>
        val statusSplit = str.a.split(" -!- ")
        if (oIDs.contains(statusSplit(0))){
          (statusSplit(1).indexOf("v."), ver) match{
            case (-1, None) =>
              statusMap.put(Status.withName(statusSplit(1)), Set(oIDs(statusSplit(0))._2))
              oIDs + (statusSplit(0)->(true, oIDs(statusSplit(0))._2))
            case (x, Some(v)) => if (statusSplit(1).substring(x+2).equals(v)) {
              statusMap.put(Status.withName(statusSplit(1).substring(0, x)), Set(oIDs(statusSplit(0))._2))
              oIDs + (statusSplit(0) -> (true, oIDs(statusSplit(0))._2))
            } else oIDs
            case (x, None) => oIDs
          }
        } else oIDs
    }
    val toSend = trueIds.foldRight(List[Data]()){
      case ((_, (false, dat)), lis) =>
        statusMap.put(Status.NotDone, Set(dat))
        dat :: lis
      case (_, lis) => lis
    }
    appendStatus(toSend, Status.NotDone)
    /*textMap.all().foreach(str => ids.foldLeft(false){
      case (true, _) => true
      case (false, id) =>
        val statusSplit = str.a.split(" -!- ")
        if (id._1.equals(statusSplit(0))){
          ???
        } else false
    })*/
    /*(try{
      val iterator = iMap.iterator()
      iterator
    } catch {
      case e: Exception => iMap.all()
    }).foreach{i =>
      i.getEmbedded("status") match{
        case Some(x) =>
          val (realStatus: String, thisVersion: Option[String]) = if (x.split("-#-").length == 1) {
            (x, None)
          } else {
            val a: Array[String] = x.split("-#-")
            (a(0), Some(a(1)))
          }
          val myStatus: String = (thisVersion, ver) match{
            case (None, None) => realStatus
            case (Some(z), Some(y)) if z.equals(y) => realStatus
            case _ => "NotDone"
          }
          //statusMap.put(Status.withName(myStatus), List(i).toSet)
        case _ => ()
      }}*/
    sendDown(statusMap.get(Status.NotDone).toSeq)
    sendDownModified(statusMap.get(Status.Error).toSeq)
    sendDownModified(statusMap.get(Status.Modified).toSeq)
    signalDown()
  }

  override def retrieveUp(): Seq[Data] = {
    // val unique = queue.extract().distinct
    queue.extract() ++ (statusMap.get(Status.Error) ++ statusMap.get(Status.Modified))
     /*
     (previous ++ queue.extract()) foreach {
       d => {
         out = out :+ (currId, d)
         history.put(currId, d.asInstanceOf[Identifiable[Data]].identity())
         currId = Connector.incr(currId)
       }
     }
     out */
  }

  override def reportUp(status: Status, data: Seq[Data]): Unit = {
    statusMap.remove( data.toSet )
    statusMap.put(status, data.toSet )
    appendStatus(data, status)
    // history.remove(ids)
  }

}