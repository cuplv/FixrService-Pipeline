/**
  * Created by chanceroberts on 5/18/17.
  */
package edu.colorado.plv.fixr.fixrservice.pipeline

import java.io.File

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

//import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

class SetupDatabases {
  def possiblyInConfig(config: Config, checkVal: String, defaultVal: String): String = {
    try{
      config.getString(checkVal)
    }
    catch{
      case _: ConfigException.Missing => defaultVal
      case _: ConfigException.WrongType => config.getAnyRef(checkVal).toString //This should work? :|
    }
  }

  def possiblyInMultipleConfigs(configs: List[Config], checkVal: String, defaultVal: String): String = {
    configs.foldLeft((false, "")) {
      case ((b, str), conf) => if (b) (b, str) else {
        try {
          (true, conf.getString(checkVal))
        }
        catch {
          case _: ConfigException.Missing => (false, "")
          case _: ConfigException.WrongType => (true, conf.getAnyRef(checkVal).toString)
        }
      }
    } match {
      case (false, _) => defaultVal
      case (true, str) => str
    }
  }

  def setUpDatabase[A,B](c: Config, name: String, defDataName: String, defCollName: String, dMap: Option[DataMap[A, B]] = None) = dMap match{
    case None => possiblyInConfig(c, name + "Type", "Heap") match {
      case "Null" => new NullMap[A, B]
      case "Heap" => new HeapMap[A, B]
      case "MongoDB" =>
        val databaseName = possiblyInConfig(c, name + "Database", defDataName)
        val collectionName = possiblyInConfig(c, name + "Collection", defCollName)
        val username = possiblyInConfig(c, name + "Username", "")
        val password = possiblyInConfig(c, name + "Password", "")
        val IP = possiblyInConfig(c, name + "IP", "localhost")
        val port = possiblyInConfig(c, name + "Port", "27017")
        new MongoDBMap[A, B](databaseName, collectionName, IP, port, username, password)
      case "Solr" =>
        val collectionName = possiblyInConfig(c, name + "Collection", defDataName)
        val fieldName = possiblyInConfig(c, name + "Field", defDataName)
        val IP = possiblyInConfig(c, name + "IP", "localhost")
        val port = possiblyInConfig(c, name + "Port", "8983")
        new SolrMap[A, B](collectionName, fieldName, IP, port)
      case "WebService" =>
        val IP = possiblyInConfig(c, name + "IP", "localhost")
        val port = possiblyInConfig(c, name + "Port", "8080")
        val dName = possiblyInConfig(c, name + "Name", "Default")
        new DataMapWebServiceClient[A, B](IP, port, dName)
    }
    case Some(x) => x
  }
}

class ComputeStep[A,B](func: (A => B), config: String, prefix: String = "", dMap: Option[DataMap[String, A]] = None, sMap: Option[DataMap[String, String]] = None) {
  val system: ActorSystem = ActorSystem("IncrementalComputation")
  val c: Config =  ConfigFactory.parseFile(new File(config+".conf"))
  val setup: SetupDatabases = new SetupDatabases
  val statMap: DataMap[String, String] = setup.setUpDatabase(c, "StatusMap", "Database", "Status", sMap)
  val idToAMap: DataMap[String, A] = setup.setUpDatabase(c, "IDtoAMap", "Database", "IDtoA", dMap)
  val idToBMap: DataMap[String, B] = setup.setUpDatabase(c, "IDtoBMap", "Database", "IDtoB")
  val errMap: DataMap[String, Exception] = setup.setUpDatabase(c, "ErrorMap", "Database", "Error")
  val provMap: DataMap[B, List[String]] = setup.setUpDatabase(c, "ProvenanceMap", "Database", "Provenance")
  val AToBMap: DataMap[A, B] = setup.setUpDatabase(c, "AToBMap", "Database", "AToB")
  implicit val timeout = Timeout(10 hours)
  implicit val executionContext = system.dispatcher

  def IncrementalComputeHelper(key: String): Unit = {
    val actor: ActorRef = system.actorOf(Props(new FunctionActor(func)), key + "LoadedActor")
    val res = actor ? idToAMap.get(key)
    res.onComplete{
      case Success(e: Exception) =>
        errMap.put(key, e)
        statMap.put(key, "Error")
      case Success("") => ()
      case Success(b: B) =>
        val l = provMap.get(b) match {
          case Some(list) => key :: list
          case None => List(key)
        }
        provMap.put(b, l)
        AToBMap.put(idToAMap.getM(key), b)
        statMap.put(key, "Done")

      case _ =>
        errMap.put(key, new UnexpectedException)
        statMap.put(key, "Error")
    }
  }

  def IncrementalCompute(blocking: Boolean = false, includeExceptions: List[Exception] = List.empty): Unit = {
    //Start by getting every single key available.
    val statusKeys: List[(String,String)] = statMap.getAllKeysAndValues
    //Iterate through all of the statusKeys
    val futures = statusKeys.foldRight(List.empty[String]){
      case ((key, stat), list) => stat match {
        case "Not Done" =>
          IncrementalComputeHelper(key)
          key :: list

        case "Error" =>
          val canRedo: Boolean = errMap.get(key) match {
            case Some(e) => includeExceptions.foldLeft(false) {
              case (false, checkE) =>
                e.toString.equals(checkE.toString)

              case _ => true
            }
            case None => true
          }
          if (canRedo) {
            statMap.put(key, "Not Done")
            IncrementalComputeHelper(key)
            key :: list
          } else list
        case "Done" => list
        case _ =>
          println("Invalid Status on " + key)
          list
      }
    }
    while(blocking && futures.foldRight(false){
      case (key, curr) =>
        if (statMap.get(key).contains("Not Done")) true
         else curr
    }){}
  }
}
/*
class ComputeAStepTuple[A,B,C](func: (((A,B)) => C), config: String, prefix: String = "") extends ComputeStep[(A,B),C](func, config, prefix){
  override val idToAMap = setUpTupleDatabase(c, "IDToA", "Database", "IDToA")
  //setUpTupleDatabase[String, String, A, B](c, "IDToA", "Database", "Status")
  def setUpTupleDatabase[D,E,F,G](c: Config, name: String, defDataName: String, defCollName: String): DataMap[String, (F,G)] = {
    val dataMapOne = setup.setUpDatabase[D,F](c, name+"SubmapOne", defDataName, defCollName)
    val dataMapTwo = setup.setUpDatabase[E,G](c, name+"SubmapTwo", defDataName, defCollName)
    new DualStringDataMap[D,E,F,G](dataMapOne, dataMapTwo)
  }

}
*/



class FunctionActor[A,B](func: A => B) extends Actor{
  def receive = {
    case Some(a: A) => try{
      val b: B = func(a)
      sender() ! b
    } catch {
      case e: Exception => sender() ! e
    }
    case _ => sender() ! "?"
  }
}

class UnexpectedException extends Exception("No one expects"){}