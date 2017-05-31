/**
  * Created by chanceroberts on 5/18/17.
  */
package edu.colorado.plv.fixr.fixrservice.pipeline

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Failure}

class ComputeStep[A,B](func: (A => B), config: String) {
  val system: ActorSystem = ActorSystem("IncrementalComputation")
  val c: Config =  ConfigFactory.load(config)
  val statMap: DataMap[String, String] = setUpDatabase(c, "StatusMap", "Database", "Status")
  val idToAMap: DataMap[String, A] = setUpDatabase(c, "IDtoAMap", "Database", "IDtoA")
  val idToBMap: DataMap[String, B] = setUpDatabase(c, "IDtoBMap", "Database", "IDtoB")
  val errMap: DataMap[String, Exception] = setUpDatabase(c, "ErrorMap", "Database", "Error")
  val provMap: DataMap[B, List[String]] = setUpDatabase(c, "ProvenanceMap", "Database", "Provenance")
  val AToBMap: DataMap[A, B] = setUpDatabase(c, "AToBMap", "Database", "AToB")
  implicit val timeout = Timeout(10 hours)
  implicit val executionContext = system.dispatcher

  def setUpDatabase[C,D](c: Config, name: String, defDataName: String, defCollName: String): DataMap[C,D] = possiblyInConfig(c, name+"Type", "Heap") match {
    case "Null" => new NullMap[C,D]
    case "Heap" => new HeapMap[C,D]
    case "MongoDB" =>
      val databaseName = possiblyInConfig(c, name+"Database", defDataName)
      val collectionName = possiblyInConfig(c, name+"Collection", defCollName)
      val username = possiblyInConfig(c, name+"Username", "")
      val password = possiblyInConfig(c, name+"Password", "")
      val IP = possiblyInConfig(c, name+"IP", "localhost")
      val port = possiblyInConfig(c, name+"Port", "27017")
      new MongoDBMap[C, D](databaseName, collectionName, IP, port, username, password)
  }

  def possiblyInConfig(config: Config, checkVal: String, defaultVal: String): String = {
    try{
      config.getString(checkVal)
    }
    catch{
      case ceM: ConfigException.Missing => defaultVal
      case ceWT: ConfigException.WrongType => config.getAnyRef(checkVal).toString //This should work? :|
    }
  }

  def IncrementalComputeHelper(key: String): Unit = {
    val actor: ActorRef = system.actorOf(Props(new FunctionActor(func)), key + "LoadedActor")
    val res = actor ? idToAMap.get(key)
    res.onComplete{
      case Success(e: Exception) =>
        errMap.put(key, e)
        statMap.put(key, "Error")
      case Success(b: B) =>
        val l = provMap.get(b) match {
          case Some(list) => key :: list
          case None => List(key)
        }
        provMap.put(b, l)
        AToBMap.put(idToAMap.getM(key), b)
        statMap.put(key, "Done")

      case Success("") => ()
      case _ =>
        errMap.put(key, new UnexpectedException)
        statMap.put(key, "Error")
    }
  }

  def IncrementalCompute(blocking: Boolean = false, includeExceptions: List[Exception] = List.empty): Unit = {
    //Start by getting every single key available. (Can probably do this through the Status Map.
    val statusKeys: List[String] = statMap.getAllKeys
    //Iterate through all of the statusKeys
    val futures = statusKeys.foldRight(List.empty[String]){
      case (key, list) => statMap.get(key) match {
        case Some("Not Done") =>
          IncrementalComputeHelper(key)
          key :: list

        case Some("Error") =>
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
        case Some("Done") => list
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
  /*
  def main(args: Array[String]) = { //If for whatever reason you want to use this...
    IncrementalCompute()
  }
  */
}

object ComputeAStep{
  def main(args: Array[String]) = {

  }
}

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