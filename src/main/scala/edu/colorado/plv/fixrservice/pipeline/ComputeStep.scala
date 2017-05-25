/**
  * Created by chanceroberts on 5/18/17.
  */
package edu.colorado.plv.fixrservice.pipeline

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Success, Failure}

class ComputeStep[A,B](func: (A => B)) {
  val system: ActorSystem = ActorSystem("IncrementalComputation")
  implicit val timeout = Timeout(5 seconds)

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

  def IncrementalCompute(statMap: DataMap[String, String], idToAMap: DataMap[String, A],
                        errMap: DataMap[String, Exception], provMap: DataMap[B, String], includeExceptions: List[Exception] = List.empty): Unit = {
    //Start by getting every single key available. (Can probably do this through the Status Map.
    val statusKeys: List[String] = statMap.getAllKeys
    //Iterate through all of the statusKeys
    val futures = statusKeys.foldRight(List.empty[(String, ActorRef, Future[Any])]){
      case (key, list) => statMap.get(key) match {
        case Some("Not Done") =>
          val actor: ActorRef = system.actorOf(Props(new FunctionActor(func)), key + "LoadedActor")
          val res = actor ? idToAMap.get(key)
          (key, actor, res) :: list

        case Some("Error") =>
          val canRedo: Boolean = errMap.get(key) match {
            case Some(e) => includeExceptions.foldLeft(false) {
              case (false, checkE) => e.isInstanceOf[checkE.type]
              case _ => true
            }
            case None => true
          }
          if (canRedo) {
            val actor: ActorRef = system.actorOf(Props(new FunctionActor(func)), key + "LoadedActor")
            val res: Future[Any] = actor ? idToAMap.get(key)
            (key, actor, res) :: list
            /*res onComplete{
              case Success(b: B) =>
                provMap.put(b, key)
                statMap.put(key, "Done")
              case Success("") => ()
              case Success(e: Exception) =>
                errMap.put(key, e)
                statMap.put(key, "Error")
              case _ =>
                errMap.put(key, new UnexpectedException)
            }*/
          } else list
        case Some("Done") => list
        case _ =>
          println("Invalid Status on " + key)
          list
      }
    }
    while(futures.foldRight(false){
      case ((key, actor, future), curr) =>
        future.value match{
          case Some(Success(e: Exception)) =>
            errMap.put(key, e)
            statMap.put(key, "Error")
            curr
          case Some(Success(b: B)) =>
            provMap.put(b, key)
            statMap.put(key, "Done")
            curr
          case Some(Success("")) => curr
            curr
          case Some(_) => curr
            errMap.put(key, new UnexpectedException)
            statMap.put(key, "Error")
            curr
          case None => true
      }
    }){}
  }

  def main(args: Array[String]) = {
    val c: Config = {
      if (args.length > 0) ConfigFactory.load(args(0)) else ConfigFactory.load("")
    }
    val statMap: DataMap[String, String] = setUpDatabase(c, "StatusMap", "Database", "Status")
    val idToAMap: DataMap[String, A] = setUpDatabase(c, "IDtoAMap", "Database", "IDtoA")
    val errMap: DataMap[String, Exception] = setUpDatabase(c, "ErrorMap", "Database", "Error")
    val provMap: DataMap[B, String] = setUpDatabase(c, "ProvenanceMap", "Database", "Provenance")
    IncrementalCompute(statMap, idToAMap, errMap, provMap)
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