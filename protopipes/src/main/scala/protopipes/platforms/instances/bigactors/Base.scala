package protopipes.platforms.instances.bigactors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import BigActorPlatform._
import protopipes.configurations.{ConfOpt, Constant, PipeConfig, PlatformBuilder}
import protopipes.connectors.{Connector, Status}
import protopipes.connectors.instances.ActorConnector
import protopipes.curators.ErrorCurator
import protopipes.data.Identifiable
import protopipes.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import protopipes.store.DataStore

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random
import collection.JavaConverters._


/**
  * Created by chanceroberts on 8/10/17.
  */
object ConfigHelper{
  def possiblyAConfig(config: Config, path: String): Option[Config] = {
    try{
      Some(config.getConfig(path))
    }
    catch{
      case e: Exception => None
    }
  }

  def possiblyHasObject(config: Config, path: String): Option[ConfigObject] = {
    try{
      Some(config.getObject(path))
    }
    catch{
      case e: Exception => None
    }
  }

}

object BigActorPlatform{

  val NAME: String = "platform-actor"

  case class Wake()
  case class PostWake()
  case class AskForJob(acRef: ActorRef)
  case class AddedWorker(acRef: ActorRef)
  case class Crashed(acRef: ActorRef, exception: Exception)
  case class AddedJobs(l: List[_])

}

class BigActorSupervisorActor[Input <: Identifiable[Input], Output](platform: Platform with BigActor[Input],
                                                                    actorNames: List[String] = List()) extends Actor{
  import context._
  become(state(Nil, Nil))
  def state(jobsLeft: List[Input], workerList: List[ActorRef], isWorking: Map[ActorRef, Boolean] = Map()): Receive = {
    case AddedWorker(worker: ActorRef) =>
      jobsLeft match{
        case job :: rest =>
          worker ! job
          state(rest, worker :: workerList)
        case Nil => become(state(jobsLeft, worker :: workerList, isWorking+(worker->true)))
      }
    case AskForJob(worker: ActorRef) => jobsLeft match{
      case Nil =>
        state(Nil, workerList, isWorking+(worker->false))
      case newJob :: rest =>
        worker ! newJob
        state(rest, workerList, isWorking+(worker->true))
    }
    case Wake() =>
      platform.run()
      self ! PostWake()
    case PostWake() =>
      workerList match{
        case Nil =>
          def addActors(numLeft: Int, actorNames: List[String] = List()): Unit = (numLeft, actorNames) match{
            case (x, _) if x <= 0 => ()
            case (_, name :: rest) =>
              val aRef = context.actorOf(Props(classOf[BigActorWorkerActor[Input, Output]], platform), name)
              self ! AddedWorker(aRef)
              addActors(numLeft-1, rest)
            case (_, Nil) =>
              val aRef = context.actorOf(Props(classOf[BigActorWorkerActor[Input, Output]], platform))
              self ! AddedWorker(aRef)
              addActors(numLeft-1)
          }
          addActors(platform.infoConfig.getInt("numberOfWorkers"), actorNames)
        case _ => workerList foreach(worker => isWorking.get(worker) match{
          case Some(true) => ()
          case _ => self ! AskForJob(worker)
        })
      }
    case (Crashed(acRef: ActorRef, exception: Exception), input: Input @ unchecked) =>
      platform.getErrorCurator().reportError(input, exception)
      self ! AskForJob(acRef)
    case Crashed(acRef: ActorRef, exception: Exception) => ()
    case AddedJobs(inputs: List[Input@unchecked]) => state(jobsLeft ::: inputs, workerList, isWorking)
    case ("AddedJob", input: Input@unchecked) => state(jobsLeft ::: List(input), workerList, isWorking)
    case other => println(s"$other was sent with nothing occurring.")
  }

  def receive(): Receive = {
    case _ => throw new Exception("The Big Actor Supervisor is never supposed to be at this state.")
  }
}

class BigActorWorkerActor[Input <: Identifiable[Input], Output <: Identifiable[Output]](platform: Platform with BigActor[Input]) extends Actor{
  var currInput: Option[Input] = None
  val longestTimeWaiting: Duration = platform.infoConfig.getInt("maxHoursOnInput") match{
    case 0 => platform.infoConfig.getInt("maxMinutesOnInput") match{
      case 0 => platform.infoConfig.getInt("maxSecondsOnInput") match{
        case 0 => Duration.Inf
        case x => x.seconds
      }
      case x => x.minutes
    }
    case x => x.hours
  }

  override def postStop(): Unit = {
    super.postStop()
    currInput match{
      case Some(input) => context.parent ! (Crashed(self, new Exception(s"An actor has crashed trying to compute $input.")), input)
      case _ => context.parent ! Crashed(self, new Exception(s"An actor has decided to stop!"))
    }
  }

  def receive: Receive = {
    case job: Input @ unchecked =>
      currInput = Some(job)
      longestTimeWaiting match{
        case Duration.Inf => platform.compute(job)
        case _ =>
          val futureJob = Future {
            platform.compute(job)
          }(context.dispatcher)
          try {
            Await.result(futureJob, longestTimeWaiting)
          } catch {
            case e: Exception => platform.getErrorCurator().reportError(job, e)
          }
      }
      currInput = None
      sender() ! AskForJob(self)
  }
}

trait BigActor[Input]{
  var infoConfig: Config = ConfigFactory.parseMap(Map[String, Any]
    ("numberOfWorkers" -> 4, "maxSecondsOnInput"-> 0, "maxMinutesOnInput" -> 0,
      "maxHoursOnInput" -> 0, "workerList" -> Seq()).asJava)
  implicit var actorSystemOpt: Option[ActorSystem] = None//ActorSystem(name)
  def compute(input: Input): Unit = ()
  def getErrorCurator(): ErrorCurator[Input]
  implicit def actorSystem: ActorSystem = actorSystemOpt match{
    case Some(sys: ActorSystem) => sys
    case None => throw new Exception("The Actor System does not exist.")
  }
  def updateConfigAndGetActorNames(conf: Config, name: String): List[String] = {
    ConfigHelper.possiblyAConfig(conf.getConfig(Constant.PROTOPIPES), "_bigactor") match {
      case Some(overwriteConfig) =>
        infoConfig = PipeConfig.resolveOptions(infoConfig, ConfOpt.overrideConfig(overwriteConfig))
        actorSystemOpt = Some(ActorSystem(name, PipeConfig.resolveOptions(conf, ConfOpt.overrideConfig(overwriteConfig))))
        ConfigHelper.possiblyAConfig(infoConfig, "akka") match{
          case Some(akkaConf) => ConfigHelper.possiblyAConfig(akkaConf, "actor") match{
            case Some(actorConf) => ConfigHelper.possiblyHasObject(actorConf, "deployment") match{
              case Some(deployment) =>
                deployment.unwrapped().asScala.foldRight(List[String]()){
                  case ((str, _), list) => str.length() match{
                    case x if x > 7 => str.substring(0,7) match{
                      case "/super/" => str.substring(7) :: list
                      case _ => list
                    }
                    case _ => list
                  }
                }
              case None => List()
            }
            case None => List()
          }
          case None => List()
        }
      case _ =>
        actorSystemOpt = Some(ActorSystem(name))
        List()
    }
  }
}

abstract class BigActorUnaryPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME+s"-unary-${Random.nextInt(99999)}") extends UnaryPlatform[Input, Output] with BigActor[Input] {
  var superActorOpt: Option[ActorRef] = None
  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  override def run(): Unit = {
    supervisor ! AddedJobs(getInputs().toList)
  }

  override def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputMap, outputMap, builder)
    val listOfActors = updateConfigAndGetActorNames(conf, name)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[Input, Output]], this, listOfActors), "super"))
  }

  override def initConnector(conf: Config, builder: PlatformBuilder): Unit = {
    val upstreamConnector = new ActorConnector[Input] {
      override val innerConnector: Connector[Input] = builder.connector[Input]("BigActorConnector")
    }
    upstreamConnector.init(conf)
    upstreamConnector.registerPlatform(this)
    upstreamConnectorOpt = Some(upstreamConnector)
  }

  override def wake(): Unit = supervisor ! Wake()

  override def terminate(): Unit = {
    actorSystem.terminate
  }

  override def getErrorCurator(): ErrorCurator[Input] = getErrorCurator()
}

abstract class BigActorBinaryPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + s"-binary-${Random.nextInt(99999)}") extends BinaryPlatform[InputL, InputR, Output] with BigActor[protopipes.data.Pair[InputL, InputR]] {
  var inputLOccurrences: Map[InputL, Integer] = Map()
  var inputROccurrences: Map[InputR, Integer] = Map()
  var superActorOpt: Option[ActorRef] = None
  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  override def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputLMap, inputRMap, outputMap, builder)
    val listOfActors = updateConfigAndGetActorNames(conf, name)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[protopipes.data.Pair[InputL, InputR], Output]], this, listOfActors), "super"))
  }

  override def compute(input: protopipes.data.Pair[InputL, InputR]): Unit = {
    inputLOccurrences.get(input.left) match{
      case Some(x) if x > 1 => inputLOccurrences += (input.left -> (x-1))
      case Some(_) =>
        inputLOccurrences -= input.left
        getUpstreamLConnector().reportUp(Status.Done,input.left)
      case None => ()
    }
    inputROccurrences.get(input.right) match{
      case Some(x) if x > 1 => inputROccurrences += (input.right -> (x-1))
      case Some(_) =>
        inputROccurrences -= input.right
        getUpstreamRConnector().reportUp(Status.Done, input.right)
      case None => ()
    }
  }

  override def run(): Unit = {
    val inputs = getInputs()
    inputs._1.foreach(inputL => inputLOccurrences += (inputL -> 0))
    inputs._2.foreach(inputR => inputROccurrences += (inputR -> 0))
    supervisor ! AddedJobs(inputs._3.toList)
    inputs._3.foreach{ input =>
      inputLOccurrences.get(input.left) match{
        case Some(x) => inputLOccurrences += (input.left -> (x+1))
        case None => ()
      }
      inputROccurrences.get(input.right) match{
        case Some(x) => inputROccurrences += (input.right -> (x+1))
        case None => ()
      }
    }
  }

  override def initConnectors(conf: Config, builder: PlatformBuilder): Unit = {
    // println("Called this")
    val upstreamLConnector = new ActorConnector[InputL]("binary-platform-connector-left") {
      override val innerConnector: Connector[InputL] = builder.connector[InputL]("binary-platform-connector-left")
    }
    upstreamLConnector.init(conf)
    upstreamLConnector.registerPlatform(this)
    upstreamLConnectorOpt = Some(upstreamLConnector)
    val upstreamRConnector = new ActorConnector[InputR]("binary-platform-connector-right") {
      override val innerConnector: Connector[InputR] = builder.connector[InputR]("binary-platform-connector-right")
    }
    upstreamRConnector.init(conf)
    upstreamRConnector.registerPlatform(this)
    upstreamRConnectorOpt = Some(upstreamRConnector)
    val pairConnector = new ActorConnector[protopipes.data.Pair[InputL,InputR]]("binary-platform-connector-pair") {
      override val innerConnector: Connector[protopipes.data.Pair[InputL,InputR]] = builder.connector[protopipes.data.Pair[InputL,InputR]]("binary-platform-connector-pair")
    }
    pairConnector.init(conf)
    // pairConnector.registerPlatform(this)
    pairConnectorOpt = Some(pairConnector)
  }

  override def wake(): Unit = supervisor ! Wake()

  override def terminate(): Unit = {
    actorSystem.terminate
  }

  override def getErrorCurator(): ErrorCurator[protopipes.data.Pair[InputL, InputR]] = getPairErrorCurator()
}
