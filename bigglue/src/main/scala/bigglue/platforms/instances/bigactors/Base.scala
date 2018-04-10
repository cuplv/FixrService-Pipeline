package bigglue.platforms.instances.bigactors

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import BigActorPlatform._
import bigglue.configurations.{ConfOpt, Constant, PipeConfig, PlatformBuilder}
import bigglue.connectors.{Connector, Status}
import bigglue.connectors.instances.{ActorConnector, WaitingConnector}
import bigglue.curators.ErrorCurator
import bigglue.data.Identifiable
import bigglue.exceptions.NotInitializedException
import bigglue.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import bigglue.store.DataStore

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
      case _: Exception => None
    }
  }

  def possiblyHasObject(config: Config, path: String): Option[ConfigObject] = {
    try{
      Some(config.getObject(path))
    }
    catch{
      case _: Exception => None
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
  case class DoNotAlertSupervisor()
  case class Terminate()

}

class BigActorSupervisorActor[Input <: Identifiable[Input], Output](platform: Platform with BigActor[Input],
                                                                    actorNames: List[String] = List()) extends Actor{
  import context._
  become(state(Nil, Nil))
  def state(jobsLeft: List[Input], workerList: List[ActorRef], isWorking: Map[ActorRef, Boolean] = Map(), gettingUp: Boolean = false): Receive = {
    case AddedWorker(worker: ActorRef) =>
      jobsLeft match{
        case job :: rest =>
          worker ! job
          become(state(rest, worker :: workerList, isWorking+(worker->true)))
        case Nil => become(state(jobsLeft, worker :: workerList, isWorking+(worker->false)))
      }
    case AskForJob(worker: ActorRef) => jobsLeft match{
      case Nil =>
        become(state(Nil, workerList, isWorking+(worker->false)))
      case newJob :: rest =>
        worker ! newJob
        become(state(rest, workerList, isWorking+(worker->true)))
    }
    case Wake() =>
      platform.run()
      self ! PostWake()
    case PostWake() =>
      (workerList, gettingUp) match{
        case (Nil, false) =>
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
          become(state(jobsLeft, workerList, isWorking, gettingUp = true))
        case (Nil, true) => ()
        case _ => workerList foreach(worker => isWorking.get(worker) match{
          case Some(true) => ()
          case _ => self ! AskForJob(worker)
        })
      }
    case (Crashed(acRef: ActorRef, exception: Exception), input: Input @ unchecked) =>
      platform.getErrorCurator().reportError(input, exception)
      self ! AskForJob(acRef)
    case Crashed(_: ActorRef, _: Exception) => ()
    case Terminate() =>
      workerList.foreach(_ ! DoNotAlertSupervisor())
      workerList.foreach(_ ! PoisonPill)
    case AddedJobs(inputs: List[Input@unchecked]) =>
      val (newJobs, newIsWorking) = giveJobsToWorkers(jobsLeft ::: inputs, workerList, isWorking)
      become(state(newJobs, workerList, newIsWorking, gettingUp))
    case ("AddedJob", input: Input@unchecked) =>
      val (newJobs, newIsWorking) = giveJobsToWorkers(jobsLeft ::: List(input), workerList, isWorking)
      become(state(newJobs, workerList, newIsWorking, gettingUp))
    case other => println(s"$other was sent with nothing occurring.")
  }

  def receive(): Receive = {
    case _ => throw new Exception("The Big Actor Supervisor is never supposed to be at this state.")
  }

  def giveJobsToWorkers(jobs: List[Input], workers: List[ActorRef], isWorking: Map[ActorRef, Boolean]): (List[Input], Map[ActorRef, Boolean]) = (jobs, workers) match{
    case (Nil, _) | (_, Nil) => (jobs, isWorking)
    case (job :: otherJobs, worker :: rest) if !isWorking(worker) =>
      worker ! job
      giveJobsToWorkers(otherJobs, rest, isWorking+(worker->true))
    case (_, worker :: rest) =>
      giveJobsToWorkers(jobs, rest, isWorking)
  }
}

/**
  * The actors that actually do the work of computing the tasks.
  * @param platform The platform that will compute the data.
  * @tparam Input The type of the data that's being sent in. This needs to be an [[Identifiable]] type.
  * @tparam Output The type of the data that's being sent out. This needs to be an [[Identifiable]] type.
  */
class BigActorWorkerActor[Input <: Identifiable[Input], Output <: Identifiable[Output]](platform: Platform with BigActor[Input]) extends Actor{
  var alertSupervisor: Boolean = true
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
      case _ if alertSupervisor => context.parent ! Crashed(self, new Exception(s"An actor has decided to stop!"))
      case _ => ()
    }
  }

  def receive: Receive = {
    case DoNotAlertSupervisor() => alertSupervisor = false
    case job: Input @ unchecked =>
      currInput = Some(job)
      longestTimeWaiting match{
        case Duration.Inf =>
          try {
            platform.compute(job)
          } catch {
            case e: Exception => platform.getErrorCurator().reportError(job, e)
          }
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
      "maxHoursOnInput" -> 0).asJava)
  implicit var actorSystemOpt: Option[ActorSystem] = None//ActorSystem(name)
  var computerOpt: Option[ActorRef] = None
  def computer: ActorRef = computerOpt match{
    case Some(actorRef) => actorRef
    case None => throw new NotInitializedException("computer", "Computing", None)
  }
  def compute(job: Input): Unit = computer ! job
  def compute_(input: Input): Unit = ()
  def getErrorCurator(): ErrorCurator[Input]
  implicit def actorSystem: ActorSystem = actorSystemOpt match{
    case Some(sys: ActorSystem) => sys
    case None => throw new Exception("The Actor System does not exist.")
  }
  def updateConfigAndGetActorNames(conf: Config, name: String): List[String] = {
    ConfigHelper.possiblyAConfig(conf.getConfig(Constant.BIGGLUE), "_bigactor") match {
      case Some(overwriteConfig) =>
        infoConfig = PipeConfig.resolveOptions(infoConfig, ConfOpt.typesafeConfig(overwriteConfig))
        actorSystemOpt = Some(ActorSystem(name, PipeConfig.resolveOptions(conf, ConfOpt.typesafeConfig(overwriteConfig))))
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

class BigActorWorker[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(platform: Platform with BigActor[Input]) extends Actor{

  def receive: Receive = {
    case job: Input @ unchecked => platform.compute_(job)
  }
}

/**
  * This is the default platform of computation. This attempts to compute through a list of inputs asynchronously.
  * In more detail, Has a supervisor [[BigActorSupervisorActor]] and a list of workers, [[BigActorWorker]].
  * However, these are not necessary to understand the example, and is just an implementation detail.
  * @param name The name of the platform. This is usually "platform-actor-unary-" followed by a random ID.
  * @tparam Input The type of the data that's being sent in. This needs to be an [[Identifiable]] type.
  * @tparam Output The type of the data that's being sent out. This needs to be an [[Identifiable]] type.
  */
abstract class BigActorUnaryPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME+s"-unary-${Random.nextInt(99999)}") extends UnaryPlatform[Input, Output] with BigActor[Input] {
  var superActorOpt: Option[ActorRef] = None
  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  /**
    * This gets the inputs that need to be computed with [[getInputs]].
    * Then, it sends that to the supervisor to compute that asynchronously.
    * All of the workers will end up calling [[BigActorMapperPlatform.compute_]] or [[BigActorReducerPlatform.compute_]]
    * when they get a job to compute depending on the computation of the platform.
    */
  override def run(): Unit = {
    supervisor ! AddedJobs(getInputs().toList)
  }

  /**
    * This sets up the platform by connecting the Input Map and Output Map to the platform, as well as
    * any other initialization that needs to be done
    * Along with that, it also sets up the actor system, and gets the list of all of the worker actors.
    * @param conf The configuration file needed to initialize.
    * @param inputMap The Map that data gets sent in from.
    * @param outputMap The Map that data gets sent out to.
    * @param builder The builder that created the platform. This was called with [[bigglue.computations.Mapper.init]] or [[bigglue.computations.Reducer.init]]
    */
  override def init(conf: PipeConfig, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    val listOfActors = updateConfigAndGetActorNames(conf.typeSafeConfig, name)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[Input, Output]], this, listOfActors), "super"))
    computerOpt = Some(actorSystem.actorOf(Props(classOf[BigActorWorker[Input, Output]], this), "computer"))
    super.init(conf, inputMap, outputMap, builder)
  }

  /**
    * This just initializes the connectors and connects them to the platform.
    * @param conf The configuration file needed to initialize.
    * @param builder The builder that created the platform. This was called with [[bigglue.computations.Mapper.init]] or [[bigglue.computations.Reducer.init]]
    */
  override def initConnector(conf: PipeConfig, builder: PlatformBuilder): Unit = {
    val upstreamConnector = new ActorConnector[Input] {
      override val innerConnector: Connector[Input] = builder.connector[Input]("BigActorConnector")
    }
    upstreamConnector.init(conf)
    upstreamConnector.registerPlatform(this)
    upstreamConnectorOpt = Some(upstreamConnector)
  }

  /**
    * This function is called when the upstream connector lets the platform know that there's data to be computed.
    * This allows the supervisor actor to start working on stuff.
    * Without going into the messy details of the ActorSystem, this calls [[run]].
    */
  override def wake(): Unit = supervisor ! Wake()

  /**
    * This function is called when closing down the pipeline.
    * This sends a call to kill the actor system and the workers.
    */
  override def terminate(): Unit = {
    supervisor ! Terminate()
    actorSystem.terminate
  }
}

abstract class BigActorBinaryPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + s"-binary-${Random.nextInt(99999)}") extends BinaryPlatform[InputL, InputR, Output] with BigActor[bigglue.data.Pair[InputL, InputR]] {
  var inputLOccurrences: Map[InputL, Integer] = Map()
  var inputROccurrences: Map[InputR, Integer] = Map()
  var superActorOpt: Option[ActorRef] = None
  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  override def init(conf: PipeConfig, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    val listOfActors = updateConfigAndGetActorNames(conf.typeSafeConfig, name)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[bigglue.data.Pair[InputL, InputR], Output]], this, listOfActors), "super"))
    computerOpt = Some(actorSystem.actorOf(Props(classOf[BigActorWorker[bigglue.data.Pair[InputL, InputR], Output]], this), "computer"))
    super.init(conf, inputLMap, inputRMap, outputMap, builder)
  }

  override def compute_(input: bigglue.data.Pair[InputL, InputR]): Unit = {
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

  override def initConnectors(conf: PipeConfig, builder: PlatformBuilder): Unit = {
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
    val pairConnector = new ActorConnector[bigglue.data.Pair[InputL,InputR]]("binary-platform-connector-pair") {
      override val innerConnector: Connector[bigglue.data.Pair[InputL,InputR]] = builder.connector[bigglue.data.Pair[InputL,InputR]]("binary-platform-connector-pair")
    }
    pairConnector.init(conf)
    // pairConnector.registerPlatform(this)
    pairConnectorOpt = Some(pairConnector)
  }

  override def wake(): Unit = supervisor ! Wake()

  override def terminate(): Unit = {
    supervisor ! Terminate()
    actorSystem.terminate
  }

  override def getErrorCurator(): ErrorCurator[bigglue.data.Pair[InputL, InputR]] = getPairErrorCurator()
}
