package protopipes.platforms.instances.bigactors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.Config
import BigActorPlatform.{AddedWorker, AskForJob, PostWake, Wake}
import protopipes.configurations.PlatformBuilder
import protopipes.connectors.Connector
import protopipes.connectors.instances.ActorConnector
import protopipes.data.Identifiable
import protopipes.platforms.{BinaryPlatform, Platform, UnaryPlatform}
import protopipes.store.DataStore

import scala.util.Random

/**
  * Created by chanceroberts on 8/10/17.
  */

object BigActorPlatform{

  val NAME: String = "platform-actor"

  case class Wake()
  case class PostWake()
  case class AskForJob(acRef: ActorRef)
  case class AddedWorker(acRef: ActorRef)

}

class BigActorSupervisorActor[Input <: Identifiable[Input], Output](platform: Platform with Computeable[Input]) extends Actor{
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
          def addActors(numLeft: Int): Unit = numLeft match{
            case x if x > 0 =>
              val aRef = context.actorOf(Props(classOf[BigActorWorkerActor[Input, Output]], platform))
              self ! AddedWorker(aRef)
              addActors(numLeft-1)
            case _ => ()
          }
          addActors(4) //addActors(possiblyInConfig(_, _, 4))
        case _ => workerList foreach(worker => isWorking.get(worker) match{
          case Some(true) => ()
          case _ => self ! AskForJob(worker)
        })
      }
    case ("AddedJob", input: Input@unchecked) => state(jobsLeft ::: List(input), workerList, isWorking)
    case _ => ()
  }

  def receive(): Receive = {
    case _ => throw new Exception("The Big Actor Supervisor is never supposed to be at this state.")
  }
}

class BigActorWorkerActor[Input <: Identifiable[Input], Output <: Identifiable[Output]](platform: BigActorUnaryPlatform[Input, Output]) extends Actor{
  def receive: Receive = {
    case job: Input @ unchecked =>
      platform.compute(job)
      sender() ! AskForJob(self)
  }
}

trait Computeable[Input]{
  def compute(input: Input): Unit = ()
}

abstract class BigActorUnaryPlatform[Input <: Identifiable[Input], Output <: Identifiable[Output]](name: String = BigActorPlatform.NAME) extends UnaryPlatform[Input, Output] with Computeable[Input] {
  implicit val actorSystem = ActorSystem(name)
  var superActorOpt: Option[ActorRef] = None

  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  override def run(): Unit = {
    val upstreamConnector = getUpstreamConnector()
    upstreamConnector.retrieveUp().foreach(input => supervisor ! ("AddedJob", input))
  }

  override def init(conf: Config, inputMap: DataStore[Input], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputMap, outputMap, builder)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[Input, Output]], this)))
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
}

abstract class BigActorBinaryPlatform[InputL <: Identifiable[InputL], InputR <: Identifiable[InputR], Output <: Identifiable[Output]]
(name: String = BigActorPlatform.NAME + s"-binary-${Random.nextInt(99999)}") extends BinaryPlatform[InputL, InputR, Output] with Computeable[protopipes.data.Pair[InputL, InputR]] {
  implicit val actorSystem = ActorSystem(name)
  var superActorOpt: Option[ActorRef] = None
  def supervisor: ActorRef = superActorOpt match{
    case Some(superActor) => superActor
    case None => throw new Exception("The Supervisor Actor does not exist.")
  }

  override def init(conf: Config, inputLMap: DataStore[InputL], inputRMap: DataStore[InputR], outputMap: DataStore[Output], builder: PlatformBuilder): Unit = {
    super.init(conf, inputLMap, inputRMap, outputMap, builder)
    superActorOpt = Some(actorSystem.actorOf(Props(classOf[BigActorSupervisorActor[protopipes.data.Pair[InputL, InputR], Output]], this)))
  }

  override def run(): Unit = {
    getInputs()._3.foreach(input => supervisor ! ("AddedJob", input))
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
}
