package mthread_abstrac

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import com.typesafe.config.Config

import collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Created by chanceroberts on 7/24/17.
  */
class AkkaAbstraction[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                                                  succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit, config: Option[Config]) extends MThreadAbstraction[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config) {
  val fixrConfig: Option[Config] = ConfigHelper.possiblyInConfig(config, "fixr", None)
  val system: ActorSystem = ActorSystem.apply(ConfigHelper.possiblyInConfig(fixrConfig, "name", "default"), config)
  val supervisor: ActorRef = system.actorOf(Props(new AkkaSupervisor[DMIn, DMOut, Input, Output](getListOfInputs, compute, succ, fail, config)))

  override def send(message: Any): Boolean = {
    supervisor ! message
    true
  }
  //override def !(message:Any): Unit = supervisor ! message
}

class AkkaSupervisor[DMIn, DMOut, Input, Output](getListOfInputs: DMIn => List[Input], compute: Input => List[Output],
                                             succ: (Input, List[Output], DMOut) => DMOut, fail: (Input, Exception) => Unit, config: Option[Config])
                                              extends Supervisor(getListOfInputs, compute, succ, fail) with Actor {
  val akkaConfig: Option[Config] = ConfigHelper.possiblyInConfig(config, "akka", None)
  val fixrConfig: Option[Config] = ConfigHelper.possiblyInConfig(config, "fixr", None)
  val n: Int = ConfigHelper.possiblyInConfig(fixrConfig, "batchSize", Int.MaxValue)
  def newState(dmI: DMIn, dmO: DMOut, pipeAct: ActorRef,
               acList: List[ActorRef] = List(), isReady: Boolean = true, jobsLeft: Int = 0, jobsDoneInBatch: Int = 0): Receive = {
    case ("init", dmI: DMIn @ unchecked, dmO: DMOut @ unchecked, aRef: ActorRef) if acList.isEmpty =>
      context.become(newState(dmI, dmO, aRef))
    case "addedJob" => context.become(newState(dmI, dmO, pipeAct, acList, true, jobsLeft+1, jobsDoneInBatch))
    case "input" => (acList, isReady) match{
      case (Nil, _) =>
        val acList = createWorkers
        giveJobsToWorkers(getListOfInputs(dmI), acList)
        context.become(newState(dmI, dmO, pipeAct, acList))
      case (_, true) => giveJobsToWorkers(getListOfInputs(dmI), acList)
      case (_, false) => self ! "input"
    }
    case ("crashed", aRef: ActorRef, Some(input: Input @ unchecked)) if isReady =>
      fail(input, new Exception("An actor has crashed trying to compute " + input + "!"))
      if (jobsLeft-1 == 0) self ! "checkAgain"
      context.become(newState(dmI, dmO, pipeAct, acList, true, jobsLeft-1, jobsDoneInBatch))
    case ("crashed", aRef: ActorRef, _) => ()
    case ("exception", input: Input @ unchecked, e: Exception) =>
      fail(input, e)
      if (jobsLeft-1 == 0) self ! "checkAgain"
      context.become(newState(dmI, dmO, pipeAct, acList, true, jobsLeft-1, jobsDoneInBatch))
    case ("finished", input: Input @ unchecked, outputs: List[Output] @ unchecked) =>
      succ(input, outputs, dmO)
      if (jobsLeft-1 == 0) self ! "checkAgain"
      if (jobsDoneInBatch+1 >= n){
        pipeAct ! "output"
        context.become(newState(dmI, dmO, pipeAct, acList, true, jobsLeft-1))
      } else {
        context.become(newState(dmI, dmO, pipeAct, acList, true, jobsLeft-1, jobsDoneInBatch+1))
      }
    case "checkAgain" => if (jobsLeft == 0){ //Quietness, for now at least.
      if (jobsDoneInBatch > 0) pipeAct ! "output"
      //Unbuild everything.
      acList.foreach{ actor => actor ! PoisonPill }
      context.become(newState(dmI, dmO, pipeAct, List(), false))
    }
    case Terminated(aRef: ActorRef) =>
      context.become(newState(dmI, dmO, pipeAct, acList.diff(List(aRef)), false))
    case other => println(other + " was sent with nothing occuring.")
  }

  def receive: Receive = {
    case ("init", dmI: DMIn @ unchecked, dmO: DMOut @ unchecked, aRef: ActorRef) => context.become(newState(dmI, dmO, aRef))
    case "context" => sender() ! context
    case other => println(other + " was sent with nothing occurring.")
  }

  def addWorkers(list: List[String], minActors: Int, timeout: Duration): List[ActorRef] = (list, minActors) match{
    case (Nil, x) if x <= 0 => List()
    case (Nil, x) =>
      val aRef = context.actorOf(Props(new AkkaWorker[Input, Output](compute, timeout)))
      context.watch(aRef)
      aRef :: addWorkers(Nil, x - 1, timeout)
    case (head :: rest, x) =>
      try {
        val aRef = context.actorOf(Props(new AkkaWorker[Input, Output](compute, timeout)))
        context.watch(aRef)
        aRef :: addWorkers(rest, x - 1, timeout)
      } catch {
        case e: Exception =>
          println(s"Exception $e occurred when attempting to create worker $head.")
          addWorkers(rest, x, timeout)
      }
  }

  def createWorkers: List[ActorRef] = {
    val minActors = ConfigHelper.possiblyInConfig(fixrConfig, "minActors", 4)
    val duration = ConfigHelper.possiblyInConfig(fixrConfig, "durationInSeconds", 10).seconds
    val listOfActors: List[String] = akkaConfig match{
      case None => List()
      case Some(conf) =>
        try{
          val newConf = conf.getObject("actor").toConfig.getObject("deployment")
          newConf.unwrapped().asScala.toList.foldRight(List.empty[String]){
            case ((str, _), list) => str.substring(0,7) match{
              case "/super/" => str.substring(7) :: list
              case _ => list
            }
          }
        }
        catch{
          case e: Exception => List()
        }
    }
    addWorkers(listOfActors, minActors, duration)
  }

  def giveJobsToWorkers(jobs: List[Input], acList: List[ActorRef]): List[ActorRef] = {
    def divideOntoActors(inputs: List[Input], actors: List[ActorRef]): (List[Input], List[ActorRef]) = (inputs.length, actors.length) match {
      case (0, _) => (inputs, actors)
      case (_, 0) => (inputs, List())
      case (_, x) =>
        self ! "addedJob"
        actors.head ! inputs.head
        divideOntoActors(inputs.tail, actors.tail)
    }
    val (newInputs, nextActorRef) = divideOntoActors(jobs, acList)
    newInputs match {
      case Nil => nextActorRef ::: acList.diff(nextActorRef) //This only works due to the list not having the same actors within it.
      case x :: _ => giveJobsToWorkers(newInputs, acList)
    }
  }
}

class AkkaWorker[Input, Output](compute: Input => List[Output], timeout: Duration) extends Actor {
  context.setReceiveTimeout(timeout)
  var currInput: Option[Input] = None
  override def postStop(): Unit = {
    super.postStop()
    context.parent ! ("crashed", self, currInput)
  }
  def receive: Receive = {
    case "context" => sender() ! context
    case i: Input@unchecked =>
      try {
        currInput = Some(i)
        val output = compute(i)
        context.parent ! ("finished", i, output)
        currInput = None
      } catch {
        case e: Exception => context.parent ! ("exception", i, e)
      }
    case _ => List.empty[Output]
  }
}