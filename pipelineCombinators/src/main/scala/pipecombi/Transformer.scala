package pipecombi
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.Config

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import pipecombi._

import scala.concurrent.{Await, Future}

/**
  * Created by edmundlam on 6/20/17.
  */


abstract class Transformer[Input <: Identifiable, Output <: Identifiable] extends Operator[Input, Output, Output] {
  def compute(input: Input): List[Output]

  def process(iFeat: DataMap[Input], oFeat: DataMap[Output]): DataMap[Output]
  override def operate(arg1: DataMap[Input], arg2: DataMap[Output]): DataMap[Output] = process(arg1,arg2)

  // def --> (output: DataMap[Output]): Transformation[Input, Output] = Transformation(this, output)

  def -->(output: DataMap[Output]): PartialTransformationPipe[Input, Output] = PartialTransformationPipe(this, output)
}


/*
case class Transformation[Input <: Identifiable,Output <: Identifiable](proc: Transformer[Input,Output], output: DataMap[Output]) {
  def ~(other: Transformation[Input, Output]): List[Transformation[Input,Output]] = {
    List(this,other)
  }
  def ~(others: List[Transformation[Input, Output]]): List[Transformation[Input,Output]] = {
    this +: others
  }
} */



abstract class IncrTransformer[Input <: Identifiable , Output <: Identifiable](c: Option[Config] = None) extends Transformer[Input, Output] {
  val actorSys: ActorSystem = ActorSystem.apply(ConfigHelper.possiblyInConfig(c, "ActorSystemName", ""), c)
  implicit val executionContext = actorSys.dispatcher
  val actorList: List[ActorRef] = c match{
    case None =>  //default
      //I know I have 4 cores on this machine, so for now, I'm going to say that it's 4.
      //Until I figure out a way to get the number of cores on a machine, this will be the default behavior, I guess...
      def addAnActor(numberOfCores: Integer, list: List[ActorRef]): List[ActorRef] = {
        addAnActor(numberOfCores-1, actorSys.actorOf(Props(new FunctionActor(compute)), "FunctionActor"+(numberOfCores-1).toString) :: list)
      }
      addAnActor(4, List.empty[ActorRef])
    case Some(conf) =>
      try {
        val newConf = conf.getObject("akka").toConfig.getObject("actor").toConfig.getObject("deployment").toConfig
        val numberOfActors = ConfigHelper.possiblyInConfig(c, "numberOfRemoteActors", 0)
        def buildAnActorList(aList: List[ActorRef], actorsLeft: Int): List[ActorRef] = actorsLeft match {
          case 0 => aList
          case x => actorSys.actorOf(Props(new FunctionActor(compute)), ConfigHelper.possiblyInConfig(c, "remoteActor"+actorsLeft, "remoteActor"+actorsLeft)) :: aList
        }
        val remoteList = buildAnActorList(List.empty[ActorRef], numberOfActors)
        //Next step: Local Actors! :)
        ???
      } catch{
        case e: Exception => ???
      }
      //Get all of the actors out of the Actor System somehow.
  }
  //implicit val timeout = Timeout(10 hours)
  def tryComputeThenStore(input: Input, outputMap: DataMap[Output]): List[Output] = {
    try {
      compute(input).map(
        output => {
          provMap.put(output.identity, input.identity)
          outputMap.put(output)
          statMap.put(input.identity, Done)
          output
        }
      )
    } catch {
      case ex:Exception => {
        // Compute exception occurred, log this in error store
        errMap.put(input.identity, GeneralErrorSummary(ex))
        List()
      }
    }
  }

  def multiComputeThenStore(inputs: List[Input], outputMap: DataMap[Output]): List[Output] = {
    //The way I wrote this, each future's index corresponds to inputs.reverse's index.
    def multiComputeThenStoreHelper(inputs: List[Input], actors: List[ActorRef], maxCores: Integer, futures: List[Future[Any]]): (List[Input], List[Future[Any]]) = (inputs.length, actors.length) match {
      case (0, _) => (inputs, List.empty[Future[List[Output]]])
      case (_, 0) => (inputs, List.empty[Future[List[Output]]])
      case (_, n) =>
        val fut = actors.head ? inputs.head
        val currList = fut :: futures
        val (inputsLeft, fs) = multiComputeThenStoreHelper(inputs.tail, actors.tail, maxCores, currList)
        (inputsLeft, n) match{
          case (lis, _) if lis.isEmpty => (lis, fs)
          case (_, x) if x == maxCores => multiComputeThenStoreHelper(inputsLeft, actors, maxCores, fs)
          case (_, x) => (inputs.tail, fs)
        }
    }
    val (_, futures) = multiComputeThenStoreHelper(inputs, actorList, actorList.length, List.empty[Future[Any]])
    futures.zip(inputs.reverse).foldRight(List.empty[Output]){
      case ((future, input), oList) =>
        //And now, for (arguably) the worst code that I could probably write for this. :\
        //while (!future.isCompleted){} //SPIN
        Await.ready(future, 1 hour)
        future.value match {
          case Some(Success(e: Exception)) =>
            errMap.put(input.identity, GeneralErrorSummary(e))
            oList
          case Some(Success(l: List[Output])) =>
            l.foldRight(oList){
              (output, outList) =>
                provMap.put(output.identity, input.identity)
                outputMap.put(output)
                statMap.put(input.identity, Done)
                output :: outList
            }
          case Some(Failure(e: Exception)) | None => //I guess I might be able to see a way that this could occur? (Timeout, for example)
            errMap.put(input.identity, GeneralErrorSummary(e))
            oList
          case _ =>
            errMap.put(input.identity, GeneralErrorSummary(new Exception("what?"))) //Should NEVER occur.
            oList
        }
    }.reverse
  }

  def process(inputMap: DataMap[Input], outputMap : DataMap[Output]): DataMap[Output] = {
    // println("Process started: " + inputMap.identities)
    val inputs = inputMap.identities.flatMap(
      inputId => statMap.get(inputId) match {
        case Some(stat) => {
          // println(stat)
          stat match {
            case Done => {
              // Already done, log this if verbosity is requested
              None
            }
            case default => {
              // Status is either NotDone or Error. For now, just recompute in both cases
              inputMap.get(inputId) match {
                case Some(input) => Some(input)
                case None => {
                  // Missing input data, log this as an 'data inconsistent' exception
                  None
                }
              }
            }
          }
        }
        case None => {
          // New input data entry, extend status map and proceed as though this input has not been done
          // println(inputId)
          statMap.put(inputId, NotDone)
          inputMap.get(inputId) match {
            case Some(input) => Some(input)
            case None => {
              // Missing input data, log this as an 'data inconsistent' exception
              None
            }
          }
        }
      }
    )
    // println(inputs)
    inputs.flatMap( tryComputeThenStore(_, outputMap) )
    outputMap
  }
}

class FunctionActor[Input <: Identifiable, Output <: Identifiable](func: Input=>List[Output]) extends Actor{
  def receive = {
    case Some(i: Input) =>
      try {
        val output = func(i)
        sender() ! output
      } catch {
        case e: Exception => sender() ! e
      }
    case _ => List.empty[Output]
  }
}



