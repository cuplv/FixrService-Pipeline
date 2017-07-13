package pipecombi
import akka.actor.{Actor, ActorRef, ActorSystem, Props, ReceiveTimeout, ActorContext}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigObject}

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import collection.JavaConverters._
import pipecombi._

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}

/**
  * Created by edmundlam on 6/20/17.
  */


abstract class Transformer[Input <: Identifiable, Output <: Identifiable](name: String = "")(implicit system: ActorSystem) extends Operator[Input, Output, Output] {
  val stepActor: ActorRef = name match{
    case "" => system.actorOf(Props(new StepActor(this)))
    case _ => system.actorOf(Props(new StepActor(this)), name)
  }
  val nameLength: Int = name.length
  val context: ActorContext = {
    val futContext = stepActor.ask("context")(Timeout(1 second))
    Await.ready(futContext, 1 second)
    futContext.value match{
      case Some(Success(a: ActorContext)) => a
      case _ => null
    }
  }
  def compute(input: Input): List[Output]

  def computeAndStore(inputs: List[Input], outputMap: DataMap[Output]): List[Output] = List.empty[Output] //Override this method

  def getListofInputs(inputMap: DataMap[Input]): List[Input]

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



abstract class IncrTransformer[Input <: Identifiable , Output <: Identifiable](name: String = "")(implicit system: ActorSystem, c: Option[Config] = None) extends Transformer[Input, Output](name) {
  //val actorSys: ActorSystem = ActorSystem.apply(ConfigHelper.possiblyInConfig(c, "ActorSystemName", "Increment"), c)
  //val actorSys: ActorSystem = context.system
  implicit val executionContext = context.system.dispatcher
  val timer = 5 seconds
  val verbose = ConfigHelper.possiblyInConfig(c, name+"_verbosity", default = false) ||
                ConfigHelper.possiblyInConfig(c, "verbosity", default = false)
  val errList = ConfigHelper.possiblyInConfig(c, name+"_exceptionsBlacklist", List.empty[String])
  /*def addAnActor(numberOfCores: Integer, list: List[ActorRef]): List[ActorRef] = numberOfCores match {
    case x if x == 0 => list
    case x => addAnActor(numberOfCores-1, context.actorOf(Props(new FunctionActor(compute, timer)), "FunctionActor"+(numberOfCores-1).toString) :: list)
  }*/
  def addAnActor(actorsLeft: Int, actorString: List[String], actorList: List[ActorRef]): List[ActorRef] = (actorsLeft, actorString) match {
    case (x, Nil) if x <= 0 => actorList
    case (x, Nil) => addAnActor(actorsLeft-1, Nil,
      context.actorOf(Props(new FunctionActor(compute, timer))) :: actorList)
    case (x, curr :: rest) =>
      def loopOverTheChildren(s: String, ac: ActorContext): Option[ActorRef] = s.indexOf('/') match{
        case -1 => Some(ac.actorOf(Props(new FunctionActor(compute, timer)), curr))
        case num =>
          ac.child(s.substring(0, num)) match{
            case Some(aRef: ActorRef) =>
              try {
                val futContext = stepActor.ask("context")(Timeout(1 second))
                Await.ready(futContext, 1 second)
                futContext.value match {
                  case Some(Success(a: ActorContext)) => println(s.substring(num+1)); loopOverTheChildren(s.substring(num+1), a)
                  case _ => None
                }
              } catch{
                case _: Exception => None
              }
            case None => None
          }
      }
      loopOverTheChildren(curr, context) match{
        case Some(ac: ActorRef) => addAnActor(actorsLeft - 1, rest, ac :: actorList)
        case None => addAnActor(actorsLeft, rest, actorList)
      }
  }
  val actorList: List[ActorRef] = c match{
    case None =>  //default case
      //I know I have 4 cores on this machine, so for now, I'm going to say that it's 4.
      //Until I figure out a way to get the number of cores on a machine, this will be the default behavior, I guess...
      addAnActor(4, List(), List())
    case Some(conf) =>
      try {
        val amountOfActorsMin = ConfigHelper.possiblyInConfig(c, name+"_minActors", 4) match{
          case x if x <= 0 => 4
          case x => x
        }
        val newConf = conf.getObject("akka").toConfig.getObject("actor").toConfig.getObject("deployment")
        val actorStringList = newConf.unwrapped().asScala.toList.foldRight(List.empty[String]){
          case ((str, _), list) => str.substring(1, nameLength+2) match{
            case x if x.equals(name+"/") && x.length > 0 => str.substring(nameLength+2) :: list
            case _ => list
          }
        }
        println(actorStringList)
        addAnActor(amountOfActorsMin, actorStringList, List())
        /*
        val newConf = conf.getObject("akka").toConfig.getObject("actor").toConfig.getObject("deployment").toConfig
        val numberOfActors = ConfigHelper.possiblyInConfig(c, "numberOfRemoteActors", 0)
        def buildAnActorList(aList: List[ActorRef], actorsLeft: Int, prefix: String): List[ActorRef] = actorsLeft match {
          case 0 => aList
          case x =>
            buildAnActorList(
              context.system.actorOf(Props(new FunctionActor(compute, timer)), ConfigHelper.possiblyInConfig(c, prefix+actorsLeft, prefix+actorsLeft)) :: aList,
              actorsLeft-1, prefix
            )
        }
        val remoteList = buildAnActorList(List.empty[ActorRef], numberOfActors, "remoteActor")
        //Next step: Local Actors! :)
        val numberOfLocalActors = ConfigHelper.possiblyInConfig(c, "numberOfLocalActors", 0)
        buildAnActorList(remoteList, numberOfLocalActors, "localActor")
        */
      } catch{
        case e: Exception => addAnActor(4, List(), List())
      }
      //Get all of the actors out of the Actor System somehow.
  }
  actorList.foreach{actorRef => println(actorRef.path)}
  val actorListLength = actorList.length
  //implicit val timeout = Timeout(10 hours)

  def computeThenStore(input: List[Input], outputMap: DataMap[Output]): List[Output] = multiComputeThenStore(input, outputMap)

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
        statMap.put(input.identity, Error)
        List()
      }
    }
  }

  def multiComputeThenStore(inputs: List[Input], outputMap: DataMap[Output]): List[Output] = {
    val inputsMaxLength = math.ceil(inputs.length/(actorListLength*1.0)).toInt+1
    //The way I wrote this, each future's index corresponds to inputs.reverse's index.
    def divideOntoActors(inputs: List[Input], actors: List[ActorRef], futures: List[Future[Any]]): (List[Input], List[Future[Any]]) = (inputs.length, actors.length) match {
      case (0, _) => (inputs, futures)
      case (_, 0) => (inputs, futures)
      case (_, x) =>
        val fut = actors.head.ask(inputs.head)(timer*inputsMaxLength)
        val currList = fut :: futures
        divideOntoActors(inputs.tail, actors.tail, currList)
    }

    def getListOfFutureOutputs(inputs: List[Input], actors: List[ActorRef], futures: List[Future[Any]]): (List[Input], List[Future[Any]]) = inputs.length match {
      case 0 => (inputs, futures)
      case x =>
        val (inputsLeft, fs) = divideOntoActors(inputs, actors, futures)
        inputsLeft match{
          case Nil => (inputsLeft, fs)
          case _ => getListOfFutureOutputs(inputsLeft, actors, fs)
        }
    }
    val (_, futures) = getListOfFutureOutputs(inputs, actorList, List.empty[Future[Any]]) //multiComputeThenStoreHelper(inputs, actorList, actorListLength, List.empty[Future[Any]])
    futures.zip(inputs.reverse).foldRight(List.empty[Output]){
      case ((future, input), oList) =>
        //And now, for (arguably) the worst code that I could probably write for this. :\
        //while (!future.isCompleted){} //SPIN
        try {
          Await.ready(future, timer)
          future.value match {
            case Some(Success(e: Exception)) =>
              errMap.put(input.identity, GeneralErrorSummary(e))
              statMap.put(input.identity, Error)
              oList
            case Some(Failure(e: Exception)) =>
              errMap.put(input.identity, GeneralErrorSummary(e))
              statMap.put(input.identity, Error)
              oList
            case Some(Success(l: List[Output])) =>
              l.foldRight(oList) {
                (output, outList) =>
                  provMap.put(output.identity, input.identity)
                  outputMap.put(output)
                  statMap.put(input.identity, Done)
                  output :: outList
              }
            case None =>
              errMap.put(input.identity, GeneralErrorSummary(new Exception("Timed out.")))
              statMap.put(input.identity, Error)
              oList
            case _ =>
              errMap.put(input.identity, GeneralErrorSummary(new Exception("what?"))) //Should NEVER occur.
              statMap.put(input.identity, Error)
              oList
          }
        }
        catch{
          case e: Exception =>
            if (verbose) println(input.identity.id + " timed out.")
            errMap.put(input.identity, GeneralErrorSummary(e))
            statMap.put(input.identity, Error)
            oList
        }
    }.reverse
  }

  def getListofInputs(inputMap: DataMap[Input]): List[Input] = {
    inputMap.identities.flatMap(
      inputId => statMap.get(inputId) match {
        case Some(stat) => {
          // println(stat)
          stat match {
            case Done => {
              // Already done, log this if verbosity is requested
              if (verbose) println(inputId.id + (inputId.version match{
                case Some(x: String) => "Version " + x
                case None => ""
              }) + " has already been completed!")
              None
            }
            case Error if (errMap.get(inputId) match{ //Error Blacklist
              case Some(e) =>
                def falseLoop(errList: List[String]): Boolean = errList match{
                  case Nil => false
                  case errSum :: rest => if (errSum.equals(e.identity().id)) true else falseLoop(rest)
                }
                falseLoop(errList)
              case None => false //Assume that the error status was a fluke.
            }) => None //There's nothing we can do here.
            case default => {
              // Status is either NotDone or Error. For now, just recompute in both cases
              inputMap.get(inputId) match {
                case Some(input) => Some(input)
                case None => {
                  // Missing input data, log this as an 'data inconsistent' exception
                  errMap.put(inputId, GeneralErrorSummary(new Exception("data inconsistent")))
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
  }

  def process(inputMap: DataMap[Input], outputMap : DataMap[Output]): DataMap[Output] = {
    // println("Process started: " + inputMap.identities)
    val inputs = getListofInputs(inputMap)
    // println(inputs)
    //inputs.flatMap( tryComputeThenStore(_, outputMap) )
    multiComputeThenStore(inputs, outputMap)
    //actorSys.terminate
    outputMap
  }
}

class FunctionActor[Input <: Identifiable, Output <: Identifiable](func: Input=>List[Output], timeout: Duration) extends Actor{
  context.setReceiveTimeout(timeout)
  def receive = {
    case "context" => sender() ! context
    case (s: String, "YourNewChild") =>
      try {
        context.actorOf(Props(new FunctionActor(func, timeout)), s)
      } catch {
        case e: Exception => sender() ! e
      }
    case i: Input =>
      try {
        val output = func(i)
        sender() ! output
      } catch {
        case e: Exception => sender() ! e
      }
    case _ => List.empty[Output]
  }
}

class StepActor[Input <: Identifiable, Output <: Identifiable](t: Transformer[Input, Output]) extends Actor {
  def receive = {
    case "context" => sender() ! context
    case (dmI: DataMap[Input], dmO: DataMap[Output], aRef: ActorRef) =>
      try{
        aRef ! (t.process(dmI, dmO), "output")
        aRef ! "readyToCompose"
      } catch {
        case e: Exception => aRef ! (e, "output")
      }
    case (dmI: DataMap[Input], dmO: DataMap[Output], n: Int, aRef: ActorRef) =>
      try{
        //Only send n things inside the DataMap
        def sendBatches(lI: List[Input], dmO: DataMap[Output], n: Int): DataMap[Output] = {
          val (currBatch, nextBatch) = lI.splitAt(n)
          t.computeAndStore(currBatch, dmO)
          aRef ! (dmO, "output")
          nextBatch match{
            case Nil => dmO
            case _ => sendBatches(nextBatch, dmO, n)
          }
        }
        sendBatches(t.getListofInputs(dmI), dmO, n)
        aRef ! "readyToCompose"
      } catch {
        case e: Exception => aRef ! (e, "output")
      }
  }
}


