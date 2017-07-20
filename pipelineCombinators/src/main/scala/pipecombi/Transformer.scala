package pipecombi
import java.io.File

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.util.{Success}
import collection.JavaConverters._
import pipecombi._

import scala.concurrent.{Await, Future}

/**
  * Created by edmundlam on 6/20/17.
  */


abstract class Transformer[Input <: Identifiable, Output <: Identifiable](name: String = "", conf: Any = "")(implicit system: ActorSystem) extends Operator[Input, Output, Output] {
  val c: Option[Config] = conf match{
    case "" => None
    case s: String => Some(ConfigFactory.parseFile(new File(s)))
    case c: Config => Some(c)
    case _ => None
  }
  val stepActor: ActorRef = name match{
    case "" => ActorSystem.apply("default", c).actorOf(Props(new TransStepActor(this)))
    case _ => ActorSystem.apply("default", c).actorOf(Props(new TransStepActor(this, ConfigHelper.possiblyInConfig(c, "batchSize", Int.MaxValue))), "super")
  }
  val nameLength: Int = "super".length //name.length
  val context: ActorContext = {
    val futContext = stepActor.ask("context")(Timeout(1 second))
    Await.ready(futContext, 1 second)
    futContext.value match{
      case Some(Success(a: ActorContext)) => a
      case _ => null
    }
  }

  def computeActorList: List[ActorRef]

  def compute(input: Input): List[Output]

  def computeAndStore(inputs: List[Input], outputMap: DataMap[Output]): Unit = () //Override this method

  def getListofInputs(inputMap: DataMap[Input]): List[Input]

  def process(iFeat: DataMap[Input], oFeat: DataMap[Output], actorList: List[ActorRef]): DataMap[Output]
  override def operate(arg1: DataMap[Input], arg2: DataMap[Output]): DataMap[Output] = process(arg1,arg2,List())

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



abstract class IncrTransformer[Input <: Identifiable , Output <: Identifiable](name: String = "", conf: Any = "")(implicit system: ActorSystem) extends Transformer[Input, Output](name, conf) {
  //val actorSys: ActorSystem = ActorSystem.apply(ConfigHelper.possiblyInConfig(c, "ActorSystemName", "Increment"), c)
  //val actorSys: ActorSystem = context.system
  implicit val executionContext = context.system.dispatcher
  val timer = 5 seconds
  val verbose = //ConfigHelper.possiblyInConfig(c, name+"_verbosity", default = false) ||
                ConfigHelper.possiblyInConfig(c, "verbosity", default = false)
  val errList = ConfigHelper.possiblyInConfig(c, "exceptionsBlacklist", List.empty[String])
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
                  case Some(Success(a: ActorContext)) => loopOverTheChildren(s.substring(num+1), a)
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
  /*
  def makeNestedMapAList(map: Map[String, Any], prefix: String = ""): List[String] = map.foldRight(List.empty[String]){
    case ((str, res), list) => res match {
      case "/" => println("Prefix: " + prefix+str); prefix+str :: list
      case m: Map[String @ unchecked, Any] =>
        println("Looping through " + str)
        val anotherList = makeNestedMapAList(m)
        list ::: anotherList
      case _ => list
    }
  }
  def addToNestedMap(map: Map[String, Any], str: String): Map[String, Any] = str.indexOf('/') match{
    case -1 => map + (str -> "/")
    case num => val newStr = str.substring(num+1)
      map.get(str.substring(0, num)) match {
        case Some("/") => map + (str -> addToNestedMap(Map[String, Any]() + ("/" -> ""), newStr.substring(0, str.indexOf('/'))))
        case None => map + (str -> addToNestedMap(Map[String, Any](), newStr.substring(0, str.indexOf('/'))))
      }
  }
  */
  override def computeActorList: List[ActorRef] = c match{
    case None =>  //default case
      //I know I have 4 cores on this machine, so for now, I'm going to say that it's 4.
      //Until I figure out a way to get the number of cores on a machine, this will be the default behavior, I guess...
      addAnActor(4, List(), List())
    case Some(conf) =>
      try {
        val amountOfActorsMin = ConfigHelper.possiblyInConfig(c, "minActors", 4) match{
          case x if x <= 0 => 4
          case x => x
        }
        val newConf = conf.getObject("akka").toConfig.getObject("actor").toConfig.getObject("deployment")
        val actorStringList = newConf.unwrapped().asScala.toList.foldRight(List.empty[String]){
          case ((str, _), list) => str.substring(1, nameLength+2) match{
            case x if x.equals("super/") && x.length > 0 =>  str.substring(nameLength+2) :: list//addToNestedMap(map, str.substring(nameLength+2))
            case _ => list
          }
        }
        /*val actorStringList = newConf.unwrapped().asScala.toList.foldRight(Map.empty[String, Any]){
          case ((str, _), map) => str.substring(1, nameLength+2) match{
            case x if x.equals(name+"/") && x.length > 0 => addToNestedMap(map, str.substring(nameLength+2))
            case _ => map
          }
        }*/
        addAnActor(amountOfActorsMin, actorStringList, List())
      } catch{
        case e: Exception => addAnActor(4, List(), List())
      }
      //Get all of the actors out of the Actor System somehow.
  }
  //actorList.foreach{actorRef => println(actorRef.path)}
  //val actorListLength = actorList.length
  //implicit val timeout = Timeout(10 hours)

  def computeThenStore(input: List[Input], outputMap: DataMap[Output], actorList: List[ActorRef]): Unit = startMultiCompute(input, outputMap, actorList)

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

  def startMultiCompute(inputs: List[Input], outputMap: DataMap[Output], actorList: List[ActorRef]): List[ActorRef]  = {
    def divideOntoActors(inputs: List[Input], actors: List[ActorRef]): (List[Input], List[ActorRef]) = (inputs.length, actors.length) match {
      case (0, _) => (inputs, actors)
      case (_, 0) => (inputs, List())
      case (_, x) =>
        stepActor ! "addedJob"
        actors.head ! inputs.head
        divideOntoActors(inputs.tail, actors.tail)
    }
    val (newInputs, nextActorRef) = divideOntoActors(inputs, actorList)
    newInputs match{
      case Nil => nextActorRef ::: actorList.diff(nextActorRef) //This only works due to the list not having the same actors within it.
      case x :: _ => startMultiCompute(newInputs, outputMap, actorList)
    }
  }
  /*
  def multiComputeThenStore(inputs: List[Input], outputMap: DataMap[Output]): Unit = {
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
  */
  def getListofInputs(inputMap: DataMap[Input]): List[Input] = {
    inputMap.identities.flatMap(
      inputId => statMap.get(inputId) match {
        case Some(stat) => {
          // println(stat)
          stat match {
            case Done => {
              // Already done, log this if verbosity is requested
              if (verbose) println(inputId.id + (inputId.version match{
                case Some(x: String) => " Version " + x
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

  override def process(inputMap: DataMap[Input], outputMap : DataMap[Output], actorList: List[ActorRef]): DataMap[Output] = {
    // println("Process started: " + inputMap.identities)
    val inputs = getListofInputs(inputMap)
    // println(inputs)
    //inputs.flatMap( tryComputeThenStore(_, outputMap) )
    startMultiCompute(inputs, outputMap, actorList)
    //actorSys.terminate
    outputMap
  }
}

class FunctionActor[Input <: Identifiable, Output <: Identifiable](func: Input=>List[Output], timeout: Duration) extends Actor{
  context.setReceiveTimeout(timeout)
  var currInput: Option[Input] = None
  override def postStop(): Unit = {
    super.postStop()
    context.parent ! (self, "crashed", currInput)
  }
  def receive: Receive = {
    case "context" => sender() ! context
    case i: Input @ unchecked =>
      try {
        currInput = Some(i)
        val output = func(i)
        //println(context.parent.path)
        context.parent ! (i, output, "finished")
        currInput = None
      } catch {
        case e: Exception => context.parent ! (i, e, "exception")
      }
    case _ => List.empty[Output]
  }
}

class TransStepActor[Input <: Identifiable, Output <: Identifiable](t: Transformer[Input, Output], n: Int = Int.MaxValue) extends Actor {
  import context._
  def newState(dmI: DataMap[Input], dmO: DataMap[Output], aRef: ActorRef,
              acList: List[ActorRef], isReady: Boolean, jobsLeft: Int = 0, jobsDoneInBatch: Int = 0): Receive = {
    case (aRef: ActorRef, "crashed", Some(input: Input)) if isReady =>
      t.errMap.put(input.identity(), GeneralErrorSummary(new Exception("The actor failed to compute it.")))
      t.statMap.put(input.identity(), Error)
      if (jobsLeft-1 <= 0) self ! "checkAgain"
      become(newState(dmI, dmO, aRef, acList, true, jobsLeft-1, jobsDoneInBatch))
    case (aRef: ActorRef, "crashed", _) => ()
      println(s"Actor ${aRef.path} crashed on the actor of ${dmI.displayName}'s input.")
    case (input: Input @ unchecked, e: Exception, "exception") =>
      t.errMap.put(input.identity(), GeneralErrorSummary(e))
      t.statMap.put(input.identity(), Error)
      if (jobsLeft-1 <= 0) self ! "checkAgain"
      become(newState(dmI, dmO, aRef, acList, true, jobsLeft-1, jobsDoneInBatch))
    case (input: Input @ unchecked, outputs: List[Output] @ unchecked, "finished") =>
      outputs.foreach{ output =>
        t.provMap.put(output.identity(), input.identity())
        dmO.put(output)
      }
      t.statMap.put(input.identity(), Done)
      if (jobsLeft-1 == 0) self ! "checkAgain"
      if (jobsDoneInBatch+1 >= n){
        aRef ! "output"
        become(newState(dmI, dmO, aRef, acList, true, jobsLeft-1))
      } else {
        become(newState(dmI, dmO, aRef, acList, true, jobsLeft-1, jobsDoneInBatch+1))
      }
    case "input" => (acList, isReady) match{
      case (Nil, _) =>
        val newActorList = t.computeActorList
        t.process(dmI, dmO, newActorList)
        become(newState(dmI, dmO, aRef, newActorList, true))
      case (_, true) => t.process(dmI, dmO, acList)
      case (_, false) => self ! "input" //Do stuff when it is actually ready to build up again.
    }
    case "addedJob" => become(newState(dmI, dmO, aRef, acList, true, jobsLeft+1, jobsDoneInBatch))
    case "checkAgain" => if (jobsLeft == 0){ //Quietness, for now at least.
      if (jobsDoneInBatch > 0) aRef ! "output"
      //Unbuild everything.
      acList.foreach{ actor => actor ! PoisonPill }
      become(newState(dmI, dmO, aRef, List(), false))
    }
    case Terminated(aRef: ActorRef) =>
      become(newState(dmI, dmO, aRef, acList.diff(List(aRef)), false))
    case x => println(x)
  }

  def receive: Receive = {
    case "context" => sender() ! context
    case (dmI: DataMap[Input], dmO: DataMap[Output], aRef: ActorRef) => become(newState(dmI, dmO, aRef, t.computeActorList, true))
    case other => println(other)
  }
}


