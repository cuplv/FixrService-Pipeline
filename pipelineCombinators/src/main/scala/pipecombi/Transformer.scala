package pipecombi
import java.io.File

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, PoisonPill, Props, Terminated}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import mthread_abstrac.{ConfigHelper, MThreadAbstraction, MThreadBuilder}

import scala.concurrent.duration._
import scala.util.Success
import collection.JavaConverters._
import pipecombi._

import scala.concurrent.{Await, Future}

/**
  * Created by edmundlam on 6/20/17.
  */


abstract class Transformer[Input <: Identifiable, Output <: Identifiable](conf: Any = "", name: String = "")(implicit system: ActorSystem) extends Operator[Input, Output, Output] {
  val c: Option[Config] = conf match{
    case "" => None
    case s: String => Some(ConfigFactory.parseFile(new File(s)))
    case c: Config => Some(c)
    case _ => None
  }
  val stepAbstract = MThreadBuilder.build(getListofInputs, compute, success, failure, c, "akka")

  def compute(input: Input): List[Output]

  def computeAndStore(inputs: List[Input], outputMap: DataMap[Output]): Unit = () //Override this method

  def getListofInputs(inputMap: DataMap[Input]): List[Input]

  def success(input: Input, outputs: List[Output], outputMap: DataMap[Output]): DataMap[Output]

  def failure(input: Input, exception: Exception): Any

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



abstract class IncrTransformer[Input <: Identifiable , Output <: Identifiable](name: String = "", conf: Any = "")(implicit system: ActorSystem) extends Transformer[Input, Output](conf, name) {
  //val actorSys: ActorSystem = ActorSystem.apply(ConfigHelper.possiblyInConfig(c, "ActorSystemName", "Increment"), c)
  //val actorSys: ActorSystem = context.system
  val timer = 5 seconds
  val verbose = //ConfigHelper.possiblyInConfig(c, name+"_verbosity", default = false) ||
                ConfigHelper.possiblyInConfig(c, "verbosity", default = false)
  val errList = ConfigHelper.possiblyInConfig(c, "exceptionsBlacklist", List.empty[String])
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

  //def computeThenStore(input: List[Input], outputMap: DataMap[Output], actorList: List[ActorRef]): Unit = startMultiCompute(input, outputMap, actorList)

  def computeThenStore(input: Input, outputMap: DataMap[Output]): List[Output] = {
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
    inputs.flatMap( computeThenStore(_, outputMap) )
    //startMultiCompute(inputs, outputMap, actorList)
    //actorSys.terminate
    outputMap
  }

  override def success(input: Input, outputs: List[Output], outputMap: DataMap[Output]): DataMap[Output] = {
    outputs.foreach{ output =>
      provMap.put(output.identity(), input.identity())
      outputMap.put(output)
    }
    statMap.put(input.identity(), Done)
    outputMap
  }

  override def failure(input: Input, e: Exception): Unit = {
    errMap.put(input.identity(), GeneralErrorSummary(e))
    statMap.put(input.identity(), Error)
  }
}



