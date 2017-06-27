package pipecombi
import com.typesafe.config.Config
import pipecombi._

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



abstract class IncrTransformer[Input <: Identifiable , Output <: Identifiable](c: Config = null) extends Transformer[Input, Output] {
  c.getObject("akka")

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

