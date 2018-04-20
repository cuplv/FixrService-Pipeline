package bigglue.curators

import java.util.Calendar

import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.store.DataMap
import bigglue.store.instances.InMemDataMap

/**
  * Created by edmundlam on 8/13/17.
  */

/**
  * This ends up keeping track of the Provenance. This is defined by the configuration file, but the
  * default for this is the [[StandardProvenanceCurator]]
  * @tparam Input The type of the input that was computed to get the output.
  * @tparam Output The type of the output that was computed.
  */
abstract class ProvenanceCurator[Input,Output] {

  def reportProvenance(src: Input, targets: List[Output]): Unit

  def reportProvenance(src: Input, target: Output): Unit = reportProvenance(src, List(target))

}

/**
  * This does absolutely nothing.
  * This is for if you don't truly care what the provenance information is.
  * @tparam Input The type of the input that was computed to get the output.
  * @tparam Output The type of the output that was computed.
  */
class IdleProvenanceCurator[Input,Output] extends ProvenanceCurator[Input,Output] {

  /** Does nothing. */
  override def reportProvenance(src: Input, targets: List[Output]): Unit = { }

}

/**
  * The Provenance Information
  * @param id The id of the input (and the version. Built from [[Identity.serialize()]].
  * @param time The time we created the Provenance Information.
  */
case class Provenance(id: String, time: String)

/**
  * The default provenance tracker; This is in charge of keeping track of the inputs that let to each output,
  * and at what time the provenance had been computed.
  * @tparam Input The type of the input that was computed to get the output.
  * @tparam Output The type of the output that was computed.
  */
class StandardProvenanceCurator[Input <: Identifiable[Input],Output <: Identifiable[Output]] extends ProvenanceCurator[Input,Output] {

  val provMap: DataMap[Identity[Output],Any] = new InMemDataMap[Identity[Output],Any]

  def provenance(src: Input, target: Output): Any = (src.identity(),Calendar.getInstance().getTime().toString)

  /**
    * For all the outputs that have been computed, we save the provenance information and provenance time as metadata within the
    * Identifiables
    * @param src The input that the list of outputs came from.
    * @param targets The list of outputs that are using the provenance information.
    */
  override def reportProvenance(src: Input, targets: List[Output]): Unit = {
    targets foreach {
      target =>
        target.setProvenance(Provenance(src.identity().serialize(), Calendar.getInstance().getTime.toString))
        //target.addEmbedded("provInfo", src.identity().getId())
        //target.addEmbedded("provTime", Calendar.getInstance().getTime.toString)
        provMap.put(target.identity(), provenance(src, target))
    }
  }

  override def toString: String = provMap.toString

}
