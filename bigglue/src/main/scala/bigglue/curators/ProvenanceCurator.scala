package bigglue.curators

import java.util.Calendar

import bigglue.data.{BasicIdentity, Identifiable, Identity}
import bigglue.store.DataMap
import bigglue.store.instances.InMemDataMap

/**
  * Created by edmundlam on 8/13/17.
  */
abstract class ProvenanceCurator[Input,Output] {

  def reportProvenance(src: Input, targets: List[Output]): Unit

  def reportProvenance(src: Input, target: Output): Unit = reportProvenance(src, List(target))

}

class IdleProvenanceCurator[Input,Output] extends ProvenanceCurator[Input,Output] {

  override def reportProvenance(src: Input, targets: List[Output]): Unit = { }

}

class StandardProvenanceCurator[Input <: Identifiable[Input],Output <: Identifiable[Output]] extends ProvenanceCurator[Input,Output] {

  val provMap: DataMap[Identity[Output],Any] = new InMemDataMap[Identity[Output],Any]

  def provenance(src: Input, target: Output): Any = (src.identity(),Calendar.getInstance().getTime().toString)

  override def reportProvenance(src: Input, targets: List[Output]): Unit = {
    targets foreach {
      target => provMap.put(target.identity(), provenance(src, target))
    }
  }

  override def toString: String = provMap.toString

}
