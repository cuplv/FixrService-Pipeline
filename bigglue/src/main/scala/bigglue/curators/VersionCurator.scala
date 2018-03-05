package bigglue.curators

import bigglue.data.{Identifiable, Identity}

/**
  * Created by edmundlam on 8/17/17.
  */
abstract class VersionCurator[Output <: Identifiable[Output]](version: String) {
  val thisVersion: String = version

  def stampVersion(outputs: List[Output]): List[Output]

  def stampVersion(output: Output): Output

  def stampVersion(id: Identity[Output]): Identity[Output]

}

class IdleVersionCurator[Output <: Identifiable[Output]] extends VersionCurator[Output]("<None>") {

  override def stampVersion(outputs: List[Output]): List[Output] = outputs

  override def stampVersion(output: Output): Output = output

  override def stampVersion(id: Identity[Output]): Identity[Output] = id

}

class StandardVersionCurator[Output <: Identifiable[Output]](version: String) extends VersionCurator[Output](version) {

  override def stampVersion(outputs: List[Output]): List[Output] = {
    outputs foreach { _.setVersion(version) }
    outputs
  }

  override def stampVersion(output: Output): Output = {
    output.setVersion(version)
    output
  }

  override def stampVersion(id: Identity[Output]): Identity[Output] = id.withVersion(version)

}