package bigglue.curators

import bigglue.data.{Identifiable, Identity}

/**
  * Created by edmundlam on 8/17/17.
  */
/**
  * This curator appends a version to the Identifiable.
  * [[IdleVersionCurator]] doesn't do anything to the version, if on the off-chance you don't care about it.
  * [[StandardVersionCurator]] is the default curator, and is the recommended curator to use.
  * @param version
  * @tparam Output
  */
abstract class VersionCurator[Output <: Identifiable[Output]](version: String) {
  val thisVersion: String = version

  def stampVersion(outputs: List[Output]): List[Output]

  /**
    * Given a version, this appends the version to an output file.
    * @param output The output that needs to be versioned.
    * @return A versioned version of the output.
    */
  def stampVersion(output: Output): Output

  def stampVersion(id: Identity[Output]): Identity[Output]

}

class IdleVersionCurator[Output <: Identifiable[Output]] extends VersionCurator[Output]("<None>") {

  override def stampVersion(outputs: List[Output]): List[Output] = outputs

  override def stampVersion(output: Output): Output = output

  override def stampVersion(id: Identity[Output]): Identity[Output] = id

}

/**
  *
  * @param version The version to add to the Identifiables.
  * @tparam Output The type that we are stamping the versions of the Identifiables to.
  */
class StandardVersionCurator[Output <: Identifiable[Output]](version: String) extends VersionCurator[Output](version) {

  /**
    * This simply stamps the version of the curator onto the outputs.
    * @param outputs The outputs that need to be curated.
    * @return The outputs that have had their meta-data changed to be the correct version.
    */
  override def stampVersion(outputs: List[Output]): List[Output] = {
    outputs foreach { _.setVersion(version) }
    outputs
  }

  /**
    * Given a version, this appends the version to an output file.
    * @param output The output that needs to be versioned.
    * @return A versioned version of the output.
    */
  override def stampVersion(output: Output): Output = {
    output.setVersion(version)
    output
  }

  override def stampVersion(id: Identity[Output]): Identity[Output] = id.withVersion(version)

}