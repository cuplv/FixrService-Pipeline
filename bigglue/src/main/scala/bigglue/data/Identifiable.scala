package bigglue.data

import bigglue.curators.{Provenance, VersionCurator}
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, JsonFormat, RootJsonFormat}

/**
  * Created by edmundlam on 8/8/17.
  */

/**
  * An Identifiable is a simple data type in BigGlue.
  * Within this type, you have an [[Identity]], which is similar to the name of the data type.
  *
  * @tparam A The type of the Identifiable. Within the example, this would be [[bigglue.examples.GitID]], [[bigglue.examples.GitRepo]], [[bigglue.examples.GitCommitInfo]], or [[bigglue.examples.GitCommitGroups]].
  */
abstract class Identifiable[A] {
  var identityOpt: Option[Identity[A]] = None
  // TODO: Fix so this isn't a var?
  var provenanceOpt: Option[Provenance] = None
  /* /** The meta-data. On list of things to fix.*/
  var embedded: Map[String, String] = Map[String, String]() */

  /**
    * When creating an Identifiable, you will have to specify an [[Identity]] for that Identifiable.
    * This can be seen in the example within all of the data types, whose Identity is simply a [[BasicIdentity]]
    * that consists of the word sum.
    * @return The Identity of the Identifiable.
    */
  protected[this] def mkIdentity(): Identity[A]

  /**
    * Gets the [[Identity]]/name of the object.
    * @return The [[Identity]] of the object/
    */
  def identity(): Identity[A] = identityOpt match {
    case Some(id) => id
    case None => {
      val id = mkIdentity()
      identityOpt = Some(id)
      id
    }
  }

  /**
    * Gets the Id from the identity.
    * @return The Identifiable's name.
    */
  def getId(): String = identity().getId()

  /**
    * Gets the Version from the identity
    * @return The Identifiable's version.
    */
  def getVersion(): Option[String] = identity().getVersion()

  /**
    * Creates a new Identity from the version and then sets that identity as it's identity.
    * @param version The version that we are appending.
    */
  def setVersion(version: String): Unit = identityOpt = Some(identity().withVersion(version))

  /**
    * @return Get the Provenance information. If there is no provenance, we return nothing.
    */
  def getProvenance(): Option[Provenance] = provenanceOpt

  /**
    * Creates a new Provenance information.
    * @param prov The provenance that we are adding to the Identifiable.
    */
  def setProvenance(prov: Provenance): Unit = provenanceOpt = Some(prov)

  /*
  /**
    * Add stuff to the other metadata of the Identifiable
    * @param key The thing to add to the metadata.
    * @param value The value of the thing to add to the metadata.
    */
  def addEmbedded(key: String, value: String): Unit = embedded = embedded + (key->value)

  /**
    * Get something from the metadata.
    * @param key The thing we want to get from the metadata.
    * @return The value of that thing, if it exists; if not, then we return None.
    */
  def getEmbedded(key: String): Option[String] = embedded.get(key)
  */
}

abstract class Either[L <: Identifiable[L], R <: Identifiable[R]] extends Identifiable[Either[L,R]]

case class Left[L <: Identifiable[L], R <: Identifiable[R]](left: L) extends Either[L,R] {
  override def mkIdentity(): BasicIdentity[Either[L,R]] = BasicIdentity[Either[L,R]]("L::" + left.identity().getId())
}

case class Right[L <: Identifiable[L], R <: Identifiable[R]](right: R) extends Either[L,R] {
  override def mkIdentity(): BasicIdentity[Either[L,R]] = BasicIdentity[Either[L,R]]("R::" + right.identity().getId())
}


case class Pair[L <: Identifiable[L],R <: Identifiable[R]](left: L, right: R) extends Identifiable[Pair[L,R]] {

  val id = new PairIdentity[L,R](left.identity(), right.identity())

  override def mkIdentity(): Identity[Pair[L,R]] = id

  def getIds(): (Identity[L],Identity[R]) = (id.leftId,id.rightId)
}

/**
  * A wrapper class for types that are not [[Identifiable]].
  * This is mainly meant for basic types like Int, String, and Boolean, but can easily work for other types of classes.
  * For example, you may need to wrap Integers into the Identifiable class to work for BigGlue, so
  * we use [[I]][Int]s.
  * With this class, the [[Identity]] of the class is simply what the string representation of that object is.
  * @param a The actual value wrapped within the identifiable.
  * @tparam A The type of the value wrapped within the identifiable.
  */
case class I[A](a: A) extends Identifiable[I[A]] {
  def i(): A = a
  override def mkIdentity(): Identity[I[A]] = BasicIdentity[I[A]](a.toString())
  override def toString: String = a.toString
}

object IdentifiableToJson extends DefaultJsonProtocol {

  import IdentityToJson._

  implicit object IToJson extends RootJsonFormat[I[String]] {

    override def write(i: I[String]): JsValue = {
      i.identityOpt match {
        case Some(id) => JsObject("id" -> id.toJsonFormat(), "data" -> JsString(i.a))
        case None => JsObject("data" -> JsString(i.a))
      }
    }

    override def read(json: JsValue): I[String] = json match {
      case obj: JsObject => {
        val map = obj.fields
        if (map.contains("id")) {
          val idVal = map.get("id").get.asJsObject
          val id = if (idVal.fields.contains("version")) idVal.convertTo[VersionedIdentity[I[String]]] else idVal.convertTo[BasicIdentity[I[String]]]
          val i = I(map.get("data").get.convertTo[String])
          i.identityOpt = Some(id)
          i
        } else {
          I(map.get("data").get.convertTo[String])
        }
      }
    }

  }

}

case class U[A](a: A) extends Identifiable[U[A]] {
  def u(): A = a
  override def mkIdentity(): Identity[U[A]] = BasicIdentity[U[A]](java.util.UUID.randomUUID().toString)
  override def toString: String = a.toString
}

object Implicits {

  implicit class Is[A](as: Seq[A]) {
    def toIds(): Seq[I[A]] = as.map( I(_))
  }

}