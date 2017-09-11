package bigglue.data


import spray.json.{JsValue, _}

/**
  * Created by edmundlam on 8/8/17.
  */

object Identity {

  final val VERSION_DELIMITER = "-#-"

  def lift[Src,Dest](identity: Identity[Src]): Identity[Dest] = BasicIdentity[Dest](identity.getId())

  def toAny[Src](identity: Identity[Src]): Identity[Any] = BasicIdentity[Any](identity.getId())

  def deserialize[A](str: String): Identity[A] = {
    // println("Deserial this: " + str)
    val trimmed = str.stripPrefix("\"").stripSuffix("\"")
    // println("Trimmed: " + trimmed)
    if (trimmed.contains(VERSION_DELIMITER)) {
      val pair = trimmed.split(VERSION_DELIMITER)
      VersionedIdentity[A](pair(0), pair(1))
    } else BasicIdentity[A](trimmed)
  }

}

abstract class Identity[A] { // extends Identifiable[A] {

  def dropVersion(): BasicIdentity[A]

  def withVersion(v: String): VersionedIdentity[A]

  def getVersion(): Option[String]

  def getId(): String

  def toJsonFormat(): JsValue

  def serialize(): String

}

object IdentityToJson extends DefaultJsonProtocol {
  implicit def basicIdentityToJson[A] = jsonFormat1(BasicIdentity[A])
  implicit def VersionedIdentityToJson[A] = jsonFormat2(VersionedIdentity[A])
}

import IdentityToJson._

case class BasicIdentity[A](id: String) extends Identity[A] {

  override def dropVersion(): BasicIdentity[A] = this

  override def withVersion(v: String) = VersionedIdentity[A](id, v)

  override def getVersion(): Option[String] = None

  override def getId(): String = id

  override def toJsonFormat(): JsValue = this.toJson

  override def serialize(): String = id

  override def toString: String = s"BasicIdentity(${id})"

}

case class VersionedIdentity[A](id: String, version: String) extends Identity[A] {

  override def dropVersion(): BasicIdentity[A] = BasicIdentity[A](id)

  override def withVersion(v: String) = VersionedIdentity[A](id, v)

  override def getVersion(): Option[String] = Some(version)

  override def getId(): String = id

  override def toJsonFormat(): JsValue = this.toJson

  override def serialize(): String = s"${id}${Identity.VERSION_DELIMITER}${version}"

  override def toString: String = s"VersionedIdentity($id, $version)"

}

import IdentifiableToJson._

class Dummy

object tester {

  def main(args: Array[String]): Unit = {

    val id = BasicIdentity[Dummy]("happy")

    val json = id.toJson

    println(json.toString())

    val reId = json.convertTo[BasicIdentity[Dummy]]

    println(reId)

    val vid = id.withVersion("v0.2")

    val vjson = vid.toJson

    println(vjson.toString())

    val reVid = vjson.convertTo[VersionedIdentity[Dummy]]

    println(reVid)

    val i = I("crap")

    i.setVersion("v0.9")

    val ijson = i.toJson

    println(ijson)

    val reI = ijson.convertTo[I[String]]

    println(reI.toString + " " + reI.identity())

    val test = "{ \"a\" : 2 }"

    val jsontest = test.parseJson

    println(jsontest)

  }

}

object PairIdentity {

  def mergeIds[A,B](id1: Identity[A], id2: Identity[B]): String = id1.getId() + ":Pair:" + id2.getId()

  /*
  def mergeVersions[A,B](id1: Identity[A], id2: Identity[B]): Option[String] = {
    (id1.identity().version,id2.identity().version) match {
      case (Some(vL),Some(vR)) => Some(vL + "::" + vR)
      case (Some(vL),None)     => Some(vL + "::()")
      case (None,Some(vR))     => Some("()::" + vR)
      case (None,None)         => None
    }
  } */

}

class PairIdentity[L <: Identifiable[L],R <: Identifiable[R]](id1: Identity[L], id2: Identity[R])
  extends BasicIdentity[Pair[L,R]](PairIdentity.mergeIds(id1,id2))  {

  val leftId = id1
  val rightId = id2

}

