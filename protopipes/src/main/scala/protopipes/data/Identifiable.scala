package protopipes.data

import protopipes.curators.VersionCurator
import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, JsonFormat, RootJsonFormat}

/**
  * Created by edmundlam on 8/8/17.
  */

abstract class Identifiable[A] {
  var identityOpt: Option[Identity[A]] = None

  protected[this] def mkIdentity(): Identity[A]

  def identity(): Identity[A] = identityOpt match {
    case Some(id) => id
    case None => {
      val id = mkIdentity()
      identityOpt = Some(id)
      id
    }
  }
  def getId(): String = identity().getId()
  def setVersion(version: String): Unit = identityOpt = Some(identity().withVersion(version))
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