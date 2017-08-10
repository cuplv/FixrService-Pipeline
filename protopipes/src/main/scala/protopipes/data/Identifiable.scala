package protopipes.data

/**
  * Created by edmundlam on 8/8/17.
  */

abstract class Identifiable[A] {
  def identity(): Identity[A]
  def getId(): String = identity().id
  def getVersion(): Option[String] = identity().version
}

object Identity {

  def lift[Src,Dest](identity: Identity[Src]): Identity[Dest] = Identity[Dest](identity.id,identity.version)

  def toAny[Src](identity: Identity[Src]): Identity[Any] = Identity[Any](identity.id,identity.version)

}

case class Identity[A](id: String, version: Option[String]) extends Identifiable[A] {

  val delimiter = "-:**:-"

  override def identity(): Identity[A] = this
}

object PairIdentity {

  def mergeIds[A,B](id1: Identity[A], id2: Identity[B]): String = id1.id ++ "::" ++ id2.id

  def mergeVersions[A,B](id1: Identity[A], id2: Identity[B]): Option[String] = {
    (id1.identity().version,id2.identity().version) match {
      case (Some(vL),Some(vR)) => Some(vL + "::" + vR)
      case (Some(vL),None)     => Some(vL + "::()")
      case (None,Some(vR))     => Some("()::" + vR)
      case (None,None)         => None
    }
  }

}

class PairIdentity[L <: Identifiable[L],R <: Identifiable[R]](id1: Identity[L], id2: Identity[R])
  extends Identity[Pair[L,R]](PairIdentity.mergeIds(id1,id2), PairIdentity.mergeVersions(id1,id2))  {

  val leftId = id1
  val rightId = id2

}


abstract class Either[L <: Identifiable[L], R <: Identifiable[R]] extends Identifiable[Either[L,R]]

case class Left[L <: Identifiable[L], R <: Identifiable[R]](left: L) extends Either[L,R] {
  override def identity(): Identity[Either[L,R]] = Identity[Either[L,R]]("L::" + left.identity().id,left.identity().version)
}

case class Right[L <: Identifiable[L], R <: Identifiable[R]](right: R) extends Either[L,R] {
  override def identity(): Identity[Either[L,R]] = Identity[Either[L,R]]("R::" + right.identity().id,right.identity().version)
}


case class Pair[L <: Identifiable[L],R <: Identifiable[R]](left: L, right: R) extends Identifiable[Pair[L,R]] {

  val id = new PairIdentity[L,R](left.identity(), right.identity())

  override def identity(): Identity[Pair[L,R]] = id

  def getIds(): (Identity[L],Identity[R]) = (id.leftId,id.rightId)
}

case class I[A](a: A) extends Identifiable[I[A]] {
  def i(): A = a
  override def identity(): Identity[I[A]] = Identity[I[A]](s"${a.hashCode()}", None)
  override def toString: String = s"${a.toString}"
}

object Implicits {

  implicit class Is[A](as: Seq[A]) {
    def toIds(): Seq[I[A]] = as.map( I(_))
  }

}