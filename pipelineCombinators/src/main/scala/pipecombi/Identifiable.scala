package pipecombi

/**
  * Created by edmundlam on 6/25/17.
  */


abstract class Identifiable {
   def identity(): Identity
   def getId(): String = identity().id
   def getVersion(): Option[String] = identity().version
   def |+| (other: Identifiable): Identifiable = identity() |+| other.identity()
   def apply(s: String): Identifiable
}

case class Identity(id: String, version: Option[String]) extends Identifiable {

   val delimiter = "-:**:-"

   override def |+| (other: Identifiable): Identifiable = {
      val newId = this.id + delimiter + other.getId()
      (this.version,other.getVersion()) match {
         case (Some(ver1),Some(ver2)) => Identity(newId, Some(ver1 + delimiter + ver2))
         case (Some(ver1),None) => Identity(newId, Some(ver1 + delimiter + "-None-"))
         case (None,Some(ver2)) => Identity(newId, Some("-None-" + delimiter + ver2))
         case (None,None) => Identity(newId, None)
      }
   }

   override def apply(s: String): Identity = Identity(s, None)

   override def identity(): Identity = this
}


abstract class Either[L <: Identifiable, R <: Identifiable] extends Identifiable{
   override def apply(s: String): Identifiable = ??? 
}

case class Left[L <: Identifiable, R <: Identifiable](left: L) extends Either[L,R] {
   override def identity(): Identity = left.identity()
}

case class Right[L <: Identifiable, R <: Identifiable](right: R) extends Either[L,R] {
   override def identity(): Identity = right.identity()
}


case class Pair[L <: Identifiable,R <: Identifiable](left: L, right: R) extends Identifiable {
   override def identity(): Identity = (left |+| right).identity
   override def apply(s: String): Identifiable = ???
}