package pipecombi

//import pipecombi.{Identifiable, Identity}

/**
  * Created by edmundlam on 6/23/17.
  */
object DataMap {
  def injectVersion(identity: Identity, version: String): Identity = identity.copy(version = Some(version))
}

abstract class DataMap[Item <: Identifiable](template: Option[Item] = None) {
  def put(identity: Identity, item: Item): Boolean
  def get(identity: Identity): Option[Item]
  def identities: List[Identity]
  def items: List[Item]

  def put(item: Item): Boolean = put(item.identity(), item)

  // def :-- [Other <: pipecombi.Identifiable] (proc: Transformation[Item, Other]): DataMap[Other] = proc.proc.process(this, proc.output)
  // def <-* [Other <: pipecombi.Identifiable] (comp: Composition[Item, Other]): DataMap[pipecombi.Pair[Item,Other]] = comp.comp.compose(this, comp.inputR)

  /*
  def :<[OtherL <: pipecombi.Identifiable, OtherR <: pipecombi.Identifiable] (procL: Transformation[Item, OtherL]) (procR: Transformation[Item, OtherR]): Unit = {
     procL.proc.process(this, procL.output)
     procR.proc.process(this, procR.output)
  }*/

  def stop: DataMap[Bot] = new BotMap()

  def displayName: String = "DataMap"

}

case class EitherDataMap[Left <: Identifiable, Right <: Identifiable](mapL:DataMap[Left], mapR:DataMap[Right]) extends DataMap[pipecombi.Either[Left,Right]] {
  override def put(identity: Identity, item: pipecombi.Either[Left, Right]): Boolean = {
    item match {
      case Left(l)  => mapL.put(identity,l)
      case Right(r) => mapR.put(identity,r)
    }
    true
  }
  override def get(identity: Identity): Option[pipecombi.Either[Left, Right]] = {
     mapL.get(identity) match {
       case Some(l) => Some(pipecombi.Left(l))
       case None => mapR.get(identity) match {
         case Some(r) => Some(pipecombi.Right(r))
         case None => None
       }
     }
  }
  override def identities: List[Identity] = mapL.identities ++ mapR.identities
  override def items: List[pipecombi.Either[Left, Right]] = {
    mapL.items.map(pipecombi.Left[Left,Right](_)) ++ mapR.items.map(pipecombi.Right[Left,Right](_))
  }
}

class Bot extends Identifiable {
  override def identity(): Identity = Identity("",None)
  override def apply(s: String): Bot = new Bot()
}
class BotMap extends DataMap[Bot] {
  override def get(identity: Identity): Option[Bot] = None
  override def put(identity: Identity, item: Bot): Boolean = true
  override def identities: List[Identity] = List()
  override def items: List[Bot] = List()
}


class InMemDataMap[Item <: Identifiable](var map: Map[Identity, Item] = Map[Identity, Item](), name: String = "DataMap")
  extends DataMap[Item] {

  // val map: scala.collection.mutable.Map[pipecombi.Identity, Item] = scala.collection.mutable.Map[pipecombi.Identity, Item]()

  override def put(identity: Identity, item: Item): Boolean = {
    map += (identity -> item)
    true
  }

  override def get(identity: Identity): Option[Item] = map.get(identity)

  override def identities: List[Identity] = map.keys.toList

  override def items: List[Item] = map.toList.map( _._2 )

  override def toString: String = if (items.length > 0) items.map( _.toString ).mkString(",") else "<None>"

  override def displayName: String = name

  override def clone(): AnyRef = {
    new InMemDataMap[Item](map = map)
  }

}
