/**
  * Created by chanceroberts on 7/18/17.
  */
import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, PoisonPill, Props, Terminated}
import akka.util.Timeout
import pipecombi.DataMap

import scala.concurrent.duration._

object DestroyedActorTest {
  def main(args: Array[String]) {
    val system = ActorSystem()
    var supervisor = system.actorOf(Props(new SupervisedActor()))
    supervisor ! "go"
  }
}

class SupervisedActor extends Actor {
  val destroyedActor = context.actorOf(Props(new DeadActor()), "destroyed")
  implicit val timeout = Timeout(10 minutes)
  override val supervisorStrategy =
    OneForOneStrategy(){
      case e => e.printStackTrace(); println(e); Restart
    }
  context.watch(destroyedActor)
  import context._
  var as = "YES"
  def state(s: String): Receive = {
    case "whatsMyState" => destroyedActor ! ("createState", s); println("State Restored?"); become(receive)
    case ("state", s: String) => as = s; become(state(s))
    case (Terminated(`destroyedActor`)) => println("!!")
  }
  def receive: Receive = {
    case "go" =>
      destroyedActor ! ("createState", "LLL")
      destroyedActor ! "state"
      destroyedActor ! (12, 0)
      destroyedActor ! (12, 1)
      destroyedActor ! "state"
      destroyedActor ! ("createState", "345")
      destroyedActor ! ("createState", "211")
      destroyedActor ! PoisonPill
      destroyedActor ! "whatwillhappen?"
    case "remove" =>
      println("You CAN change the state of the actors if they crashed. (and/or do other things as needed...)")
    case ("state", s: String) => as = s; become(state(s))
    case (aRef: ActorRef, "crashed", str: String) =>
      println(s"I can't believe you'd do such a thing, ${aRef.path}! >:(")
      println(s"Who will take care of $str now?!")
    case (Terminated(aRef: ActorRef)) => println(aRef.path)
    case e => println(e)

  }

}

class DeadActor extends Actor {
  import context._
  context.parent ! "whatsMyState"
  var as = "123"
  override def postStop(): Unit = {
    super.postStop()
    println("Whoops!")
    context.parent ! (self, "crashed", as)
  }

  def state(s: String): Receive = {
    case (n: Int, m: Int) => println(n/m + n/m)
    case "state" => println(s); as += "1"; println(as)
    case "sendState" => context.parent ! ("state", s)
    case ("createState", st: String) => become(state(st)); println(s"I'm state $s currently!")
  }

  def receive: Receive = {
    case (n: Int, m: Int) => println(n/m)
    case "state" => println("noState"); as += "2"
    case ("createState", s: String) => become(state(s))
  }
}
