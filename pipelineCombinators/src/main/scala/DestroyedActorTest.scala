/**
  * Created by chanceroberts on 7/18/17.
  */
import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, Terminated}
import akka.util.Timeout

import scala.concurrent.duration._

object DestroyedActorTest {
  def main(args: Array[String]) {
    val system = ActorSystem()
    val supervisor = system.actorOf(Props(new SupervisedActor()))
    supervisor ! "go"
  }
}

class SupervisedActor extends Actor {
  val destroyedActor = context.actorOf(Props(new DeadActor()))
  implicit val timeout = Timeout(10 minutes)
  /*override val supervisorStrategy =
    OneForOneStrategy(){
      case e => println(e); self ! "remove"; Restart
    }*/
  context.watch(destroyedActor)
  import context._
  def state(s: String): Receive = {
    case "whatsMyState" => destroyedActor ! ("createState", s); println("State Restored?"); become(receive)
  }
  def receive: Receive = {
    case "go" =>
      destroyedActor ! self
      destroyedActor ! ("createState", "LLL")
      destroyedActor ! "state"
      destroyedActor ! (12, 0)
      destroyedActor ! (12, 1)
      destroyedActor ! "state"
      destroyedActor ! ("createState", "345")
      destroyedActor ! ("createState", "211")
    case "remove" =>
      println("You CAN change the state of the actors if they crashed. (and/or do other things as needed...)")
    case ("state", s: String) => become(state(s))
    case e => println(e)

  }

}

class DeadActor extends Actor {
  import context._
  context.parent ! "whatsMyState"
  override def postStop(): Unit = {
    super.postStop()
    println("Whoops!")
    self ! "sendState"
  }

  def state(s: String): Receive = {
    case (n: Int, m: Int) => println(n/m + n/m)
    case "state" => println(s)
    case "sendState" => context.parent ! ("state", s)
    case ("createState", st: String) => become(state(st)); println(s"I'm state $s currently!")
  }

  def receive: Receive = {
    case (n: Int, m: Int) => println(n/m)
    case "state" => println("noState")
    case ("createState", s: String) => become(state(s))
  }
}
