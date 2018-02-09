import bigglue.configurations.PipeConfig
import bigglue.connectors.Status.Status
import bigglue.connectors.instances.{Accumulator, JobQueue}
import bigglue.data.Identifiable
import bigglue.platforms.Platform
import bigglue.store.instances.InMemDataQueue

/**
  * Created by chanceroberts on 2/4/18.
  */
class SortingConnector[Data <: Identifiable[Data]] extends JobQueue[Data]{
  val dataQ = new InMemDataQueue[Data]()

  override def init(conf: PipeConfig): Unit = {
    super.init(conf)
  }

  override def sendDown(data: Seq[Data]): Unit = {
    dataQ.put(data)
    dataQ.size() match{
      case x if x > 10 => reallySendDown()
      case _ => ()
    }
  }

  def reallySendDown(): Unit = {
    import scala.math.Ordering._
    val seq = dataQ.extract().sortBy(_.identity().toString)
    seq.foreach(input => super.sendDown(Seq(input)))
    signalDown()
  }
}

class WithProvenanceConnector[Data <: Identifiable[Data]] extends JobQueue[Data]{

}
