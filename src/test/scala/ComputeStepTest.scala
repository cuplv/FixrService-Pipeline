import edu.colorado.plv.fixrservice.pipeline.{ComputeStep, HeapMap}
import org.scalatest.FlatSpec

/**
  * Created by chanceroberts on 5/25/17.
  */
class ComputeStepTest extends FlatSpec {

  "ComputeStep" should "only compute the files that have not been computed yet." in {
    val cStep = new ComputeStep({i: Int => i+1}, "")
    cStep.statMap.put("id_1", "Not Done")
    cStep.statMap.put("id_2", "Not Done")
    cStep.statMap.put("id_3", "Done")
    cStep.statMap.put("id_4", "Not Done")
    cStep.idToAMap.put("id_1", 1)
    cStep.idToAMap.put("id_2", 5)
    cStep.idToAMap.put("id_3", 3)
    cStep.idToAMap.put("id_4", 10)
    cStep.IncrementalCompute(blocking = true)
    assert(cStep.statMap.getAllKeys.foldLeft(true){
      case (true, key) => cStep.statMap.get(key).contains("Done")
      case (false, key) => false
    })
    assert(cStep.provMap.get(2).contains(List("id_1")))
    assert(cStep.provMap.get(6).contains(List("id_2")))
    assert(cStep.provMap.get(11).contains(List("id_4")))
    assert(cStep.provMap.get(4).isEmpty)
  }

  it should "put exceptions in the errMap" in {
    val cStep = new ComputeStep({i: Int => if (i > 10) throw new ArithmeticException() else i+1}, "")
    cStep.statMap.put("id_1", "Not Done")
    cStep.statMap.put("id_2", "Not Done")
    cStep.idToAMap.put("id_1", 100)
    cStep.idToAMap.put("id_2", 1)
    cStep.IncrementalCompute(blocking = true)
    assert(cStep.statMap.get("id_1").contains("Error"))
    assert(cStep.statMap.get("id_2").contains("Done"))
    assert(cStep.errMap.get("id_1") match{
      case Some(e) => e.isInstanceOf[ArithmeticException]
      case None => false
    })
    assert(cStep.provMap.get(2).contains(List("id_2")))
  }

  it should "skip over stuff that has an error that can't be redone yet" in {
    val cStep = new ComputeStep({i: Int => i+1}, "")
    cStep.statMap.put("id_1", "Error")
    cStep.idToAMap.put("id_1", 10)
    cStep.errMap.put("id_1", new ArrayIndexOutOfBoundsException)
    cStep.IncrementalCompute(blocking = true)
    assert(cStep.statMap.get("id_1").contains("Error"))
  }

  it should "try to fix the stuff that it can actually fix now" in {
    val cStep = new ComputeStep({i: Int => i+1}, "")
    cStep.statMap.put("id_1", "Error")
    cStep.errMap.put("id_1", new ArrayIndexOutOfBoundsException)
    cStep.idToAMap.put("id_1", 12)
    cStep.IncrementalCompute(blocking = true, List(new ArrayIndexOutOfBoundsException))
    assert(cStep.statMap.get("id_1").contains("Done"))
    assert(cStep.provMap.get(13).contains(List("id_1")))
  }
}
