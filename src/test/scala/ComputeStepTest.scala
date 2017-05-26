import edu.colorado.plv.fixrservice.pipeline.{ComputeStep, HeapMap}
import org.scalatest.FlatSpec

/**
  * Created by chanceroberts on 5/25/17.
  */
class ComputeStepTest extends FlatSpec {

  "ComputeStep" should "only compute the files that have not been computed yet." in {
    val statMap = new HeapMap[String, String]
    val idToAMap = new HeapMap[String, Int]
    val errMap = new HeapMap[String, Exception]
    val provMap = new HeapMap[Int, List[String]]
    statMap.put("id_1", "Not Done")
    statMap.put("id_2", "Not Done")
    statMap.put("id_3", "Done")
    statMap.put("id_4", "Not Done")
    idToAMap.put("id_1", 1)
    idToAMap.put("id_2", 5)
    idToAMap.put("id_3", 3)
    idToAMap.put("id_4", 10)
    val cStep = new ComputeStep({i: Int => i+1}, "")
    cStep.IncrementalCompute(statMap, idToAMap, errMap, provMap)
    assert(statMap.getAllKeys.foldLeft(true){
      case (true, key) => statMap.get(key).contains("Done")
      case (false, key) => false
    })
    assert(provMap.get(2).contains(List("id_1")))
    assert(provMap.get(6).contains(List("id_2")))
    assert(provMap.get(11).contains(List("id_4")))
    assert(provMap.get(4).isEmpty)
  }

  it should "put exceptions in the errMap" in {
    val statMap = new HeapMap[String, String]
    val idToAMap = new HeapMap[String, Int]
    val errMap = new HeapMap[String, Exception]
    val provMap = new HeapMap[Int, List[String]]
    statMap.put("id_1", "Not Done")
    statMap.put("id_2", "Not Done")
    idToAMap.put("id_1", 100)
    idToAMap.put("id_2", 1)
    val cStep = new ComputeStep({i: Int => if (i > 10) throw new ArithmeticException() else i+1}, "")
    cStep.IncrementalCompute(statMap, idToAMap, errMap, provMap)
    assert(statMap.get("id_1").contains("Error"))
    assert(statMap.get("id_2").contains("Done"))
    assert(errMap.get("id_1") match{
      case Some(e) => e.isInstanceOf[ArithmeticException]
      case None => false
    })
    assert(provMap.get(2).contains(List("id_2")))
  }

  it should "skip over stuff that has an error that can't be redone yet" in {
    val statMap = new HeapMap[String, String]
    val idToAMap = new HeapMap[String, Int]
    val errMap = new HeapMap[String, Exception]
    val provMap = new HeapMap[Int, List[String]]
    statMap.put("id_1", "Error")
    idToAMap.put("id_1", 10)
    errMap.put("id_1", new ArrayIndexOutOfBoundsException)
    val cStep = new ComputeStep({i: Int => i+1}, "")
    cStep.IncrementalCompute(statMap, idToAMap, errMap, provMap)
    assert(statMap.get("id_1").contains("Error"))
  }

  it should "try to fix the stuff that it can actually fix now" in {
    val statMap = new HeapMap[String, String]
    val idToAMap = new HeapMap[String, Int]
    val errMap = new HeapMap[String, Exception]
    val provMap = new HeapMap[Int, List[String]]
    val cStep = new ComputeStep({i: Int => i+1}, "")
    statMap.put("id_1", "Error")
    errMap.put("id_1", new ArrayIndexOutOfBoundsException)
    idToAMap.put("id_1", 12)
    cStep.IncrementalCompute(statMap, idToAMap, errMap, provMap, List(new ArrayIndexOutOfBoundsException))
    assert(statMap.get("id_1").contains("Done"))
    assert(provMap.get(13).contains(List("id_1")))
  }
}
