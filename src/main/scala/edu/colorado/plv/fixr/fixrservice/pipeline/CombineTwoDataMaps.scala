package edu.colorado.plv.fixr.fixrservice.pipeline

import com.typesafe.config.Config

/**
  * Created by chanceroberts on 6/26/17.
  */
class CombineTwoDataMaps[A,B,C,D,E,F](dataMapOne: DataMap[A,B], dataMapTwo: DataMap[C,D], function: (DataMap[A,B], DataMap[C,D]) => DataMap[E,F],
                                      prefixOne: String = "", prefixTwo: String = "", c: Config = null) {
  //val dMap = new DualDataMap[A,C,B,D](dataMapOne, dataMapTwo)
  val dMap: DataMap[E,F] = function(dataMapOne, dataMapTwo)
  val setup = new SetupDatabases
  val sMap = new HeapMap[E, String]

  dMap.getAllKeys.foldLeft(){
    case ((), key) => sMap.put(key, "Not Done")
  }
}

class BatchProduct[A,B,C,D](dataMapOne: DataMap[A,B], dataMapTwo: DataMap[C,D]) extends CombineTwoDataMaps[A,B,C,D,String,(B,D)](dataMapOne, dataMapTwo,
  (dMapOne, dMapTwo) => {
    val twoKeysAndValues = dMapTwo.getAllKeysAndValues
    val dMap3 = new HeapMap[String, (B,D)]
    dMapOne.getAllKeysAndValues.foldLeft(){
      case ((), (keyA, valueA)) => twoKeysAndValues.foldLeft(){
        case ((), (keyB, valueB)) => dMap3.put(keyA.toString+"-:**:-"+keyB.toString, (valueA, valueB))
      }
    }
    dMap3
  })

