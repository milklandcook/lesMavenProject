package com_les

object Exam_01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var score = 9

    /////// 로직 시작 /////
    var testArray = Array(22,33,44,50)
    testArray.filter(x=>{ x%10==0})
  }
}

  //반복하기
  var priceData = Array(1000.0.1200.0,1300.0,1500.0,10000.0)
  var promotionRate = 0.2
  var priceDataSize = priceData.size

  for(i <-0 to priceDataSize-1){
    var promotionEffect = priceData(i) * promotionRate
    priceData(i) = priceData(i) - promotionEffect

  var priceData2 = Array((1000.0.1200.0,1300.0,1500.0,10000.0)

  priceData2.map(x=>{
    x  (x*promotionRate)}
  )

  priceData2.filter(x=>{
}
  }
  }

