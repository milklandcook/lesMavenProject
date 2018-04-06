package com_les

object Example_DataRefining {

  def main(args: Array[String]): Unit = {
    // oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_ex"


    val selloutData = spark.read.format("jdbc").
     options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load


    selloutDataFromOracle.createOrReplaceTempView("selloutTable")

  }
}
