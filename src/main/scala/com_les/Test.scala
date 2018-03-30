///////////////////////////     Postgres / GreenplumDB 데이터 로딩 ////////////////////////////////////
// 접속정보 설정
var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
var staticUser = "kopo"
var staticPw = "kopo"
var selloutDb = "kopo_batch_season_mpara"

// jdbc (java database connectivity) 연결
val selloutDataFromPg= spark.read.format("jdbc").
  options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load


// 데이터베이스 주소 및 접속정보 설정
var outputUrl = "jdbc:oracle:thin:@192.168.110.27:1522/XE"
//staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
var outputUser = "LES"
var outputPw = "les"

// 데이터 저장
var prop = new java.util.Properties
prop.setProperty("driver", "oracle.jdbc.OracleDriver")
prop.setProperty("user", outputUser)
prop.setProperty("password", outputPw)
var table = "ssssss"
//append
selloutDataFromPg.write.mode("overwrite").jdbc(outputUrl, table, prop)

