package com_les

/**
  * @author ${user.name}
  */
object App {

  def quizDef(inputValue: String): (Int, Int) = {

    var inputValue = "2017;34"
    var target = inputValue.split(";")
    var yearValue = target(0)
    var weekValue = target(1)
    (yearValue.toInt, weekValue.toInt)
  }

  var test = "2017;34"

  var answer = quizDef(test)
}
