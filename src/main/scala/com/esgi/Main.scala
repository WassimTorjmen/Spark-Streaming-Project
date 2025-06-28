package com.esgi

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def main(args: Array[String]): Unit = {
    println("Main lance ProducerKafka + ConsumerKafka")

    // lance le Producer dans un thread séparé
    Future { ProducerKafka.main(Array.empty) }

    // laisse 2 s au broker + producer
    Thread.sleep(2000)

    // lance le Consumer
    ConsumerKafka.main(Array.empty)
  }
}
