import org.apache.spark._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.math.{atan2, cos, pow, sin, sqrt, toRadians}

object Main {
  def geodesic_distance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    // Радиус Земли в километрах
    val R = 6373.0

    // Конвертация в радианы
    val lt1 = toRadians(lat1)
    val lt2 = toRadians(lat2)
    val ln1 = toRadians(lon1)
    val ln2 = toRadians(lon2)

    val dlon = ln2 - ln1
    val dlat = lt2 - lt1

    // Вычисление геодезического расстояния по формуле Хаверсина
    val a = pow(sin(dlat / 2), 2) + cos(lt1) * cos(lt2) * pow(sin(dlon / 2), 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    val distance = R * c

    distance
  }

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val cfg = new SparkConf()
      .setAppName("Lab2").setMaster("local[2]")
    val sc = new SparkContext(cfg)


    val stationsWithHeader = sc.textFile("data/stations.csv")
    val stationsHeader = stationsWithHeader.first
    val stations = stationsWithHeader.filter(_ != stationsHeader).map(row => row.split(",", -1))

    val tripsWithHeader = sc.textFile("data/trips.csv")
    val tripsHeader = tripsWithHeader.first
    val trips = tripsWithHeader.filter(_ != tripsHeader).map(row => row.split(",", -1))

    trips.take(10).foreach(el => println(el.mkString(", ")))
    val tripsInternal = trips.mapPartitions(rows => {
      val timeFormat = DateTimeFormatter.ofPattern("M/d/yyyy H:m")
      rows.map(row =>
        new Trip(tripId = row(0).toInt,
          duration = if (row(1).isEmpty) 0 else row(1).toInt,
          startDate = LocalDateTime.parse(row(2), timeFormat),
          startStation = row(3),
          startTerminal = row(4).toInt,
          endDate = LocalDateTime.parse(row(5), timeFormat),
          endStation = row(6),
          endTerminal = row(7).toInt,
          bikeId = row(8).toInt,
          subscriptionType = row(9),
          zipCode = row(10)))
    })

    println()
    tripsInternal.take(10).foreach(el => println(el))

    val stationsInternal = stations.map(row =>
      new Station(stationId = row(0).toInt,
        name = row(1),
        lat = row(2).toDouble,
        long = row(3).toDouble,
        dockcount = row(4).toInt,
        landmark = row(5),
        installation = row(6),
        notes = null))
    println()
    stationsInternal.take(10).foreach(el => println(el))

    ////                TASK 1            ////Найти велосипед с максимальным временем пробега.
    val reduceResTask1 = tripsInternal.map(trip => (trip.bikeId, trip.duration)).reduceByKey(_ + _)
    val resTask1 = reduceResTask1.sortBy(_._2, ascending = false).first()
    println()
    println("Task 1: Максимальное время пробега у bikeId: " + resTask1._1 + " с duration: " + resTask1._2)

    ////                TASK 2            ////Найти наибольшее геодезическое расстояние между станциями.
    val cartesianRes = stationsInternal.cartesian(stationsInternal).filter(pair => pair._1.stationId < pair._2.stationId) // Чтобы избежать повторений
    val pairs = cartesianRes.map(st => ((st._1.stationId, st._2.stationId), geodesic_distance(lat1 = st._1.lat, lon1 = st._1.long, lat2 = st._2.lat, lon2 = st._2.long)))
    val res2 = pairs.sortBy(_._2, ascending = false).first()
    println()
    println("Task2: наибольшее геодезическое расстояние между станциями с id: " + res2._1._1 + " и " + res2._1._2 + " и равняется: " + res2._2 + " километров")

    ////                TASK 3            ////Найти путь велосипеда с максимальным временем пробега через станции.
    val tripsWithMaxDur = tripsInternal.filter(e => e.bikeId == resTask1._1).take(10)
    println()
    println("Task3")
    tripsWithMaxDur.foreach(e => println("Путь от: " + e.startStation + " до" + e.endStation))

    ////                TASK 4            ////Найти количество велосипедов в системе.
    val countBikes = tripsInternal.groupBy(_.bikeId).count()
    println()
    println("Task4: число велосипедов: " + countBikes)

    ////                TASK 5            ////Найти пользователей потративших на поездки более 3 часов.
    val res5 = tripsInternal.map(e => (e.bikeId, e.duration)).reduceByKey(_ + _).filter(_._2 > 30 * 60 * 60 )
    println()
    println("Task5")
    res5.take(20).foreach(e => println("BikeId: " + e._1 + " потратил время: " + e._2))

    sc.stop()
  }
}
