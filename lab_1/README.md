# Лабораторная работа 1 - Apache Spark


**Задание 1 - Найти велосипед с максимальным временем пробега.**
```
    val reduceResTask1 = tripsInternal.map(trip => (trip.bikeId, trip.duration)).reduceByKey(_ + _)
    val resTask1 = reduceResTask1.sortBy(_._2, ascending = false).first()
    println()
    println("Task 1: Максимальное время пробега у bikeId: " + resTask1._1 + " с duration: " + resTask1._2)
```

**Задание 2 - Найти наибольшее геодезическое расстояние между станциями.**

Для этого задания была реализована функция geodesic_distance для вычисления расстояния по координатам широты и долготы
```
val cartesianRes = stationsInternal.cartesian(stationsInternal).filter(pair => pair._1.stationId < pair._2.stationId) // Чтобы избежать повторений
val pairs = cartesianRes.map(st => ((st._1.stationId, st._2.stationId), geodesic_distance(lat1 = st._1.lat, lon1 = st._1.long, lat2 = st._2.lat, lon2 = st._2.long)))
val res2 = pairs.sortBy(_._2, ascending = false).first()
println()
println("Task2: наибольшее геодезическое расстояние между станциями с id: " + res2._1._1 + " и " + res2._1._2 + " и равняется: " + res2._2 + " километров")
```
**Задание 3 - Найти путь велосипеда с максимальным временем пробега через станции.**

```
    val tripsWithMaxDur = tripsInternal.filter(e => e.bikeId == resTask1._1).take(10)
    println()
    println("Task3")
    tripsWithMaxDur.foreach(e => println("Путь от: " + e.startStation + " до" + e.endStation))
```
**Задание 4 - Найти количество велосипедов в системе.**
```
    val countBikes = tripsInternal.groupBy(_.bikeId).count()
    println()
    println("Task4: число велосипедов: " + countBikes)
```
**Задание 5 - Найти пользователей потративших на поездки более 3 часов.**

За id пользователя брался bikeId
```
    val res5 = tripsInternal.map(e => (e.bikeId, e.duration)).reduceByKey(_ + _).filter(_._2 > 30 * 60 * 60 )
    println()
    println("Task5")
    res5.take(20).foreach(e => println("BikeId: " + e._1 + " потратил время: " + e._2))
```