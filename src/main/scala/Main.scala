//Versione Java 17, Scala 2.12.21, Spark 3.5.1

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.util.Try

object Main {
  def roundFunc(x: Double): Double = math.round(x * 10.0) / 10.0

  //Assumo che le date siano sempre nel formato "DATA-SPAZIO-ORA"
  //Nella prima versione usavo split, ma ho deciso di toglierlo perchè allocava un array per ogni stringa
  def truncDate(x: String): String = {
    val i = x.indexOf(' ')
    if (i > 0) x.substring(0, i) else x
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Terremoti-local")
      .master("local[*]")
      .getOrCreate()

    //Per ridurre warning
    spark.sparkContext.setLogLevel("WARN")

    val filename = if (args.length > 0) args(0) else "data/dataset-earthquakes-trimmed.csv"

    //Leggo i dati dal csv e li memorizzo in un RDD
    //Aggiunta ripartizione rispetto alla versione 1
    val data = spark.read.option("header", value = true).csv(filename).rdd.repartition(200)

    //Versione 1 per rimozione duplicati: distinct() su data

    //Tempo iniziale
    val t0 = System.nanoTime()

    /* Versione 1
        Uso row.getAs[String] e poi faccio casting -> operazione non necessaria
        Uso cache
       Versione 2
        Faccio data.fieldIndex per ogni riga
        Uso cache
     */

    // Fuori mapPartitions, prendo lo schema una volta sola
    val schemaRow = data.first()
    val latIdx = schemaRow.fieldIndex("latitude")
    val lonIdx = schemaRow.fieldIndex("longitude")
    val dateIdx = schemaRow.fieldIndex("date")

    val roundedData = data.mapPartitions { rows =>
        rows.flatMap { row =>
          if (row == null) {
            Iterator.empty
          } else {
            try {
              val lat = Try(row.getString(latIdx).toDouble).getOrElse(0.0)
              val lon = Try(row.getString(lonIdx).toDouble).getOrElse(0.0)
              val date = Option(row.getString(dateIdx)).getOrElse("")
              Iterator.single(((roundFunc(lon), roundFunc(lat), truncDate(date)), 1))
            } catch {
              case _: Exception => Iterator.empty
            }
          }
        }
      }.reduceByKey(_ + _)
      .keys.persist(StorageLevel.MEMORY_AND_DISK)


    // Raggruppamento per data
    // Pattern matching su tuple = zero overhead
    val byDay = roundedData.map { case (lon, lat, day) =>
      (day, (lon, lat))
    }

    type Loc = (Double, Double)
    type Pair = (Loc, Loc)

    implicit val locOrd: Ordering[Loc] =
      Ordering.Tuple2(Ordering.Double, Ordering.Double)

    def canonPair(a: Loc, b: Loc): Pair =
      if (locOrd.lteq(a, b)) (a, b) else (b, a)

    /* Versione 1
        groupByKey() = enorme shuffle (tutti dati di una giornata su un executor)
        toArray.distinct = OOM se una giornata ha migliaia di locazioni
        combinations(2) = esplosione combinatoria
     */

    // Pre‑filtra duplicati per giorni prima dello shuffle
    val distinctByDay = byDay
      .distinct()  // Elimina duplicati (day, loc) prima di groupByKey

    val dayPairs = distinctByDay
      .groupByKey(200)
      .flatMap { case (day, locsIter) =>
        val locs = locsIter.toSet  // Per distinct con meno memoria
        locs.toArray.combinations(2).map { case Array(a, b) =>
          (canonPair(a, b), day)
        }
      }

    /* Versione 1
        dayPairs → map → reduceByKey (shuffle #1)
        pairCounts → reduce (shuffle #2)
        datesSorted → filter → collect (shuffle #3)
     */

    // Conta coppie (shuffle #1)
    val pairCounts = dayPairs
      .map { case (pair, day) => (pair, 1L) }  // ← Esplicito invece di mapValues
      .reduceByKey(_ + _)

    // Trova il massimo
    val bestTuple = pairCounts.reduce { (x, y) =>
      if (x._2 >= y._2) x else y
    }

    val bestPair = bestTuple._1  // (Pair, Long)
    val bestCount = bestTuple._2

    // Date del bestPair (shuffle #2)
    val datesSorted = dayPairs
      .filter(_._1 == bestPair)
      .map(_._2)
      .distinct()
      .collect()
      .sorted

    val t1 = System.nanoTime()

    val (locA, locB) = bestPair
    datesSorted.foreach(println)
    println(f"Coppia con più co-occorrenze: (${locA._2}%.1f, ${locA._1}%.1f), (${locB._2}%.1f, ${locB._1}%.1f)")
    println("Numero elementi: " + datesSorted.length)
    println(s"Tempo elaborazione: ${(t1 - t0) / 1e6} ms")


    spark.stop()
  }
}

