object RDD {

  def main(args: Array[String]): Unit = {

    val sc = Spark.getSparkContext("RDD")

    // collect
    val rdd = sc.parallelize(1 to 100)
    val result = rdd.collect()
    println(result.mkString(", "))

    // count
    println(rdd.count)

    // mapPartitions
    // 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    val rdd2 = sc.parallelize(1 to 10, 3)
    val r1 = rdd2.mapPartitions((n => n.map(y => y + 1)))
    println(r1.collect().mkString(", "))

    // mapValues
    // (a,2), (b,2), (c,2)
    val rdd3 = sc.parallelize(List("a", "b", "c")).map((_, 1))
    val r2 = rdd3.mapValues(x => x + 1)
    println(r2.collect().mkString(", "))

    // zip
    // (1,y), (2,s), (3,b)
    val t1 = sc.parallelize(List(1, 2, 3))
    val t2 = sc.parallelize(List("y", "s", "b"))
    val r3 = t1.zip(t2)
    println(r3.collect().mkString(", "))

    // groupBy
    // element is tuple
    // ("even", 2, 4, 6, 8, 10)
    // ("odd", 1, 3, 5, 7, 9)
    val g1 = sc.parallelize(1 to 10).groupBy({
      case i: Int if (i % 2 == 0) => "even"
      case _ => "odd"
    }).collect()

    g1.foreach(x => println(x._1, x._2.mkString(", ")))

    // groupByKey
    // (a, 1, 1, 1)
    // (b, 1, 1)
    // (c, 1)
    val g2 = sc.parallelize(List("a", "b", "a", "a", "b", "c")).map((_, 1)).groupByKey().collect()
    g2.foreach(x => println(x._1, x._2.mkString(", ")))

    // cogroup
    // (a 1 39 1000)
    // (b 2)
    val c1 = sc.parallelize(List(("a", 1), ("b", 2), ("a", 39)))
    val c2 = sc.parallelize(List(("a", 1000)))
    val r4 = c1.cogroup(c2).collect()
    r4.foreach(x => println(x._1, x._2._1.mkString(", "), x._2._2.mkString(", ")))

    // cartesian
    // 15개
    val ca1 = sc.parallelize(List(1, 2, 3, 4, 5))
    val ca2 = sc.parallelize(List("a", "b", "d"))
    val rr1 = ca1.cartesian(ca2).collect()
    println(s"cartesian length :${rr1.length}")
    rr1.foreach(x => println(x._1, x._2))

    // subtract
    // b c d
    // + subtractByKey:  key-value 형태 일 때
    val s1 = sc.parallelize(List("a", "b", "c", "d"))
    val s2 = sc.parallelize(List("a", "1"))
    s1.subtract(s2).collect().foreach(x => println(x))

    // union
    // a b c d a 1
    val u1 = sc.parallelize(List("a", "b", "c", "d"))
    val u2 = sc.parallelize(List("a", "1"))
    u1.union(u2).collect().foreach(x => println(x))

    // distinct
    // a 1 b c d
    u1.union(u2).distinct().collect().foreach(x => println(x))

    // intersection
    // a
    val i1 = sc.parallelize(List("a", "b", "c", "d"))
    val i2 = sc.parallelize(List("a", "1"))
    i1.intersection(i2).collect().foreach(x => println(x))

    // join
    // 같은 키로 그룹 형성
    // Tuple(Key, Tuple( j1 element, j2 element))
    // (a,(1,55))
    val j1 = sc.parallelize(List("a", "b")).map((_, 1))
    val j2 = sc.parallelize(List("a")).map((_, 55))

    j1.join(j2).collect().foreach(x => println(x._1, x._2))

    // leftOuterJoin, rightOuterJoin
    val lo1 = sc.parallelize(List("a", "b", "c")).map((_, 1))
    val ro1 = sc.parallelize(List("b")).map((_, 100))

    // (a,(1,None))	(b,(1,Some(100)))	(c,(1,None))
    val roo1 = lo1.leftOuterJoin(ro1).collect()
    println(roo1.mkString("\t"))

    // (b,(Some(1),100))
    val roo2 = lo1.rightOuterJoin(ro1).collect()
    println(roo2.mkString("\t"))

    // reduceByKey
    // 같은 키 가진 값들 하나로 병합
    // (a,3), (b,2), (c,1)
    val rbk1 = sc.parallelize(List("a", "b", "a", "b", "c", "a")).map((_, 1))
    println(rbk1.reduceByKey(_ + _).collect().mkString(", "))


  }
}
