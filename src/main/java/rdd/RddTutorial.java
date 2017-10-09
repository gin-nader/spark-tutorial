package rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.function.Function;

import static java.sql.DriverManager.println;

/**
 * This program covers the Spark Programming Guide for resilient distributed datasets, or RDDs.
 */
public class RddTutorial {

    /**
     * This method implements some of the basics of RDDs which features include passing functions to Spark,
     * using closures, working with key value pairs, broadcast variables and accumulators.
     * @param args
     */
    public static void main(String[] args) {

        // Initializing Spark
        SparkConf conf = new SparkConf().setAppName("RddTutorial").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Parallelized Collections
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        distData.reduce((a, b) -> a + b);

        // External Datasets
        JavaRDD<String> distFile = sc.textFile("README.md");
        distFile.map(s -> s.length()).reduce((a, b) -> a + b);

        // RDD Operations

        // Basics
        // counts total amount of line lengths in input file
        JavaRDD<String> lines = sc.textFile("README.md");
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);


        // Passing Functions to Spark - this gives my syntax errors for some reason
        JavaRDD<String> lines2 = sc.textFile("README.md");
        /*JavaRDD<Integer> lineLengths2 = lines.map(new Function <String, Integer>() {
            public Integer call(String s) { return s.length(); }
        });
        int totalLength2 = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) { return a + b; }
        });*/

        // Understanding closures

        final int[] counter = {0};
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Wrong: Don't do this!!
        // This creates copies of counter so the correct counter is not properly referenced
        rdd.foreach(x -> counter[0] += x);

        println("Counter value: " + counter[0]);

        // Working with Key-Value Pairs
        // This counts how many times each line of text occurs in a file
        JavaRDD<String> lines3 = sc.textFile("README.md");
        JavaPairRDD<String, Integer> pairs = lines3.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        // Broadcast Variables
        // caches read only value on each machine rather than shipping a copy with each task
        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] {1, 2, 3});

        broadcastVar.value();
        // returns [1, 2, 3]

        // Accumulators
        // values that can only be added to and a parallel safe. very useful for storing counters
        LongAccumulator accum = sc.sc().longAccumulator();

        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        // ...
        // 10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

        accum.value();
        // returns 10

        LongAccumulator accum2 = sc.sc().longAccumulator();
        //data.map(x -> { accum.add(x); return f(x); });
        // Here, accum is still 0 because no actions have caused the `map` to be computed.

    }
}
