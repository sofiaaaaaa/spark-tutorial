package com.sparkTutorial.rdd;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;

public class WordCount2 {

    public static void main(String[] args) throws Exception {
        System.out.println(" Start time: "+  new Timestamp(System.currentTimeMillis()));

        // test1433957069710630361.csv
        // test3704171197221609216.csv
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[10]");
        conf.set("spark.testing.memory", "2147480000");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/test1433957069710630361.csv");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        Map<String, Long> wordCounts = words.countByValue();

//        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
//            System.out.println(entry.getKey() + " : " + entry.getValue());
//        }

        System.out.println(" Success!! End time: "+  new Timestamp(System.currentTimeMillis()));

    }
}
