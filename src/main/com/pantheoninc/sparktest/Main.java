package com.pantheoninc.sparktest;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import scala.Tuple2;


public class Main {

    public static void main(String[] args) {

        LogManager.getLogger("org").setLevel(Level.ERROR);
        System.out.println("Hello World!");
        System.setProperty("hadoop.home.dir", "c:\\\\Spark\\winutils");

        // Создать SparkContext
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int path = 0;

        if(path == 0) {
            JavaPairRDD<String, String> storeAddress = sc.parallelizePairs(new ArrayList<Tuple2<String, String>>() {{
                add(new Tuple2<>("Ritual", "1026 Valencia St"));
                add(new Tuple2<>("Philz", "748 Van Ness Ave"));
                add(new Tuple2<>("Philz", "3101 24th St"));
                add(new Tuple2<>("Starbucks", "Seattle"));
            }});
            JavaPairRDD<String, Double> storeRating = sc.parallelizePairs(new ArrayList<Tuple2<String, Double>>() {{
                add(new Tuple2<>("Ritual", 4.9));
                add(new Tuple2<>("Philz", 4.8));
            }});

            JavaPairRDD<String, Tuple2<String, Optional<Double>>> leftResult = storeAddress.leftOuterJoin(storeRating);
            JavaPairRDD<String, Tuple2<Optional<String>, Double>> rightResult = storeAddress.rightOuterJoin(storeRating);

            System.out.println(leftResult.take(10));

        }


        if(path == 1) {
//        sc.setLogLevel("ERROR");
// Загрузить исходные данные.
            JavaRDD<String> inputRDD = sc.textFile("C:\\Users\\eduardm\\Downloads\\waytous-beta.log");
//        inputRDD.saveAsObjectFile("C:\\Users\\eduardm\\Downloads\\spark.out");
//        JavaRDD<String> inputRDD = sc.objectFile("C:\\Users\\eduardm\\Downloads\\spark.out");
            inputRDD.persist(StorageLevel.MEMORY_ONLY());

            Timer timer = new Timer().startLog();

            // Разбить на слова.
            JavaRDD<String> wordsRDD = inputRDD.flatMap(line -> Arrays.asList(line.split("[\\W+]")).iterator());
            wordsRDD.persist(StorageLevel.MEMORY_ONLY());

            timer.tickLog();

            // Преобразовать в пары и выполнить подсчет.
            JavaPairRDD<String, Integer> countsRDD = wordsRDD.mapToPair(word -> {
                return new Tuple2<>(word, 1);
            }).reduceByKey((a, b) -> a + b);

            timer.tickLog();

            System.out.println(countsRDD.take(3));

            timer.tickLog("counted");
            timer.stopLog();

            System.out.println("Debug: " + countsRDD.toDebugString());
            System.out.println("Partitions: " + countsRDD.partitions().size());
        }
    }


    static class Timer {
        long start;
        List<Long> ticks;

        Timer() {
            ticks = new ArrayList<>();
        }

        Timer start() {
            start = Calendar.getInstance().getTimeInMillis();
            ticks.add(start);
            return this;
        }

        Long tick() {
            long now = Calendar.getInstance().getTimeInMillis();
            ticks.add(now);
            long delta = now - start;
            start = now;
            return delta;
        }

        Long stop() {
            long now = Calendar.getInstance().getTimeInMillis();
            ticks.add(now);
            long delta = now - ticks.get(0);
            return delta;
        }

        Timer startLog() {
            start();
            System.out.println("Timer started at: " + new Date(start).toString());
            return this;
        }

        void tickLog() {
            System.out.println("Timer [" + ticks.size() + "]: " + tick() + " ms");
        }

        void tickLog(String mark) {
            System.out.println("Timer [" + ticks.size() + ":" + mark + "]: " + tick() + " ms");
        }

        void stopLog() {
            stop();
            System.out.println("Timer stopped at: " + new Date(start).toString() + " with " + (ticks.size() - 2) + " tick(s) and total time: " + (ticks.get(ticks.size() - 1) - ticks.get(0)) + " ms");
        }
    }
}
