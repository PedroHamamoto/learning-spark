package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class Main {

    private static final Random NUMBER_GENERATOR = new Random();

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Double> inputData = new LinkedList<>();
        IntStream.range(1, 10)
                .parallel()
                .forEach(i -> inputData.add(NUMBER_GENERATOR.nextDouble()));

        SparkConf conf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = context.parallelize(inputData);

        System.out.println(rdd.reduce(Double::sum));

        context.close();

    }

}
