package com.advancedtechnicalengineering.sparkdemo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.*;
import java.util.Properties;

/**
 * A Streaming Spark demo using Twitter data
 */
public class TwitterSpark {
    public static void main(String[] args) {
        long timeout = 5000;
        Properties properties = getProperties();
        String consumerKey = properties.getProperty("consumerKey");
        String consumerSecret = properties.getProperty("consumerSecret");
        String token = properties.getProperty("token");
        String tokenString = properties.getProperty("tokenString");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterSpark");
        JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(timeout));
        TwitterReceiver receiver = new TwitterReceiver(consumerKey, consumerSecret, token, tokenString);
        JavaDStream<String> stream = context.receiverStream(receiver);

        stream.print();

        context.start();
        context.awaitTermination(timeout);
    }

    private static Properties getProperties() {
        try {
            Properties properties = new Properties();
            InputStream stream = new FileInputStream("config.xml");

            properties.loadFromXML(stream);
            stream.close();

            return properties;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
