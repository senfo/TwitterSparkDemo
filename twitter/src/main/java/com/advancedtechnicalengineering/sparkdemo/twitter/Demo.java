package com.advancedtechnicalengineering.sparkdemo.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Demonstrates Twitter integration
 */
public class Demo implements Runnable {
    /**
     * Implements the run method to process Twitter stream in a background thread
     */
    @Override
    public void run() {
        Integer tweetCount = 0;
        Properties properties = getProperties();
        String consumerKey = properties.getProperty("consumerKey");
        String consumerSecret = properties.getProperty("consumerSecret");
        String token = properties.getProperty("token");
        String tokenString = properties.getProperty("tokenString");
        TwitterDataProvider provider = new TwitterDataProvider(consumerKey, consumerSecret, token, tokenString);
        BlockingQueue<String> msgQueue = provider.getMsgQueue();

        provider.connect();

        while (tweetCount < 1000) {
            if (!msgQueue.isEmpty()) {
                try {
                    tweetCount++;
                    System.out.println(msgQueue.take());
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        provider.disconnect();
    }

    /**
     * The primary entry point to the demo application
     * @param args Any command line arguments
     */
    public static void main(String[] args) {
        (new Thread(new Demo())).start();
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
