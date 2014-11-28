package com.advancedtechnicalengineering.sparkdemo.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Provides a data source for Twitter streams
 */
public class TwitterDataProvider implements Serializable {
    private Client client;
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String tokenString;

    /**
     * Initializes a new instance of the TwitterDataProvider class. Requires a registered app on http://dev.twitter.com
     * @param consumerKey Twitter consumer key
     * @param consumerSecret Twitter consumer secret
     * @param token Twitter app token
     * @param tokenString Twitter app token string
     */
    public TwitterDataProvider(String consumerKey, String consumerSecret, String token, String tokenString) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.tokenString = tokenString;
    }

    /**
     * Connects to Twitter and starts listening
     */
    public void connect() {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("Ferguson");
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, token, tokenString);
        ClientBuilder clientBuilder = new ClientBuilder()
                .name("SparkDemo-Client")
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);
        client = clientBuilder.build();

        endpoint.trackTerms(terms);
        client.connect();
    }

    /**
     * Disconnects from the Twitter stream
     */
    public void disconnect() {
        client.stop();
    }

    /**
     * Gets the queue containing String messages from Twitter
     * @return The queue containing String messages from Twitter
     */
    public BlockingQueue<String> getMsgQueue() {
        return msgQueue;
    }

    /**
     * Gets the queue containing events from Twitter
     * @return The queue containing events from Twitter
     */
    public BlockingQueue<Event> getEventQueue() {
        return eventQueue;
    }
}
