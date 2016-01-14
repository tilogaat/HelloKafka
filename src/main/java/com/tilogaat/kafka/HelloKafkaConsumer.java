package com.tilogaat.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.nio.ByteBuffer;
import java.util.*;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;


/**
 * Created by user on 8/4/14.
 */
public class HelloKafkaConsumer{
    final static String clientId = "SimpleConsumerDemoClient";
    final static String TOPIC = "tilotopic2";
    static ConsumerConnector consumerConnector;
    Timer timer = new Timer();
    static ExecutorService executorService = Executors.newFixedThreadPool(5);

    public static void main(String[] argv) throws Exception {

        try {
            HelloKafkaConsumer helloKafkaConsumer = new HelloKafkaConsumer();
            //helloKafkaConsumer.start();
            Future<String> future = executorService.submit(new Callable<String>() {
                public String call() throws Exception {
                    run();
                    return "Dequeued all messages from kafka";
                }
            });

            String result = future.get(20, TimeUnit.MINUTES);
        } catch (TimeoutException tex) {
            System.out.println("Test finished!");
            System.exit(0);
        }
    }

    public HelloKafkaConsumer(){
        Properties properties = new Properties();
        properties.put("zookeeper.connect","localhost:2181");
        properties.put("group.id","test-group-tilo1");
        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    //@Override
    public static void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        final KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
        ConsumerIterator <byte[], byte[]> it = stream.iterator();
        while(it.hasNext()) {
            System.out.println("Reached here");
            System.out.println(new String(it.next().message()));
        }
    }

    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }
}
