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
    static String TOPIC = "test";
    static ConsumerConnector consumerConnector;
    Timer timer = new Timer();
    static ExecutorService executorService = Executors.newFixedThreadPool(5);
    static int count = 0;

    public static void main(String[] argv) throws Exception {

        try {
            HelloKafkaConsumer helloKafkaConsumer = new HelloKafkaConsumer(argv[1]);
            count = 0;
            TOPIC = argv[0];
            System.out.println("Starting test at :"+System.currentTimeMillis()+": value dequeued is :"+count);
            Future<String> future = executorService.submit(new Callable<String>() {
                public String call() throws Exception {
                    run();
                    return "Dequeued all messages from kafka";
                }
            });

            String result = future.get(60, TimeUnit.SECONDS);
        } catch (TimeoutException tex) {
            System.out.println("Finished test at :"+System.currentTimeMillis()+": value dequeued is :"+count);
          //  executorService.shutdown();
            System.exit(0);
        }
    }

    public HelloKafkaConsumer(String zookeeper){
        Properties properties = new Properties();
        properties.put("zookeeper.connect",zookeeper);
        properties.put("group.id", "tilo-consumer-1");
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
            count++;
            System.out.println(it.next().message());
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
