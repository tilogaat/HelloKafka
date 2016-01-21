package com.tilogaat.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/*
https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 */
public class SimpleExample {
    public static void main(String args[]) {
        SimpleExample example = new SimpleExample();
        long maxReads = Long.parseLong(args[0]);
        String topic = args[1];
        int num_partitions = Integer.parseInt(args[2]);
        List<String> seeds = new ArrayList<String>();
        seeds.add(args[3]);
        int port = Integer.parseInt(args[4]);

        ExecutorService executor = Executors.newFixedThreadPool(num_partitions);
        try {
            for (int i =0 ; i< num_partitions; i++) {
                executor.submit(new SimpleConsumerRunnable(seeds, port, topic, i, i, maxReads));
            }
            
            //example.su(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:" + e);
            e.printStackTrace();
        }
    }
}