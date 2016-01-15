package com.tilogaat.kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by user on 8/4/14.
 */
public class HelloKafkaProducer {
    static String TOPIC = "tilotopic2";

    public static void main(String[] argv){
        TOPIC=argv[0];
        Properties properties = new Properties();
        System.out.println(argv[1]);
        properties.put("metadata.broker.list", argv[1]); //localhost:9092
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        // for (int i = 0 ; i < 200; i++) {
        SimpleDateFormat sdf = new SimpleDateFormat();
        KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, "Test message from java program " + sdf.format(new Date()));
        producer.send(message);
        //}

        producer.close();
    }
}