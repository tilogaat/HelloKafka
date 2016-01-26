package com.tilogaat.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

/*
Taken from https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 */
public class ConsumerTest implements Callable<Integer> {
    private KafkaStream m_stream;
    private int m_threadNumber;
    private boolean toLog;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber, boolean tolog) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        toLog = tolog;
    }

    public Integer call() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
        System.out.println("Threadnumber: " + m_threadNumber+ "Start time: "+sdf.format(new Date()));
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        int count = 0;
        while (it.hasNext()) {
            String message = new String(it.next().message());
            if (toLog) {
                System.out.println("Thread " + m_threadNumber + ": " + message);
            }
            count++;
        }

        System.out.println("Threadnumber: " + m_threadNumber+ " - Count: "+count);
        System.out.println("Threadnumber: " + m_threadNumber+ "End time: "+sdf.format(new Date()));

        return count;
    }
}