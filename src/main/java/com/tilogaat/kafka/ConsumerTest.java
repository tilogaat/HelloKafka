package com.tilogaat.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.util.concurrent.Callable;

public class ConsumerTest implements Callable<Integer> {
    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public Integer call() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        int count = 0;
        while (it.hasNext()) {
            String message = new String(it.next().message());
            //System.out.println("Thread " + m_threadNumber + ": " + message);
            count++;
        }

        System.out.println("Threadnumber: " + m_threadNumber+ " - Count: "+count);
        return count;
    }
}