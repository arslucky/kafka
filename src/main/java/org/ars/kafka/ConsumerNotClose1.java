package org.ars.kafka;

import static java.lang.Thread.sleep;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * @author arsen.ibragimov
 *
 * no producer and consumer closing
 * steps:
 * 1. start program
 * 2. halt and restart at once
 * 3. consumer start to pick up records in ~41-43 seconds
 * Note: depends from 'session.timeout.ms' 45 seconds by default
 * where 'group.min.session.timeout.ms' (6 sec) <= 'session.timeout.ms' <= 'group.max.session.timeout.ms' (30 minutes)
 *
 * result
 *
start:09:26:18.018
send:0 09:26:18.018
send:1 09:26:18.018
send:2 09:26:18.018
send:3 09:26:19.019
send:4 09:26:19.019
send:5 09:26:19.019
send:6 09:26:19.019
send:7 09:26:20.020
send:8 09:26:20.020
send:9 09:26:20.020
timeout:41321
get:0 09:26:18.018  936
get:1 09:26:18.018  937
get:2 09:26:18.018  938
get:3 09:26:19.019  939
get:4 09:26:19.019  940
get:5 09:26:19.019  941
get:6 09:26:19.019  942
get:7 09:26:20.020  943
get:8 09:26:20.020  944
get:9 09:26:20.020  945
 */
public class ConsumerNotClose1 {

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumerNotClose1.class.getSimpleName();
    static String group = topic;

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Long> producer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Producer() {
            config.put( ProducerConfig.CLIENT_ID_CONFIG, "producer1");
            config.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
            config.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");

            producer = new KafkaProducer<>( config);
        }

        @Override
        public void run() {
            try {
                for( int i = 0; i < 10; i++) {
                    long ms = System.currentTimeMillis();
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, ms);
                    producer.send( record);
                    System.out.println( "send:" + i + " " + dateFormat.format( new Date( ms)));
                    sleep( 200);
                }
            } catch( Exception e) {
                e.printStackTrace();
            }
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaConsumer<Integer, Long> consumer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Consumer() {
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, "client1");
                config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
                config.put( ConsumerConfig.GROUP_ID_CONFIG, group);
                config.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // true by default
                // config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"); // 'session.timeout.ms' 45 seconds by default
                config.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
                config.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

                consumer = new KafkaConsumer<>( config);
            } catch( Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try {
                consumer.subscribe( Arrays.asList( topic));
                long start = System.currentTimeMillis();
                boolean flag = false;

                while( true) {
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 1000));

                    if( records.count() == 0 && !flag) {
                        flag = true;
                        start = System.currentTimeMillis();
                    }
                    if( records.count() != 0 && flag) {
                        flag = false;
                        System.out.println( "timeout:" + (System.currentTimeMillis() - start));
                    }
                    for( ConsumerRecord<Integer, Long> record : records) {
                        System.out.println( String.format( "get:%s %s  %s", record.key(), dateFormat.format( new Date( record.value())), record.offset()));
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main( String[] args) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            System.out.println( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            System.out.println( "topic:" + topic);
            System.out.println( "group:" + group);

            Thread producerThread = new Thread( new Producer());
            Thread consumerThread = new Thread( new Consumer());

            producerThread.start();
            consumerThread.start();
        } catch( Exception e) {
            System.out.println( e);
        }
    }
}