package org.ars.kafka;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * one producer, partition and multiple consumers with the same and other groups
 */
public class ConsumersDifferGroups1 {

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumersDifferGroups1.class.getSimpleName();

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Long> producer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
        long count;

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
                for( int i = 0; i < 5; i++) {
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, System.currentTimeMillis());
                    producer.send( record);
                    count++;
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                System.out.println( String.format( "producer:stop sent:%d", count));
            }
        }
    }

    static class Consumer implements Runnable {

        final AtomicBoolean closed = new AtomicBoolean();
        Properties config = new Properties();
        KafkaConsumer<Integer, Long> consumer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
        long count;

        public Consumer( String group) {
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, "client" + currentThread().getId());
                config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
                config.put( ConsumerConfig.GROUP_ID_CONFIG, group);
                config.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
                config.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

                consumer = new KafkaConsumer<>( config);
            } catch( Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            System.out.println( String.format( "consumer:%d:start", currentThread().getId()));
            try {
                consumer.subscribe( Arrays.asList( topic));

                while( !closed.get()) {
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 100));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        System.out.println( String.format( "consumer:%s-%s %s %s", currentThread().getId(), record.key(), dateFormat.format( new Date( record.value())), record.offset()));
                        count++;
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println( String.format( "consumer:%d:stop processed:%d %s", currentThread().getId(), count, consumer.groupMetadata().groupId()));
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set( true);
        }
    }

    public static void main( String[] args) {
        try {
            Producer producer = new Producer();
            Thread producerThread = new Thread( producer);

            Consumer consumer1 = new Consumer( "group1");
            Thread consumerThread1 = new Thread( consumer1);

            Consumer consumer2 = new Consumer( "group1");
            Thread consumerThread2 = new Thread( consumer2);

            Consumer consumer3 = new Consumer( "group2");
            Thread consumerThread3 = new Thread( consumer3);

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            System.out.println( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            System.out.println( "topic:" + topic);

            producerThread.start();
            consumerThread1.start();
            consumerThread2.start();
            consumerThread3.start();
            sleep( 2000);
            consumer1.shutdown();
            consumer2.shutdown();
            consumer3.shutdown();

            System.out.println( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            System.out.println( e);
        }
    }
}
