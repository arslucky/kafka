package org.ars.kafka;

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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/*
 * @author arsen.ibragimov
 *
 * one producer, partition and multiple consumers with the same and other groups
 */
public class ConsumersDifferGroups1 {

    static Logger log = LogManager.getLogger( ConsumersDifferGroups1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.SSS";
    static String topic = ConsumersDifferGroups1.class.getSimpleName();

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }
    
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
                for( int i = 0; i < 1; i++) {
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, System.currentTimeMillis());
                    producer.send( record);
                    count++;
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( String.format( "producer:stop sent:%d", count));
            }
        }
    }

    static class Consumer implements Runnable {

        final AtomicBoolean closed = new AtomicBoolean();
        Properties config = new Properties();
        KafkaConsumer<Integer, Long> consumer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
        long count;
        String client;

        public Consumer( String client, String group) {
            this.client = client;
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, this.client);
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
            log.info( String.format( "%s:start", this.client));
            try {
                consumer.subscribe( Arrays.asList( topic));

                while( !closed.get()) {
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 100));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        log.info( String.format( "%s %s %s", this.client, dateFormat.format( new Date( record.value())), record.offset()));
                        count++;
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                log.info( String.format( "%s:stop processed:%d %s", this.client, count, consumer.groupMetadata().groupId()));
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

            Consumer consumer1 = new Consumer( "client1", "group1");
            Thread consumerThread1 = new Thread( consumer1);

            Consumer consumer2 = new Consumer( "client2", "group1");
            Thread consumerThread2 = new Thread( consumer2);

            Consumer consumer3 = new Consumer( "client3", "group2");
            Thread consumerThread3 = new Thread( consumer3);

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            log.info( "topic:" + topic);

            producerThread.start();
            consumerThread1.start();
            consumerThread2.start();
            consumerThread3.start();
            sleep( 2000);
            consumer1.shutdown();
            consumer2.shutdown();
            consumer3.shutdown();

            log.info( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
