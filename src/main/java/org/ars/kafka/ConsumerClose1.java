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
 * close producer and consumer, normal case
 */
public class ConsumerClose1 {

    static Logger log = LogManager.getLogger( ConsumerClose1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumerClose1.class.getSimpleName();
    static String group = topic;

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        final AtomicBoolean closed = new AtomicBoolean();
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
                while( !closed.get()) {
                    for( int i = 0; i < 3; i++) {
                        long ms = System.currentTimeMillis();
                        ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, ms);
                        producer.send( record);
                        log.info( "send:" + i + " " + dateFormat.format( new Date( ms)));
                        sleep( 200);
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( "producer:stop");
            }
        }

        public void shutdown() {
            closed.set( true);
        }
    }

    static class Consumer implements Runnable {

        final AtomicBoolean closed = new AtomicBoolean();
        Properties config = new Properties();
        KafkaConsumer<Integer, Long> consumer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Consumer() {
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, "client1");
                config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
                config.put( ConsumerConfig.GROUP_ID_CONFIG, group);
                config.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // true by default
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

                while( !closed.get()) {
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 100));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        log.info( String.format( "get:%s %s  %s", record.key(), dateFormat.format( new Date( record.value())), record.offset()));
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                log.info( "consumer:stop");
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

            Consumer consumer = new Consumer();
            Thread consumerThread = new Thread( consumer);

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            log.info( "topic:" + topic);
            log.info( "group:" + group);

            producerThread.start();
            consumerThread.start();
            sleep( 2000);
            producer.shutdown();
            sleep( 1000);
            consumer.shutdown();
        } catch( Exception e) {
            log.error( e);
        }
    }
}
