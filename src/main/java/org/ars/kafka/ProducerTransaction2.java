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
 * ./kafka-topics.sh --bootstrap-server localhost:9091 --create --topic ProducerTransaction2 --replication-factor 2
 *
 */
public class ProducerTransaction2 {

    static Logger log = LogManager.getLogger( ProducerTransaction2.class);

    private static final String TIME_FORMAT = "kk:mm:ss.SSS";
    static String topic = ProducerTransaction2.class.getSimpleName();
    static String group = topic;

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, String> producer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Producer() {
            config.put( ProducerConfig.CLIENT_ID_CONFIG, "producer1");
            config.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9091,kafka2:9093,kafka3:9094");
            config.put( ProducerConfig.TRANSACTIONAL_ID_CONFIG, "1");
            config.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>( config);
        }

        @Override
        public void run() {
            try {
                producer.initTransactions();
                    for( int i = 0; i < 5; i++) {
                        long ms = System.currentTimeMillis();
                        producer.beginTransaction();

                        String res = "commited";
                        if( i % 2 == 0) {
                            res = "aborted";
                        }

                        String value = String.format( "%s %s", res, dateFormat.format( new Date( ms)));
                        ProducerRecord<Integer, String> record = new ProducerRecord<>( topic, i, value);
                        producer.send( record);

                        if( "commited".equals( res)) {
                            producer.commitTransaction();
                        } else {
                            producer.abortTransaction();
                        }
                        log.info( "send:" + i + " " + value);
                    }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( "producer:stop");
            }
        }
    }

    static class Consumer implements Runnable {

        final AtomicBoolean closed = new AtomicBoolean();
        Properties config = new Properties();
        KafkaConsumer<Integer, String> consumer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);

        public Consumer() {
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, "client1");
                config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9091,kafka2:9093,kafka3:9094");
                config.put( ConsumerConfig.GROUP_ID_CONFIG, group);
                config.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
                config.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

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
                    ConsumerRecords<Integer, String> records = consumer.poll( Duration.ofMillis( 100));
                    for( ConsumerRecord<Integer, String> record : records) {
                        log.info( String.format( "get:%s %s  %s", record.key(), record.value(), record.offset()));
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
            sleep( 500);
            consumerThread.start();
            sleep( 2000);
            consumer.shutdown();
        } catch( Exception e) {
            log.error( e);
        }
    }
}
