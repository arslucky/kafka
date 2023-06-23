package org.ars.kafka;

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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/*
 * @author arsen.ibragimov
 *
 * no producer and consumer closing
 * steps:
 * 1. start program
 * 2. halt and restart at once
 * 3. consumer start to pick up records in ~10 seconds
 * Note: depends from 'session.timeout.ms=10000', 45 seconds by default
 * where 'group.min.session.timeout.ms' (6 sec) <= 'session.timeout.ms' <= 'group.max.session.timeout.ms' (30 minutes)
 *
result

09:59:28 [INFO] org.ars.kaf.ConsumerNotClose1 - start:09:59:28.028
09:59:28 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:0 09:59:28.028
09:59:28 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:1 09:59:28.028
09:59:28 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:2 09:59:28.028

halt the first running

09:59:34 [INFO] org.ars.kaf.ConsumerNotClose1 - start:09:59:34.034
09:59:35 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:0 09:59:34.034
09:59:35 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:1 09:59:35.035
09:59:35 [INFO] org.ars.kaf.ConsumerNotClose1$Producer - send:2 09:59:35.035
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - timeout:8468
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:0 09:59:28.028 46
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:1 09:59:28.028 47
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:2 09:59:28.028 48
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:0 09:59:34.034 49
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:1 09:59:35.035 50
09:59:44 [INFO] org.ars.kaf.ConsumerNotClose1$Consumer - get:2 09:59:35.035 51
*/
public class ConsumerNotClose1 {

    static Logger log = LogManager.getLogger( ConsumerNotClose1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumerNotClose1.class.getSimpleName();
    static String group = topic;

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

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
                for( int i = 0; i < 3; i++) {
                    long ms = System.currentTimeMillis();
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, ms);
                    producer.send( record);
                    log.info( String.format( "send:%d %s", record.key(), dateFormat.format( new Date( ms))));
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
                // config.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // true by default
                config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000"); // 'session.timeout.ms' 45 seconds by default
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
                        log.info( "timeout:" + (System.currentTimeMillis() - start));
                    }
                    for( ConsumerRecord<Integer, Long> record : records) {
                        log.info( String.format( "get:%d %s %d", record.key(), dateFormat.format( new Date( record.value())), record.offset()));
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
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            log.info( "topic:" + topic);
            log.info( "group:" + group);

            Thread producerThread = new Thread( new Producer());
            Thread consumerThread = new Thread( new Consumer());

            producerThread.start();
            consumerThread.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}