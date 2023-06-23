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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @author arsen.ibragimov
 *
 * Note: similar to @see org.ars.kafka.ConsumersShift1
 *
 * One producer, partition and multiple consumers with shift on during consuming.
 * A new consumer will be picked up after 'heartbeat.interval.ms=5' seconds
 * The value must be set lower than 'session.timeout.ms', but typically should be set no higher than 1/3 of that value
 *
08:32:22 [INFO] org.ars.kaf.ConsumerUnsubscribe2$Consumer - client1 08:32:21.021 9
08:32:22 [INFO] org.ars.kaf.ConsumerUnsubscribe2$Consumer - client1:break unsubscribe
08:32:22 [INFO] org.apa.kaf.cli.con.KafkaConsumer - [Consumer clientId=client1, groupId=group1] Unsubscribed all topics or patterns and assigned partitions
08:32:22 [INFO] org.apa.kaf.com.uti.AppInfoParser - App info kafka.consumer for client1 unregistered
08:32:22 [DEBUG] org.apa.kaf.cli.con.KafkaConsumer - [Consumer clientId=client1, groupId=group1] Kafka consumer has been closed

08:32:27 [DEBUG] org.apa.kaf.cli.con.int.AbstractCoordinator - [Consumer clientId=client2, groupId=group1] Sending Heartbeat request with generation 437 and member id client2-c1c7e8f2-965d-404e-a497-3d0eab02d361 to coordinator kafka:9091 (id: 2147482646 rack: null)
08:32:27 [INFO] org.ars.kaf.ConsumerUnsubscribe2$Consumer - client2 08:32:22.022 10
08:32:27 [INFO] org.ars.kaf.ConsumerUnsubscribe2$Consumer - client2 08:32:22.022 11
*/
public class ConsumerUnsubscribe2 {

    static Logger log = LoggerFactory.getLogger( ConsumerUnsubscribe2.class);

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumerUnsubscribe2.class.getSimpleName();
    static AtomicBoolean flag = new AtomicBoolean();

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
                for( int i = 0; i < 3; i++) {
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
                config.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);// max.poll.records
                config.put( ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 5000); // heartbeat.interval.ms
                // config.put( ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // auto.commit.interval.ms
                config.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
                config.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

                this.consumer = new KafkaConsumer<>( config);
            } catch( Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            log.info( String.format( "%s:start", this.client));
            boolean interrupt = false;
            try {
                consumer.subscribe( Arrays.asList( topic));

                while( !closed.get()) {
                    if( interrupt) {
                        log.info( String.format( "%s:break unsubscribe", this.client));
                        consumer.commitSync();
                        consumer.unsubscribe();
                        break;
                    }
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 2000));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        log.info( String.format( "%s %s %d", this.client, dateFormat.format( new Date( record.value())), record.offset()));
                        count++;
                        if( count % 1 == 0 && flag.compareAndSet( false, true)) {
                            interrupt = true;
                        }
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

            log.info( "topic:" + topic);

            Consumer consumer1 = new Consumer( "client1", "group1");
            Thread consumerThread1 = new Thread( consumer1);

            Consumer consumer2 = new Consumer( "client2", "group1");
            Thread consumerThread2 = new Thread( consumer2);

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            producerThread.start();
            consumerThread1.start();
            consumerThread2.start();
            sleep( 7000); // has to be more than 'heartbeat.interval.ms' 5 seconds
            consumer1.shutdown();
            consumer2.shutdown();

            log.info( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
