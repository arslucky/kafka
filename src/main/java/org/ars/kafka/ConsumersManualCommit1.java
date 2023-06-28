package org.ars.kafka;

import static java.lang.Thread.sleep;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/*
 * @author arsen.ibragimov
 *
 * Manual commit offset
 */
public class ConsumersManualCommit1 {

    static Logger log = LogManager.getLogger( ConsumersManualCommit1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumersManualCommit1.class.getSimpleName();
    static AtomicBoolean flag = new AtomicBoolean();

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
                for( int i = 0; i < 5; i++) {
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
                config.put( ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                config.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");// max.poll.records
                config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
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
            boolean interrupt = false;
            try {
                consumer.subscribe( Arrays.asList( topic));

                while( !closed.get()) {
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 100));
                    for( TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<Integer, Long>> partitionRecords = records.records( partition);
                        for( ConsumerRecord<Integer, Long> record : partitionRecords) {
                            log.info( String.format( "%s, partition:%d, offset:%d, value:%s", this.client, record.partition(), record.offset(), dateFormat.format( new Date( record.value()))));
                            count++;

                            if( count % 3 == 0 && flag.compareAndSet( false, true)) {
                                log.info( String.format( "%s:break unsubscribe", this.client));
                                interrupt = true;
                                long lastOffset = partitionRecords.get( partitionRecords.size() - 1).offset();
                                consumer.commitSync( Collections.singletonMap( partition, new OffsetAndMetadata( lastOffset + 1 /* next consumed record */)));
                                consumer.unsubscribe(); // the first thread fall out in ~'session.timeout.ms=10000' + 'heartbeat.interval.ms=3000' interval due to not closed
                                break;
                            }
                        }
                        if( interrupt) {
                            break;
                        }
                    }
                    if( interrupt) {
                        break;
                    }
                }

            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                log.info( String.format( "%s:stop processed:%d %s", this.client, count, consumer.groupMetadata().groupId()));
                if( !interrupt) {
                    consumer.commitSync(); // second thread commit
                    consumer.close();
                }
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

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            log.info( "topic:" + topic);

            producerThread.start();
            sleep( 500);
            consumerThread1.start();
            consumerThread2.start();
            sleep( 15000);
            consumer1.shutdown();
            consumer2.shutdown();

            log.info( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
