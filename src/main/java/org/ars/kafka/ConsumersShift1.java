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
 * one producer, partition and multiple consumers with shift on during consuming
 * a new consumer will be picked up after 'heartbeat.interval.ms' 3 seconds by default
 * The value must be set lower than 'session.timeout.ms', but typically should be set no higher than 1/3 of that value
*/
public class ConsumersShift1 {

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumersShift1.class.getSimpleName();
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
                for( int i = 0; i < 10; i++) {
                    long ms = System.currentTimeMillis();
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, i, ms);
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

        public Consumer( String client, String group) {
            try {
                config.put( ConsumerConfig.CLIENT_ID_CONFIG, client);
                config.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
                config.put( ConsumerConfig.GROUP_ID_CONFIG, group);
                config.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);// max.poll.records
                // config.put( ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000); // heartbeat.interval.ms
                config.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
                config.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

                this.consumer = new KafkaConsumer<>( config);
            } catch( Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            System.out.println( String.format( "consumer:%d:start", currentThread().getId()));
            try {
                boolean interupt = false;
                consumer.subscribe( Arrays.asList( topic));
                while( !closed.get()) {
                    if( interupt) {
                        System.out.println( String.format( "consumer:%d:break", currentThread().getId()));
                        break;
                    }
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 100));
                    // System.out.println( String.format( "consumer:%d:poll:%d", currentThread().getId(), records.count()));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        System.out.println( String.format( "get:%s-%s %s %s", currentThread().getId(), record.key(), dateFormat.format( new Date( record.value())), record.offset()));
                        count++;
                        if( count % 5 == 0 && flag.compareAndSet( false, true)) {
                            interupt = true;
                        }
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println( String.format( "consumer:%d:stop processed:%d %s", currentThread().getId(), count, consumer.groupMetadata().groupId()));
                // consumer.unsubscribe(); //reset partition offset, another thread will pick up the same records
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

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            System.out.println( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            System.out.println( "topic:" + topic);

            producerThread.start();
            consumerThread1.start();
            consumerThread2.start();
            sleep( 4000); // has to be more than 'heartbeat.interval.ms' 3 seconds by default
            consumer1.shutdown();
            consumer2.shutdown();

            System.out.println( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            System.out.println( e);
        }
    }
}
