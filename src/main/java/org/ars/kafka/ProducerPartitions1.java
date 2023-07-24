package org.ars.kafka;

import static java.lang.Thread.sleep;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/*
 * @author arsen.ibragimov
 *
 * one producer, 2 partitions and 2 consumers in the same group
 *
 * create topic with 2 partitions
 * ./kafka-topics.sh --bootstrap-server localhost:9091 --create --topic ProducerPartitions1 --partitions 2
 * or alter if it already exists
 * ./kafka-topics.sh --bootstrap-server localhost:9091 --alter --topic ProducerPartitions1 --partitions 2
 *
 * Note: if active 2 brokers than create partition on each of them otherwise on one
 *
result:

15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partitions.size:2
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:0, offset:65
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:1, offset:65
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:0, offset:66
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:1, offset:66
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:0, offset:67
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:1, offset:67
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:0, offset:68
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:1, offset:68
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:0, offset:69
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1 partition:1, offset:69
15:17:55 [INFO] org.ars.kaf.ProducerPartitions1$Producer - producer1:stop sent:10
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1:start
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2:start
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2 key:1, value:15:17:55.055, partition:1, offset:65
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1 key:0, value:15:17:55.055, partition:0, offset:65
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2 key:3, value:15:17:55.055, partition:1, offset:66
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1 key:2, value:15:17:55.055, partition:0, offset:66
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2 key:5, value:15:17:55.055, partition:1, offset:67
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1 key:4, value:15:17:55.055, partition:0, offset:67
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2 key:7, value:15:17:55.055, partition:1, offset:68
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1 key:6, value:15:17:55.055, partition:0, offset:68
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2 key:9, value:15:17:55.055, partition:1, offset:69
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1 key:8, value:15:17:55.055, partition:0, offset:69
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1 - stop:15:17:56.056
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client2:stop processed:5 group1
15:17:56 [INFO] org.ars.kaf.ProducerPartitions1$Consumer - client1:stop processed:5 group1
 */
public class ProducerPartitions1 {

    static Logger log = LogManager.getLogger( ProducerPartitions1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.SSS";
    static String topic = ProducerPartitions1.class.getSimpleName();

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Long> producer = null;
        SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
        long count;
        String client;

        public Producer( String client) {
            this.client = client;
            config.put( ProducerConfig.CLIENT_ID_CONFIG, this.client);
            config.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
            config.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");

            producer = new KafkaProducer<>( config);
        }

        @Override
        public void run() {
            try {
                int partitionsSize = producer.partitionsFor( topic).size();
                log.info( String.format( "%s partitions.size:%d", this.client, partitionsSize));
                for( int i = 0; i < 10; i++) {
                    int partition = i % partitionsSize; // the same logic to determine partition number is applied into kafka
                    ProducerRecord<Integer, Long> record = new ProducerRecord<>( topic, partition, i, System.currentTimeMillis());
                    Future<RecordMetadata> res = producer.send( record);
                    log.info( String.format( "%s partition:%d, offset:%d", this.client, res.get().partition(), res.get().offset()));
                    count++;
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( String.format( "%s:stop sent:%d", this.client, count));
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
                // config.put( ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);// max.poll.records
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
                        log.info( String.format( "%s key:%d, value:%s, partition:%d, offset:%d", this.client, record.key(), dateFormat.format( new Date( record.value())), record.partition(),
                                record.offset()));
                        count++;
                    }
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                log.info( String.format( "%s:stop processed:%d %s", this.client, this.count, this.consumer.groupMetadata().groupId()));
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set( true);
        }
    }

    public static void main( String[] args) {
        try {
            Producer producer = new Producer( "producer1");
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
            sleep( 500);
            consumer1.shutdown();
            consumer2.shutdown();

            log.info( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
