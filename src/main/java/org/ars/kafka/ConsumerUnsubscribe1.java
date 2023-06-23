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
 * max.poll.records = 1
 * auto.commit.interval.ms = 1000
 * delay between poll loop 3000 ms
 *
 * result:
 * 1. the 'commit complete' request is sent at next 'poll loop' iteration, each 3 seconds even if interval was sent to 1 second
 * 2. send commit before unsubscribing
 *
--------- First start --------------
07:42:47 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:42:46.046 380

07:42:50 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Sending asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=381, leaderEpoch=0, metadata=''}}
07:42:50 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:42:47.047 381
07:42:50 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator$OffsetCommitResponseHandler - [Consumer clientId=client1, groupId=group1] Committed offset 381 for partition ConsumerUnsubscribe1-0

07:42:53 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Completed asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=381, leaderEpoch=0, metadata=''}}
07:42:53 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Sending asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=382, leaderEpoch=0, metadata=''}}
07:42:53 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:42:47.047 382
07:42:53 [DEBUG] org.apa.kaf.cli.con.int.AbstractCoordinator$HeartbeatResponseHandler - [Consumer clientId=client1, groupId=group1] Received successful Heartbeat response
07:42:53 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator$OffsetCommitResponseHandler - [Consumer clientId=client1, groupId=group1] Committed offset 382 for partition ConsumerUnsubscribe1-0

07:42:56 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1:break unsubscribe

---------- Second start, 382 offset picked up again as not committed --------------
07:46:23 [DEBUG] org.apa.kaf.cli.con.int.AbstractFetch - [Consumer clientId=client1, groupId=group1] Fetch READ_UNCOMMITTED at offset 382 for partition ConsumerUnsubscribe1-0 returned fetch data PartitionData(partitionIndex=0, errorCode=0, highWatermark=386,
07:46:23 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:42:47.047 382

07:46:26 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Sending asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=383, leaderEpoch=0, metadata=''}}
07:46:26 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:46:22.022 383
07:46:26 [DEBUG] org.apa.kaf.cli.con.int.AbstractCoordinator$HeartbeatResponseHandler - [Consumer clientId=client1, groupId=group1] Received successful Heartbeat response
07:46:26 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator$OffsetCommitResponseHandler - [Consumer clientId=client1, groupId=group1] Committed offset 383 for partition ConsumerUnsubscribe1-0

07:46:29 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Completed asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=383, leaderEpoch=0, metadata=''}}
07:46:29 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator - [Consumer clientId=client1, groupId=group1] Sending asynchronous auto-commit of offsets {ConsumerUnsubscribe1-0=OffsetAndMetadata{offset=384, leaderEpoch=0, metadata=''}}
07:46:29 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1 07:46:23.023 384
07:46:29 [DEBUG] org.apa.kaf.cli.con.int.ConsumerCoordinator$OffsetCommitResponseHandler - [Consumer clientId=client1, groupId=group1] Committed offset 384 for partition ConsumerUnsubscribe1-0
07:46:32 [INFO] org.ars.kaf.ConsumerUnsubscribe1$Consumer - client1:break unsubscribe
*/
public class ConsumerUnsubscribe1 {

    static Logger log = LoggerFactory.getLogger( ConsumerUnsubscribe1.class);

    private static final String TIME_FORMAT = "kk:mm:ss.sss";
    static String topic = ConsumerUnsubscribe1.class.getSimpleName();

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
                // config.put( ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500); // heartbeat.interval.ms
                config.put( ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000); // auto.commit.interval.ms
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
                        consumer.unsubscribe();
                        break;
                    }
                    ConsumerRecords<Integer, Long> records = consumer.poll( Duration.ofMillis( 2000));
                    for( ConsumerRecord<Integer, Long> record : records) {
                        log.info( String.format( "%s %s %d", this.client, dateFormat.format( new Date( record.value())), record.offset()));
                        count++;
                        if( count % 3 == 0) {
                            interrupt = true;
                        }
                        sleep( 3000);
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

            SimpleDateFormat dateFormat = new SimpleDateFormat( TIME_FORMAT);
            log.info( "start:" + dateFormat.format( new Date( System.currentTimeMillis())));

            producerThread.start();
            consumerThread1.start();
            sleep( 10000);
            consumer1.shutdown();

            log.info( "stop:" + dateFormat.format( new Date( System.currentTimeMillis())));
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
