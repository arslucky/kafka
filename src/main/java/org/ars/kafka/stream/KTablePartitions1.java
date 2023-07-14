package org.ars.kafka.stream;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * @author arsen.ibragimov
 *
 *         KTable partitions output
 *         'Stateful transformations depend on state for processing inputs and producing outputs and require a state store associated with the stream processor.'
 *         ./kafka-topics.sh --bootstrap-server kafka1:9091 --create --partitions 2 --topic KTablePartitions1
 */
public class KTablePartitions1 {

    static Logger log = LogManager.getLogger( KTablePartitions1.class);

    static String topic = KTablePartitions1.class.getSimpleName();

    static final String BOOTSTRAP_SERVERS = "kafka1:9091";

    static {
        Configurator.setRootLevel( Level.WARN);
        Configurator.setLevel( "org.apache.kafka.clients.consumer", Level.WARN);
    }

    static class Producer implements Runnable {

        Properties config = new Properties();
        KafkaProducer<Integer, Integer> producer = null;

        public Producer() {
            config.put( ProducerConfig.CLIENT_ID_CONFIG, "producer1");
            config.put( ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
            config.put( ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

            producer = new KafkaProducer<>( config);
        }

        @Override
        public void run() {
            log.info( "producer:start");
            try {
                for( int i = 0; i < 3; i++) {
                    int key = i;
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, key, i);
                    Future<RecordMetadata> res = producer.send( record);
                    log.info( String.format( "%s send key:%d, value:%d, partitions:%d", currentThread().getName(), key, i, res.get().partition()));
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

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer( int index) {
            String baseDir = String.format( "./kafka-streams/%s/%s", KTablePartitions1.class.getSimpleName(), index);

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.STATE_DIR_CONFIG, baseDir);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"); // default 1
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        }

        @Override
        public void run() {
            try {
                log.info( "consumer:start");
                StreamsBuilder builder = new StreamsBuilder();

                KStream<Integer, Integer> stream = builder.stream( topic);

                stream.foreach( ( key, value) -> {
                    log.info( String.format( "%s get key:%d, value:%d", currentThread().getName(), key, value));
                });

                KTable<Integer, Long> tableCount = stream.groupByKey().count();

                tableCount.toStream().foreach( ( key, value) -> {
                    log.info( String.format( "%s tableCount key:%d, count:%d", currentThread().getName(), key, value));
                });

                KTable<Integer, Integer> tableSum = stream.groupByKey().reduce( ( val1, val2) -> val1 + val2);

                tableSum.toStream().foreach( ( key, value) -> {
                    log.info( String.format( "%s tableSum key:%d, sum:%d", currentThread().getName(), key, value));
                });

                streams = new KafkaStreams( builder.build(), config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 1500);
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                streams.close();
                log.info( "consumer:stop");
            }
        }
    }

    public static void main( String[] args) {
        try {
            Thread thread1 = new Thread( new Producer());
            Thread thread2 = new Thread( new Consumer( 1));
            Thread thread3 = new Thread( new Consumer( 2));

            thread1.start();
            sleep( 500);
            thread2.start();
            thread3.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
