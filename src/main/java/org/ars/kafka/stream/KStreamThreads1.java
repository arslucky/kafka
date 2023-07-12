package org.ars.kafka.stream;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.io.IOException;
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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;


/**
 * @author arsen.ibragimov
 *
 *         calculate keys count in multiple stream threads
 *         create topic with 2 partitions before starting a test
 *         ./kafka-topics.sh --bootstrap-server kafka1:9091 --create --partitions 2 --topic KStreamThreads1
 */
public class KStreamThreads1 {

    static Logger log = LogManager.getLogger( KStreamThreads1.class);

    static String topic = KStreamThreads1.class.getSimpleName();

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
                    log.info( String.format( "send key:%d, value:%d, partitions:%d", key, i, res.get().partition()));
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

        public Consumer() throws IOException {
            String baseDir = "./kafka-streams/" + KStreamThreads1.class.getSimpleName();

            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.STATE_DIR_CONFIG, baseDir);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        }

        @Override
        public void run() {
            try {
                log.info( currentThread().getName() + ":consumer:start");
                StreamsBuilder builder = new StreamsBuilder();
                builder.stream( topic).groupByKey().count().toStream().foreach( ( key, value) -> {
                    log.info( String.format( "%s key:%d, count:%d", currentThread().getName(), key, value));
                });
                streams = new KafkaStreams( builder.build(), config);

                streams.start();
                sleep( 1500);
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                streams.close();
                log.info( String.format( "%s consumer:stop:state:%s", currentThread().getName(), streams.state().name()));
            }
        }
    }

    public static void main( String[] args) {
        try {
            Thread thread1 = new Thread( new Producer());
            Thread thread2 = new Thread( new Consumer());

            thread1.start();
            sleep( 1000);
            thread2.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
