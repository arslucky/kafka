package org.ars.kafka.stream;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
 *         put stream client closing to shutdown hook
 */
public class KStreamShutdownHook1 {

    static Logger log = LogManager.getLogger( KStreamShutdownHook1.class);

    static String topic = KStreamShutdownHook1.class.getSimpleName();

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
            log.info( currentThread().getName() + ":producer:start");
            try {
                int key = 1;
                for( int i = 0; i < 3; i++) {
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, key, i);
                    producer.send( record);
                    log.info( String.format( "%s:send key:%d, value:%d", currentThread().getName(), key, i));
                }
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
                log.info( currentThread().getName() + ":producer:stop");
            }
        }
    }

    static class Consumer implements Runnable {

        Properties config = new Properties();
        KafkaStreams streams = null;

        public Consumer() {
            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "_client");
            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");
        }

        @Override
        public void run() {
            try {
                log.info( currentThread().getName() + ":consumer:start");
                StreamsBuilder builder = new StreamsBuilder();
                builder.stream( topic).foreach( ( key, value) -> {
                    log.info( String.format( "%s:get key:%d, value:%d", currentThread().getName(), key, value));
                });
                streams = new KafkaStreams( builder.build(), config);

                Thread shutdownThread = new Thread( () -> {
                    // System.out.println( currentThread().getName() + ":shutdownThread:closing");
                    log.info( currentThread().getName() + ":shutdownThread:closing");
                    streams.close();
                });
                shutdownThread.setUncaughtExceptionHandler( ( t, e) -> {
                    log.error( e);
                });

                Runtime.getRuntime().addShutdownHook( shutdownThread);

                streams.start();
                sleep( 3000);
                System.exit( -1);
            } catch( Exception e) {
                e.printStackTrace();
            } finally {
                streams.close();
                log.info( currentThread().getName() + ":consumer:stop");
            }
        }
    }

    public static void main( String[] args) {
        try {
            Thread thread1 = new Thread( new Producer());
            Thread thread2 = new Thread( new Consumer());

            thread1.start();
            sleep( 500);
            thread2.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
