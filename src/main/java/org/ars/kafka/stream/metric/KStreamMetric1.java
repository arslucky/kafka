package org.ars.kafka.stream.metric;

import static java.lang.Thread.sleep;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
 */
public class KStreamMetric1 {

    static Logger log = LogManager.getLogger( KStreamMetric1.class);

    static String topic = KStreamMetric1.class.getSimpleName();

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
                int key = 1;
                for( int i = 0; i < 3; i++) {
                    ProducerRecord<Integer, Integer> record = new ProducerRecord<>( topic, key, i);
                    producer.send( record);
                    log.info( "send:" + i);
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

        public Consumer() {
            config.put( StreamsConfig.APPLICATION_ID_CONFIG, topic + "-app");
            config.put( StreamsConfig.CLIENT_ID_CONFIG, "client1");

            config.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
            // rebalance optimization
            config.put( "internal.leave.group.on.close", "true");
            config.put( ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");

            config.put( StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
        }

        @Override
        public void run() {
            try {
                log.info( "consumer:start");
                StreamsBuilder builder = new StreamsBuilder();
                builder.stream( topic).foreach( ( key, value) -> {
                    log.info( "key:{}, value:{}", key, value);
                });
                streams = new KafkaStreams( builder.build(), config);

                Runtime.getRuntime().addShutdownHook( new Thread( streams::close));

                streams.start();
                sleep( 1500);

                for( Map.Entry<MetricName, ? extends Metric> metricNameEntry : streams.metrics().entrySet()) {
                    Metric metric = metricNameEntry.getValue();
                    MetricName metricName = metricNameEntry.getKey();
                    if( !metric.metricValue().equals( 0.0) && !metric.metricValue().equals( Double.NEGATIVE_INFINITY)) {
                        log.info( "MetricName {} = {}", metricName.name(), metric.metricValue());
                    }
                }
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
            Thread thread2 = new Thread( new Consumer());

            thread1.start();
            sleep( 500);
            thread2.start();
        } catch( Exception e) {
            e.printStackTrace();
        }
    }
}
