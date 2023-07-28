package org.ars.kafka.stream.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.ars.kafka.stream.KStream1;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * @author arsen.ibragimov
 *
 */
class KStreamUnitTest1 {

    private static TopologyTestDriver testDriver;

    private static TestInputTopic<Integer, Integer> inputTopic;

    private static TestOutputTopic<Integer, Integer> outputTopic;

    @BeforeAll
    static void init() {
        Properties config = new Properties();

        config.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        config.put( StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

        Topology topology = KStream1.Consumer.build();
        topology.addSink( "sinkProcessor", "result-topic", "KSTREAM-SOURCE-0000000000");

        testDriver = new TopologyTestDriver( topology, config);

        inputTopic = testDriver.createInputTopic( "KStream1", Serdes.Integer().serializer(), Serdes.Integer().serializer());
        outputTopic = testDriver.createOutputTopic( "result-topic", Serdes.Integer().deserializer(), Serdes.Integer().deserializer());
    }

    @Test
    void testCheckValue() {
        inputTopic.pipeInput( 1, 2);
        TestRecord<Integer, Integer> record = outputTopic.readRecord();
        assertEquals( 1, record.getKey());
        assertEquals( 2, record.getValue());
    }

    @AfterAll
    static void close() {
        testDriver.close();
    }
}
