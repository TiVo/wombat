/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class KafkaOutputter {

    private ObjectMapper sortedMapper;
    private ObjectMapper unsortedMapper;
    private String kafkaTopicBase;
    private KafkaProducer<String, String> kafkaProducer;
    List<Future<RecordMetadata>> responses;
    
    public KafkaOutputter(String kafkaBroker, int kafkaPort,
            String kafkaTopicBase) {
        sortedMapper = new ObjectMapper();
        sortedMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        unsortedMapper = new ObjectMapper();
        
        // Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker + ":" + kafkaPort);
        props.put("request.required.acks", "all");


        this.kafkaTopicBase = kafkaTopicBase;
        
        kafkaProducer = new KafkaProducer<String, String>(props,
                new StringSerializer(),
                new StringSerializer());
        
        sortedMapper = new ObjectMapper();
        sortedMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

        unsortedMapper = new ObjectMapper();
        
        responses = new ArrayList<Future<RecordMetadata>>();
    }

    public void output(DataChange change) throws JsonProcessingException {
        // TODO Auto-generated method stub
        System.out.println("DATA CHANGE: ");
        String primaryKeyStr = sortedMapper.writeValueAsString(change.getPrimaryKey());
        
        String jsonString;
        if (change.getPayload() != null) {
            jsonString = unsortedMapper.writeValueAsString(change.getPayload());
        } else {
            jsonString = "";
        }

        BinlogEnvelope env = new BinlogEnvelope(jsonString)
        .withPrimaryKey(primaryKeyStr)
        .withTableName(change.getTableName())
        .withBinlogTransactionStartPosition(change.getTransactionStartPosition())
        .withBinlogFilename(change.getBinlogFilename())
        .withBinlogEventStartPosition(change.getEventStartPosition());
        

        String topicName = String.format("%s.%s", kafkaTopicBase, env.getTableName());

        String kafkaPayload = env.marshal();

        System.out.println(topicName);
        System.out.println(kafkaPayload);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                topicName, env.getPrimaryKey(), kafkaPayload);

        responses.add(kafkaProducer.send(record));
    }

    public void flush() throws InterruptedException, ExecutionException {
        // In 0.8.3, we will have producer.flush(). Until then, this is how to
        // block until all have been delivered.
        for (Future<RecordMetadata> future : responses) {
            // will throw an exception if there are any errors.
            future.get();
        }
        responses.clear();
    }
    

}
