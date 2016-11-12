/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.tivo.wombat.replicator.Envelope.ParserException;

public class Listener {
    
    public static void main(String[] args) throws ClassNotFoundException, ParserException, IOException {
        
        String propertiesFilename = System.getenv("CONFIG_FILE");
        if (propertiesFilename == null) {
            throw new RuntimeException("environment variable 'CONFIG_FILE' is not set");
        }
        
        Properties prop = new Properties();
        
        try (InputStream input = new FileInputStream(propertiesFilename)) {
            prop.load(input);
        }
                 
        String mysqlServerId = prop.getProperty("MYSQL_SOURCE_IDENTIFIER");
        String mysqlHost = prop.getProperty("MYSQL_SOURCE_HOST");
        String mysqlPortStr = prop.getProperty("MYSQL_SOURCE_PORT");
        String mysqlUser = prop.getProperty("MYSQL_SOURCE_USER");
        String mysqlPassword = prop.getProperty("MYSQL_SOURCE_PASSWORD");

        if (mysqlServerId == null || mysqlHost == null || mysqlPortStr == null || mysqlUser == null || mysqlPassword == null) {
            throw new RuntimeException("missing parameters. Check that the following properties all exist in " + propertiesFilename + ": MYSQL_SOURCE_IDENTIFIER, MYSQL_SOURCE_HOST, MYSQL_SOURCE_PORT, MYSQL_SOURCE_USER, MYSQL_SOURCE_PASSWORD");
        }
        
        if (mysqlServerId.equals("") || mysqlHost.equals("") || mysqlPortStr.equals("") || mysqlUser.equals("")) {
            throw new RuntimeException("missing parameters. Check that the following properties are not empty: MYSQL_SOURCE_IDENTIFIER, MYSQL_SOURCE_HOST, MYSQL_SOURCE_PORT, MYSQL_SOURCE_USER");
        }
        
        System.out.println("Replicating from:");
        System.out.println("MYSQL_SOURCE_IDENTIFIER: " + mysqlServerId);
        System.out.println("MYSQL_SOURCE_HOST: " + mysqlHost);
        System.out.println("MYSQL_SOURCE_PORT: " + mysqlPortStr);
        System.out.println("MYSQL_SOURCE_USER: " + mysqlUser);
        System.out.println("MYSQL_SOURCE_PASSWORD: <not displayed>");
        
        int mysqlPort = Integer.parseInt(mysqlPortStr);
        
        BinaryLogClient client = new BinaryLogClient(
                mysqlHost,
                mysqlPort,
                mysqlUser,
                mysqlPassword);
        
        // load checkpoint location

        ICheckpointer checkpointHelper;
        boolean shouldCheckpoint = true;
        if (shouldCheckpoint) {
            String checkpointFilename = System.getenv("CHECKPOINT_FILE");
            if (checkpointFilename == null) {
                throw new RuntimeException("environment variable 'CHECKPOINT_FILE' is not set");
            }

            checkpointHelper = new FileCheckpointer(checkpointFilename);
        } else {
            checkpointHelper = new NonCheckpointer();
        }
        client.setBinlogFilename(checkpointHelper.getBinlogFilename());
        client.setBinlogPosition(checkpointHelper.getBinlogPosition());
        
                client.setKeepAlive(false);
        // explicitly set binlog name to null. That gets the most recent binlog number.
        client.setServerId(1337); // what should this be? Needs to be unique across all systems.
        
        String kafkaBroker = prop.getProperty("KAFKA_BROKER");
        String kafkaPortStr = prop.getProperty("KAFKA_PORT");
        String kafkaTopicBase = prop.getProperty("KAFKA_TOPIC_BASE");

        if (kafkaBroker == null || kafkaPortStr == null || kafkaTopicBase == null || 
                kafkaBroker.equals("") || kafkaPortStr.equals("") || kafkaTopicBase.equals("")) {
            throw new RuntimeException("missing parameters. Check that the following properties all exist in " + propertiesFilename + " are are not empty: KAFKA_BROKER, KAFKA_PORT, KAFKA_TOPIC_BASE");
        }

        int kafkaPort = Integer.parseInt(kafkaPortStr);
        
        System.out.println("Replicating to:");
        System.out.println("KAFKA_BROKER: " + kafkaBroker);
        System.out.println("KAKFA_PORT: " + kafkaPortStr);
        System.out.println("KAFKA_TOPIC_BASE: " + kafkaTopicBase);
        
        KafkaOutputter outputter = new KafkaOutputter(kafkaBroker,
                kafkaPort,
                kafkaTopicBase);

        EventHandler handler = new EventHandler(
                mysqlHost,
                mysqlPort,
                mysqlUser,
                mysqlPassword,
                mysqlServerId,
                checkpointHelper,
                outputter);

        client.registerEventListener(handler);
        client.registerLifecycleListener(handler);
        
        ConnectionLifecycleListener lifecycleListener = new ConnectionLifecycleListener();
        client.registerLifecycleListener(lifecycleListener);

        // connect once. If I get disconnected, this will exit. This is the
        // right thing to do, during development, so I can learn what types of
        // errors happen.
        client.connect();
        
        System.out.println("Ended");
    }
}
