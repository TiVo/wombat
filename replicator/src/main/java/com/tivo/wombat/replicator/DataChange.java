/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

public class DataChange {

    private Map<String, Serializable> payload;
    private Map<String, Serializable> primaryKey;
    private Pair<String, String> tableName;
    private long transactionStartPosition;
    private String binlogFilename;
    private long eventStartPosition;

    public void setPayload(Map<String, Serializable> payload) {
        // TODO Auto-generated method stub
        this.payload = payload;
        
    }

    public void setPrimaryKey(Map<String, Serializable> primaryKey) {
        // TODO Auto-generated method stub
        this.primaryKey = primaryKey;
        
    }

    public void setTableName(Pair<String, String> tableName) {
        // TODO Auto-generated method stub
        this.tableName = tableName;
    }

    public void setOffset(long transactionStartPosition, String binlogFilename, long eventStartPosition) {
        // TODO Auto-generated method stub
        this.transactionStartPosition = transactionStartPosition;
        this.binlogFilename = binlogFilename;
        this.eventStartPosition = eventStartPosition;
    }

    public void setServerId(String mysqlServerId) {
        // TODO Auto-generated method stub
        
    }

    public Map<String, Serializable> getPrimaryKey() {
        // TODO Auto-generated method stub
        return primaryKey;
    }

    public Pair<String, String> getTableName() {
        return tableName;
    }

    public Map<String, Serializable> getPayload() {
        // TODO Auto-generated method stub
        return payload;
    }

    public long getTransactionStartPosition() {
        return transactionStartPosition;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    public long getEventStartPosition() {
        return eventStartPosition;
    }
}
