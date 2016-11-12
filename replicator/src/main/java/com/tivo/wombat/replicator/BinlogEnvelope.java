/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import org.apache.commons.lang3.tuple.Pair;

public class BinlogEnvelope extends Envelope {

    private String primaryKey;
    private String tableName;

    public BinlogEnvelope(String payload) {
        super("BL/1", payload);
    }
    
    public BinlogEnvelope withBinlogFilename(String binlogFilename) {
        addHeader("BinlogFilename", binlogFilename);
        return this;
    }

    public BinlogEnvelope withBinlogTransactionStartPosition(long transactionStartPosition) {
        addHeader("BinlogTransactionStartPosition", String.valueOf(transactionStartPosition));
        return this;
    }
    
    public BinlogEnvelope withBinlogEventStartPosition(long eventStartPosition) {
        addHeader("BinlogEventStartPosition", String.valueOf(eventStartPosition));
        return this;
    }
    
    public BinlogEnvelope withRowInTransaction(long rowIdxInTransaction) {
        addHeader("RowInTransaction", String.valueOf(rowIdxInTransaction));
        return this;
    }
    
    public BinlogEnvelope withTableName(Pair<String, String> tableUniqueName) {
        this.tableName = String.format("%s.%s", tableUniqueName.getLeft(), tableUniqueName.getRight());
        return this;
    }
    
    public String getTableName() {
        return tableName;
    }

    public BinlogEnvelope withPrimaryKey(String primaryKeyValue) {
        this.primaryKey = primaryKeyValue;
        return this;
    }
    
    public String getPrimaryKey() {
        return primaryKey;
    }

    public BinlogEnvelope withServerId(String serverId) {
        addHeader("ServerId", serverId);
        return this;
    }
}
