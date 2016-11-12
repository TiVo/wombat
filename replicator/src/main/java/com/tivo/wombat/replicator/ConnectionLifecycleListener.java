/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;

public class ConnectionLifecycleListener implements LifecycleListener {

    public enum Reason {
        BINLOG_NOT_AVAILABLE,
        EOF,
        SOCKET_EXCEPTION
    }

    private Reason failureReason;
            
    @Override
    public void onConnect(BinaryLogClient client) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
        // TODO Auto-generated method stub
        if (ex.getMessage()
                .equals("1236 - Could not find first log file name in binary log index file")) {
            // The binary log has been rotate out from the master, so we can't
            // reconnect to it. Shutdown the listener, and tell the operator
            // that we need to rebootstrap.
            System.out.println(String.format("Binlog %s/%d is no longer available on the master. Need to re-bootstrap.", 
                    client.getBinlogFilename(), client.getBinlogPosition()));
            this.failureReason = Reason.BINLOG_NOT_AVAILABLE;
        }
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient client,
            Exception ex) {
        // TODO Auto-generated method stub

    }

    @Override
    public void onDisconnect(BinaryLogClient client) {
        // TODO Auto-generated method stub
    }

    public Reason getFailureReason() {
        return failureReason;
    }

}
