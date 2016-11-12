/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.XidEventData;

public class EventHandler implements BinaryLogClient.EventListener, LifecycleListener {


    private String hostname;
    private int port;
    private String username;
    private String password;

    private final HashMap<Long, TableMapEventData> tableIdToDataMap = new HashMap<Long, TableMapEventData>();
    private final HashMap<Pair<String, String>, Long> databaseNameToTableIdMap = new HashMap<Pair<String, String>, Long>();
;
    private HashMap<Pair<String, String>, TableMetadata> databaseNameToTableMetadata = new HashMap<Pair<String, String>, TableMetadata>();
    private long transactionStartPosition;
    private String binlogFilename;
    private String mysqlServerId;
    private ICheckpointer checkpointHelper;
    private BinaryLogClient binlogClient;
    private final KafkaOutputter outputter;
    
    public EventHandler(String hostname, int port, String username, String password,
            String mysqlServerId, ICheckpointer checkpointHelper, KafkaOutputter outputter) throws ClassNotFoundException {
        
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.mysqlServerId = mysqlServerId;
        this.checkpointHelper = checkpointHelper;

        // we're going to be loading this driver, so figure out early if there
        // is going to be a problem.
        Class.forName("com.mysql.jdbc.Driver");
        
        this.outputter = outputter;
    }

    @Override
    public void onEvent(Event event) {
        try {

            EventData data = event.getData();

            if (! (event.getHeader() instanceof EventHeaderV4)) {
                throw new RuntimeException("Header is not V4!");
            }
            System.out.println(event);  

            EventHeaderV4 header = event.getHeader();

            List<DataChange> dataChanges = new ArrayList<DataChange>();
            
            EventType type = header.getEventType();
            if (type == EventType.TABLE_MAP) {
                handleTableMap((TableMapEventData) data);
            } else if (EventType.isWrite(type)) {
                handleWriteRows((WriteRowsEventData) data, dataChanges);
            } else if (EventType.isDelete(type)) {
                handleDeleteRows((DeleteRowsEventData) data, dataChanges);
            } else if (EventType.isUpdate(type)) {
                handleUpdateRows((UpdateRowsEventData) data, dataChanges);
            } else if (type == EventType.QUERY) {
                handleQuery((QueryEventData) data, header);
            } else if (type == EventType.ROTATE) {
                handleRotate((RotateEventData) data);
            } else if (type == EventType.XID) {
                handleXid((XidEventData) data, header);
            }

            for (DataChange change : dataChanges) {
                change.setOffset(this.transactionStartPosition,
                        this.binlogFilename, header.getPosition());
                change.setServerId(this.mysqlServerId);
                outputter.output(change);
            }
            
            outputter.flush();
            
        } catch (Throwable e) {
            // during development, on any and all errors, disconnect.
            // This allows me to learn what errors will happen, and how to
            // handle each one appropriately.
            e.printStackTrace();
            try {
                this.binlogClient.disconnect();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

        }
    }
    
    private void handleXid(XidEventData data, EventHeaderV4 header) throws InterruptedException, ExecutionException {
        // at end of a transaction, checkpoint to start AFTER this transaction
        outputter.flush();
        long nextTransaction = header.getNextPosition();
        checkpointHelper.setBinlogFilename(this.binlogFilename);
        checkpointHelper.setBinlogPosition(nextTransaction);
        checkpointHelper.save();
    }

    private void handleTableMap(TableMapEventData data) throws SQLException {
        Pair<String, String> tableUniqueName = new ImmutablePair<String, String>(data.getDatabase(), data.getTable());
        if (databaseNameToTableIdMap.containsKey(tableUniqueName)) {
            // delete the id->info mapping
            System.out.println("Removing old db info!");
            long oldTableId = databaseNameToTableIdMap.get(tableUniqueName);
            tableIdToDataMap.remove(oldTableId);
        }
        tableIdToDataMap.put(data.getTableId(), data);
        databaseNameToTableIdMap.put(tableUniqueName, data.getTableId());

        if (databaseNameToTableMetadata.containsKey(tableUniqueName)) {
            return;
        }
        
        // lookup the data
        TableMetadata tableMetadata = new TableMetadata();

        String dbName = data.getDatabase();
        String tableName = data.getTable();

        // XXX I should close this connection!
        Connection connection;
        connection = DriverManager.getConnection(
                "jdbc:mysql://" + hostname + ":" + port + "/" + dbName, // notice the dbname is here. mysql's jdbc driver requires it. 
                username,
                password);
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        // get column names
        ResultSet rs = databaseMetaData.getColumns(null, null, tableName, null);
        // what about failures to connect to database? Need to make sure that we catch the case where we add an empty list, and then fail (meaning, no column names)
        ArrayList<String> columnNames = new ArrayList<String>();

        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            // 1 based, but we don't care. We just add them in order. This
            // assumes that the columns come in ORDINAL_POSITION order, but the
            // docs for getColumns() says they do.
            // String position = rs.getString("ORDINAL_POSITION");
            columnNames.add(columnName);
        }
        tableMetadata.setColumnNames(columnNames);

        // get primary keys
        ResultSet rs2 = databaseMetaData.getPrimaryKeys(null, null, tableName);

        List<String> primaryKeyColumns = new ArrayList<String>();
        while (rs2.next()) {
            primaryKeyColumns.add(rs2.getString("COLUMN_NAME"));
        }
        if (primaryKeyColumns.isEmpty()) {
            throw new RuntimeException(String.format("Table %s.%s does not have a primary key!", data.getDatabase(), data.getTable()));
        }
        tableMetadata.setPrimaryKeys(primaryKeyColumns);
        System.out.println(tableMetadata);

        // Only upon successful lookup, with no sql errors, do we save the data.
        databaseNameToTableMetadata.put(tableUniqueName, tableMetadata);
    }

    private void handleUpdateRows(UpdateRowsEventData data, List<DataChange> dataChanges) throws JsonProcessingException {
        System.out.println(data);
        TableMapEventData tableData = tableIdToDataMap.get(data.getTableId());
        Pair<String, String> tableUniqueName = new ImmutablePair<String, String>(tableData.getDatabase(), 
                tableData.getTable());
        TableMetadata tableMetadata = databaseNameToTableMetadata.get(tableUniqueName);
        // XXX Error handling if tableMetadata == null
        ArrayList<String> columnNames = tableMetadata.getColumnNames();
        List<Integer> primaryKeyIndices = tableMetadata.getPrimaryKeyIndices();
        
        // For N updates, we output up to 2*N rows. Because each update might
        // output a delete AND an insert, if the primary key has changed.
        List<Entry<Serializable[],Serializable[]>> rows = data.getRows();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {
            Serializable[] oldValues = row.getKey();
            Serializable[] newValues = row.getValue();

            // see if primary key changed. If so, issue a delete for the old one.
            Map<String, Serializable> oldPrimaryKey = new HashMap<String, Serializable>();
            for (int index : primaryKeyIndices) {
                String columnName = columnNames.get(index);
                Serializable value = oldValues[index];
                oldPrimaryKey.put(columnName, value);
            }
            Map<String, Serializable> newPrimaryKey = new HashMap<String, Serializable>();
            for (int index : primaryKeyIndices) {
                String columnName = columnNames.get(index);
                Serializable value = newValues[index];
                newPrimaryKey.put(columnName, value);
            }
            
            if (!oldPrimaryKey.equals(newPrimaryKey)) {
                DataChange change = new DataChange();
                change.setPayload(null);
                change.setPrimaryKey(oldPrimaryKey);
                change.setTableName(tableUniqueName);
                dataChanges.add(change);
                
            }
            
            Map<String,Serializable> map = new HashMap<String, Serializable>();

//          System.out.println(row);
            for (int i = 0; i < newValues.length; i++) {
                String column = columnNames.get(i);
                map.put(column, newValues[i]);
            }

            DataChange change = new DataChange();
            change.setPayload(map);
            change.setPrimaryKey(newPrimaryKey);
            change.setTableName(tableUniqueName);
                  
            dataChanges.add(change);
            
        }
    }

    private void handleDeleteRows(DeleteRowsEventData data, List<DataChange> dataChanges) throws JsonProcessingException {
        System.out.println(data);
        TableMapEventData tableData = tableIdToDataMap.get(data.getTableId());
        Pair<String, String> tableUniqueName = new ImmutablePair<String, String>(tableData.getDatabase(), 
                tableData.getTable());
        
        TableMetadata tableMetadata = databaseNameToTableMetadata.get(tableUniqueName);
        // XXX Error handling if tableMetadata == null
        ArrayList<String> columnNames = tableMetadata.getColumnNames();
        List<Integer> primaryKeyIndices = tableMetadata.getPrimaryKeyIndices();
        
        List<Serializable[]> rows = data.getRows();
        for (Serializable[] row: rows) {
            
            Map<String, Serializable> primaryKeyMap = new HashMap<String, Serializable>();
            for (int index : primaryKeyIndices) {
                String columnName = columnNames.get(index);
                Serializable value = row[index];
                primaryKeyMap.put(columnName, value);
            }
           
            DataChange change = new DataChange();
            change.setPayload(null);
            change.setPrimaryKey(primaryKeyMap);
            change.setTableName(tableUniqueName);
            
            dataChanges.add(change);
        }
    }

    private void handleWriteRows(WriteRowsEventData data, List<DataChange> dataChanges) throws JsonProcessingException {
        TableMapEventData tableData = tableIdToDataMap.get(data.getTableId());
        Pair<String, String> tableUniqueName = new ImmutablePair<String, String>(tableData.getDatabase(), 
                tableData.getTable());
        TableMetadata tableMetadata = databaseNameToTableMetadata.get(tableUniqueName);
        // XXX Error handling if tableMetadata == null

        ArrayList<String> columnNames = tableMetadata.getColumnNames();
        List<Integer> primaryKeyIndices = tableMetadata.getPrimaryKeyIndices();
        
        List<Serializable[]> rows = data.getRows();
        for (Serializable[] row: rows) {
            Map<String,Serializable> map = new HashMap<String, Serializable>();

//            System.out.println(row);
            for (int i = 0; i < row.length; i++) {
                String column = columnNames.get(i);
                map.put(column, row[i]);
            }

            Map<String, Serializable> primaryKeyMap = new HashMap<String, Serializable>();
            for (int index : primaryKeyIndices) {
                String columnName = columnNames.get(index);
                Serializable value = row[index];
                primaryKeyMap.put(columnName, value);
            }

            DataChange change = new DataChange();
            change.setPayload(map);
            change.setPrimaryKey(primaryKeyMap);
            change.setTableName(tableUniqueName);
            
            dataChanges.add(change);
        }
    }

    private void handleQuery(QueryEventData data, EventHeaderV4 header) {
        System.out.println("GOT A QUERY: " + data);
        String sql = data.getSql();
        sql = sql.toLowerCase();
        
        if (sql.equals("begin")) {
            // start of a transaction. save it, so we can restart from here next time.
            this.transactionStartPosition = header.getPosition();
        } else if (sql.startsWith("alter table") || sql.startsWith("rename table")) {
            System.out.println("A table structure changed. Stopping replication and exiting. To resume, re-bootstrap the stream.");
            System.exit(1);
        }
    }


    private void handleRotate(RotateEventData data) {
        binlogFilename = data.getBinlogFilename();
    }

    @Override
    public void onConnect(BinaryLogClient client) {
        this.binlogFilename = client.getBinlogFilename();
        this.binlogClient = client;
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
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

}
