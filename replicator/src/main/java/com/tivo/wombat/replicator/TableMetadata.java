/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.util.ArrayList;
import java.util.List;

public class TableMetadata {
    public List<String> primaryKeys;
    public List<Integer> primaryKeyIndex;
    public ArrayList<String> columnNames;
    
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    /**
     * When primaryKeys is set, we will automatically calculate the primaryKeyIndex
     * 
     * @param columnNames
     */
    public void setPrimaryKeys(List<String> primaryKeyNames) {
        this.primaryKeys = primaryKeyNames;
        List<Integer> primaryKeyIndices = new ArrayList<Integer>();
        for (String columnName : this.primaryKeys) {
            primaryKeyIndices.add(this.columnNames.indexOf(columnName));
        }
        this.primaryKeyIndex = primaryKeyIndices;
    }

    public List<Integer> getPrimaryKeyIndices() {
        return primaryKeyIndex;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        this.columnNames = columnNames;
    }
    
}
