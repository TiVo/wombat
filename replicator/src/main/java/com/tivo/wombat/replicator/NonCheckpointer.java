/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

public class NonCheckpointer implements ICheckpointer {

    @Override
    public String getBinlogFilename() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setBinlogFilename(String binlogFilename) {
        // TODO Auto-generated method stub

    }

    @Override
    public long getBinlogPosition() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setBinlogPosition(long binlogPosition) {
        // TODO Auto-generated method stub

    }

    @Override
    public void save() {
        // TODO Auto-generated method stub

    }

}
