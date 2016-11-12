/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

public interface ICheckpointer {

    public abstract String getBinlogFilename();

    public abstract void setBinlogFilename(String binlogFilename);

    public abstract long getBinlogPosition();

    public abstract void setBinlogPosition(long binlogPosition);

    public abstract void save();

}
