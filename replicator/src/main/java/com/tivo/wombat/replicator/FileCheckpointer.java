/*
 * Copyright 2015 TiVo Inc.  All rights reserved.
 */

package com.tivo.wombat.replicator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class FileCheckpointer implements ICheckpointer {

    private String checkpointFilename;
    private String binlogFilename;
    private long binlogPosition;
    
    /**
     * @param checkpointFilename
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    public FileCheckpointer(String checkpointFilename) throws FileNotFoundException, IOException {
        this.checkpointFilename = checkpointFilename;
        load();
    }

    private void load() throws FileNotFoundException, IOException {
        Properties checkpoint = new Properties();
        
        try (InputStream input = new FileInputStream(checkpointFilename)) {
            checkpoint.load(input);

            String binlogFilename = checkpoint.getProperty("BINLOG_FILENAME");  
            String binlogPositionStr = checkpoint.getProperty("BINLOG_POSITION");       
            
            if (binlogFilename != null && binlogPositionStr != null) {
                this.binlogFilename = binlogFilename;
                this.binlogPosition = Long.parseLong(binlogPositionStr);
                return;
            }
        } catch (FileNotFoundException e) {
        }
        System.out.println("No existing checkpoint. Will connect to the end of the MySQL binlog (i.e. at \"now\").");
        
        // This is the value com.github.shyiko.mysql.binlog.BinaryLogClient uses to determine that it should connect to the end of the binlog stream.
        this.binlogFilename = null;
        this.binlogPosition = 0;
    }
    
    /* (non-Javadoc)
     * @see com.tivo.wombat.replicator.ICheckpointer#getBinlogFilename()
     */
    @Override
    public String getBinlogFilename() {
        return binlogFilename;
    }

    /* (non-Javadoc)
     * @see com.tivo.wombat.replicator.ICheckpointer#setBinlogFilename(java.lang.String)
     */
    @Override
    public void setBinlogFilename(String binlogFilename) {
        this.binlogFilename = binlogFilename;
    }

    /* (non-Javadoc)
     * @see com.tivo.wombat.replicator.ICheckpointer#getBinlogPosition()
     */
    @Override
    public long getBinlogPosition() {
        return binlogPosition;
    }

    /* (non-Javadoc)
     * @see com.tivo.wombat.replicator.ICheckpointer#setBinlogPosition(long)
     */
    @Override
    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    /* (non-Javadoc)
     * @see com.tivo.wombat.replicator.ICheckpointer#save()
     */
    @Override
    public void save() {
        // XXX when saving to Kafka, primary key should be 
        // (GroupId, MySQLServerIdentifier).
        // GroupId is something like a Kafka consumer group.
        // This would let me have multiple Wombat instances all listening to same MySQL server but each have different offsets.
        // Should MySQLServerIdentifier be IP address? hostname? Something else? Dunno.
        
        try {
            File checkpointFile = new File(this.checkpointFilename);
            File directory = checkpointFile.getParentFile();

            File file = File.createTempFile("checkpoint", "tmp", directory);
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(String.format("BINLOG_FILENAME=%s\n", this.binlogFilename));
            bw.write(String.format("BINLOG_POSITION=%d\n", this.binlogPosition));
            bw.close();
            Files.move(file.toPath(), checkpointFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("ERROR to stdout! " + e);
        }
    }
}
