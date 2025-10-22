package edu.ismagi.hadoop.hdfslab;

import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.Block;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: hadoop jar /shared_volume/HadoopFileStatus.jar <FILE_PATH> <NEW_FILE_NAME>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(args[0]);
            FileStatus status = fs.getFileStatus(path);
            if (!fs.exists(path)) {
                System.out.println("File does not exist");
                System.exit(1);
            }
            System.out.println(Long.toString(status.getLen()) + " bytes");
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen());
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission().toString());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());
            BlockLocation[] blkLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blkLocation : blkLocations) {
                String[] hosts = blkLocation.getHosts();
                System.out.println("Block Offset: " + blkLocation.getOffset());
                System.out.println("Block Length: " + blkLocation.getLength());
                System.out.print("Block Hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            // Rename file
            String newFilePath = path.getParent().toString() + Path.SEPARATOR + args[1];
            fs.rename(path, new Path(newFilePath));
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }
}
