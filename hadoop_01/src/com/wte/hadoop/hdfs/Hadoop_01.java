package com.wte.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Hadoop_01 {
	Configuration conf=null;
	FileSystem fs=null;
	@Before
	public void conn() throws Exception{
		conf=new Configuration();
		fs=FileSystem.get(conf);
	}
	@After
	public void close() throws Exception{
		fs.close();
	}
	@Test
	public void mkdir() throws Exception{
		Path path=new Path("/data");
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		fs.mkdirs(path);			
		}
	@Test
	public void upload() throws Exception{
		InputStream input =new BufferedInputStream(new FileInputStream(new File("c:\\nginx")));
		Path path=new Path("/data/xxoo.txt");
		FSDataOutputStream output=fs.create(path);
		IOUtils.copyBytes(input, output, conf, true);
		
	}
	@Test
	public void delete() throws Exception{
		Path path=new Path("/user/root/");
		if(fs.exists(path)){
			fs.delete(path,true);
		}	
	}
	@Test
	public void blks() throws IOException{
		Path f=new Path("/user/root/text.txt");
		FileStatus file=fs.getFileStatus(f);
		BlockLocation[] blks = fs.getFileBlockLocations(file, 0, file.getLen());
		for(BlockLocation block:blks){
			System.out.println(block);
		}
		FSDataInputStream in = fs.open(f);
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		in.seek(1048576);
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
		System.out.println((char)in.readByte());
	}
}
