package cn.zqx.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class TestDemo {
	private FileSystem fs=null;
	@Before
	public void init() throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		//FileSystem是hadoop文件系统的抽象类，HDFS分布式文件系统只是Hadoop文件系统中的一种，对应实现类
		//org.apache.hadoop.hdfs.DistributedFileSystem.HDFS是最常用的一种
		fs = FileSystem.get(new URI("hdfs://176.19.1.38:9000"), conf);
	}
	@Test
	public void testConnect() throws Exception{
		Configuration conf = new Configuration();
		//FileSystem是hadoop文件系统的抽象类，HDFS分布式文件系统只是Hadoop文件系统中的一种，对应实现类
		//org.apache.hadoop.hdfs.DistributedFileSystem.HDFS是最常用的一种
		FileSystem fs = FileSystem.get(new URI("hdfs://176.19.1.38:9000"), conf);
		fs.close();
	}
	
	@Test
	public void testmkdir() throws IOException, URISyntaxException{
		fs.mkdirs(new Path("/park02"));	
		fs.close();
	}
	@Test
	public void testCopyToLocal() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(false,new Path("/park01/1.txt"), new Path("hello.txt"),true);
		fs.close();
	}
	@Test
	public void testCopyFromLocal() throws Exception{
		fs.copyFromLocalFile(false, new Path("hello.txt"), new Path("/park01"));
		fs.close();
	}
	@Test
	public void testDelete() throws Exception{
		fs.delete(new Path("/park02"),true);
		fs.close();
	}
	@Test
	public void teststatus() throws Exception{
		FileStatus[] status = fs.listStatus(new Path("/park01"));
		for(FileStatus f:status){
			System.out.println(f);
		}
		fs.close();
	}

}
