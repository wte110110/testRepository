package com.wte.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWC {
	public static void main(String[] args) throws Exception {
		//读取配置文件所有信息
		Configuration conf=new Configuration(true);
		//MRJobConfig
		Job job=Job.getInstance(conf);
	    job.setJarByClass(MyWC.class);   
	    job.setJobName("wte-wc");	     
		//	    job.setInputPath(new Path("in"));
//	    job.setOutputPath(new Path("out"));
	    Path path=new Path("/user/root/text.txt");
	    FileInputFormat.addInputPath(job, path);
	    Path output=new Path("data/WC/output02");
	    if(output.getFileSystem(conf).exists(output)){
	    	output.getFileSystem(conf).delete(output,true);
	    }
		FileOutputFormat.setOutputPath(job, output);
	    job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);

	    // Submit the job, then poll for progress until the job is complete
	    job.waitForCompletion(true);
	}
}
