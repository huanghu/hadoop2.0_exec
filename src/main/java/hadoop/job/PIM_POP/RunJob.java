package hadoop.job.PIM_POP;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://namenode-host.company.com:8020/");
		conf.set("mapred.job.tracker", "jobtracker-host.company.com:8021");
		conf.set("hadoop.job.user","root");
		
		DistributedCache.addCacheFile(new Path("/user/huanghu/input/attribute/distribute.txt").toUri(), conf);
		
	    Job job = new Job(conf, "Attribute");
	    job.setJarByClass(ValueMapReduce.class);
		((JobConf)job.getConfiguration()).setJar("/Users/huanghu/Documents/program/hadoop/hadoop2.0_exec/target/hadoop2.0_exec-0.0.1-SNAPSHOT.jar");
	    job.setMapperClass(ValueMapReduce.Map.class);
	    job.setReducerClass(ValueMapReduce.Reduce.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path("/user/huanghu/input/attribute"));
	    FileOutputFormat.setOutputPath(job, new Path("/user/huanghu/out/attribute"));
	    
		checkFile(conf, "/user/huanghu/out/attribute");
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	private static void checkFile(Configuration conf ,String pathStr){
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(pathStr);
			if(fs.exists(path)){
				fs.delete(path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
