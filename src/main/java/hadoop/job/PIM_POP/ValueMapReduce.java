package hadoop.job.PIM_POP;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ValueMapReduce {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, 
                Context context) throws IOException, InterruptedException {
			Text outKey = new Text(key.toString());
			context.write(outKey, value);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		 protected void reduce(Text key, Iterable<Text> values, Context context
         ) throws IOException, InterruptedException {
			 System.out.println("being");
			 Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			 
			 if(cacheFiles != null && cacheFiles.length > 0){
				 String line;
				 String[] tokens = null;
				 
				 BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				 
				 try{
					 while ((line = joinReader.readLine()) != null) {
						 tokens = line.split("," ,2);
					}
				 }finally{
					 joinReader.close();
				 }
				 
				 for(Text value : values){
					 Text outValue = new Text();
					 List<String> outStrings = new ArrayList<String>(2);
					 
					 String wareSku = value.toString();
					 String[] wareSkuAttributes = wareSku.split("\t");
					 //获得1000000041:150000000^1000000041:150000000
					 String wareSkuAttribute = wareSkuAttributes[1];
					 for(String token : tokens){
						 String attributeId = token.split("\t")[1];
						 String type = token.split("\t")[3];
						 String attributeValue = token.split("\t")[2];
						 
						 //将1000000041:150000000^1000000041:150000000分隔
						 String[] attributes = wareSkuAttribute.split("^");
						 for(String attribute : attributes){
							 String[] attrValues = attribute.split(":");
							 if(attrValues[0].equals(attributeId)){
								if(type.indexOf("color:1") >= 0){
									//如果是color:1 放入outStrings第一个
									outStrings.add(1, attributeValue);
								}else{
									//如果是size:1 放入outStrings第二个
									outStrings.add(2, attributeValue);
								}
							 }
						 }
						 
						 outValue.set(outStrings.toString());
						 
					 }
				 }
			 }
			 
//			 String attributeCache = "150000000	1000000041	红色	color:1";
//			 for(Text value : values){
//				 
//			 }
		 }
	}
}
