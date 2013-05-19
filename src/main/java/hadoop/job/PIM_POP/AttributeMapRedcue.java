package hadoop.job.PIM_POP;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * vender_catagory_attribute???ware_catagory_attribute??????
 * @author huanghu
 *
 */
public class AttributeMapRedcue {
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, 
                Context context) throws IOException, InterruptedException {
			System.out.println("map value " + value);
			Text outKey = new Text(key.toString());
			context.write(outKey, value);
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		  protected void reduce(Text key, Iterable<Text> values, Context context
          ) throws IOException, InterruptedException {
			  List<java.util.Map<String ,String>> wareCategoryList = null;
			  try {
				  wareCategoryList = new ArrayList<java.util.Map<String ,String>>();
				  Class.forName("oracle.jdbc.OracleDriver");
				  Connection con = DriverManager.getConnection("jdbc:oracle:thin:@10.10.225.20:1521:testdb1","popware","popware");
				
				  Statement  sm = con.createStatement();
				  String sql = "select a.category_attribute_id as attributeId, a.features as features from popdir.WARE_CATEGORY_ATTRIBUTE a inner join  popware.VENDER_CATEGORY_VALUE b on a.category_attribute_id = b.attribute_id where a.features like 'size:1%' or a.features like 'color:1%' ";
				  ResultSet rs = sm.executeQuery(sql);
				
				  while(rs.next()){
					  java.util.Map<String, String> entitySql = new HashMap<String, String>();
					  String attributeId = rs.getString(1);
					  String features = rs.getString(2);
					  entitySql.put("attributeId", attributeId);
					  entitySql.put("features", features);
					  
					  wareCategoryList.add(entitySql);
				  }	
			  } catch (ClassNotFoundException e) {
				  e.printStackTrace();
			  } catch (SQLException e) {
				  e.printStackTrace();
			  }
			  
			  List<Text> alreadyOutText = new ArrayList<Text>();
			  for(Text value: values) {
				  String v = value.toString();
				  String attributeIdVender = v.split("\t")[1];
				  for(java.util.Map<String, String> wareCategory : wareCategoryList){
					  String attributeIdWare = wareCategory.get("attributeId");
					  
					  if(attributeIdVender.equals(attributeIdWare)){
						  Text outValue = new Text();
						  outValue.set(v + "	" + wareCategory.get("features"));
						  if(alreadyOutText.contains(outValue)){
							  //?????????
							  continue;
						  }else{
							  alreadyOutText.add(outValue);
							  context.write(new Text(attributeIdWare) ,outValue);  
						  }
						  
					  }
				  }
			  }
		  }
	}
}
