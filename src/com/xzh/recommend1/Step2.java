package com.xzh.recommend1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * 按用户分组，计算所有物品出现的组合列表，得到用户对物品的喜爱度得分矩阵
 * 
 * itemid  userid   event   time
 * i161	   u2625	click	2014/9/18 15:03
 * 
u13	i160:1,
u14	i25:1,i223:1,
u16	i252:1,
u21	i266:1,
u24	i64:1,i218:1,i185:1,
u26	i276:1,i201:1,i348:1,i321:1,i136:1,
 * @author root
 *
 */
public class Step2 {

	
	public static boolean run(Configuration config,Map<String, String> paths){
		try {
//			config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
			FileSystem fs =FileSystem.get(config);
			Job job =Job.getInstance(config);
			job.setJobName("step2");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);
//			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			

			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outpath=new Path(paths.get("Step2Output"));
			if(fs.exists(outpath)){
				fs.delete(outpath,true);
			}
			FileOutputFormat.setOutputPath(job,outpath);
			
			boolean f= job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	 static class Step2_Mapper extends Mapper<LongWritable,Text,Text,Text>{

		 //如果使用：用户+物品，同时作为输出key,更优
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			//.csv文件默认按照,分割
			String[]  tokens=value.toString().split(",");
			String item=tokens[0];  //物品
			String user=tokens[1];  //用户
			String action =tokens[2];  //行为
			Text k= new Text(user);
			Integer rv =StartRun.R.get(action);
//			if(rv!=null){
			Text v =new Text(item+":"+ rv.intValue());
			//用户id作为key       item:action作为value   (用户行为转换为数字,便于后边构造矩阵)
			context.write(k, v);
		}
	}
	
	 
	 static class Step2_Reducer extends Reducer<Text, Text, Text, Text>{

			protected void reduce(Text key, Iterable<Text> i,Context context) throws IOException, InterruptedException {
				
				Map<String,Integer> r =new HashMap<String,Integer>();
				//用户id作为key       item:action作为value 
				for(Text value:i){
					String[] vs = value.toString().split(":");
					String item= vs[0];  //物品
					Integer action=Integer.parseInt(vs[1]);  //行为
					//如果r为空,则action为0,否则获取item
					//每次都使得原先同一个item所对应的action相加  此处为同一个用户对应不同的item  
					action = ((Integer) (r.get(item)==null?0:r.get(item))).intValue() + action;
					//key:item  value:item+action
					r.put(item,action);
				}
				StringBuffer sb =new StringBuffer();
				
				for(Entry<String,Integer> entry :r.entrySet() ){
					//相邻两个kv,按照","隔开,一个kv,按照:隔开。
					sb.append(entry.getKey()+":"+entry.getValue().intValue()+",");
				}
			    
				context.write(key,new Text(sb.toString()));
			}
		}
}
