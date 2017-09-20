package com.xzh.recommend1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

//import com.sun.org.apache.xpath.internal.operations.Mult;

/**
 * 
 * 把同现矩阵和得分矩阵相乘
 * @author root
 *
 */
public class Step4 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step4");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			//FileInputFormat.addInputPath(job, new
			//Path(paths.get("Step4Input")));
			FileInputFormat.setInputPaths(job,new Path[] { new Path(paths.get("Step4Input1")),
						new Path(paths.get("Step4Input2")) });
			Path outpath = new Path(paths.get("Step4Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private String flag;// A同现矩阵 or B得分矩阵

		//每个maptask，初始化时调用一次
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// 判断读的数据集

			System.out.println(flag + "**********************");
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			//Step3 的reduce输出        K为不同物品   itemA:itemB      V为对应的计数器之和
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());

			if (flag.equals("step3")) {   //同现矩阵
				String[] v1 = tokens[0].split(":");
				String itemID1 = v1[0];  //一个物品
				String itemID2 = v1[1];  //另外一个物品
				String num = tokens[1];  //计数器总和

				Text k = new Text(itemID1);   //以前一个物品为key 比如i100
				Text v = new Text("A:" + itemID2 + "," + num);  // A:i109,1
				//i100 A:i109,1
				context.write(k,v);
			} else if (flag.equals("step2")) {    //用户对物品喜爱得分矩阵
				//u26	i276:1,i201:1,i348:1,i321:1,i136:1,
				String userID = tokens[0];  //用户Id
				for (int i = 1; i < tokens.length; i++) {
					String[] vector = tokens[i].split(":");
					String itemID = vector[0];// 物品id
					String pref = vector[1];   // 喜爱分数

					Text k = new Text(itemID); // 以物品为key 比如：i100
					Text v = new Text("B:" + userID + "," + pref); // B:u401,2
					//i100 B:u401,2
					context.write(k,v);
				}
			}
		}
	}

	static class Step4_Reducer extends Reducer<Text,Text,Text,Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// A同现矩阵 or B得分矩阵
			//某一个物品，针对它和其他所有物品的同现次数，都在mapA集合中
			Map<String, Integer> mapA = new HashMap<String, Integer>();// 和该物品（key中的itemID）同现的其他物品的同现集合// 。其他物品ID为map的key，同现数字为值
			Map<String, Integer> mapB = new HashMap<String, Integer>();// 该物品（key中的itemID），所有用户的推荐权重分数。
        
			for (Text line : values) {
				String val = line.toString();
				//i100 A:i109,1
				if (val.startsWith("A:")) {    //表示物品同现数字
					String[] kv = Pattern.compile("[\t,]").split(val.substring(2));     //同显矩阵计数器之和  
					try {
						//key:物品id(i109)   value:1    计数器
						mapA.put(kv[0],Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}

				} else if (val.startsWith("B:")) {
					//i100 B:u401,2
					String[] kv = Pattern.compile("[\t,]").split(
							val.substring(2));
					try {
						//u401       2(评分)
						mapB.put(kv[0],Integer.parseInt(kv[1]));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			double result = 0;
			Iterator<String> iter = mapA.keySet().iterator();
			while (iter.hasNext()) {
				String mapk = iter.next();// itemID

				int num = mapA.get(mapk).intValue();
				Iterator<String> iterb = mapB.keySet().iterator();
				while (iterb.hasNext()) {
					String mapkb = iterb.next();// userID
					int pref = mapB.get(mapkb).intValue();
					
					result = num * pref;    //同现矩阵与用户对物品喜爱得分矩阵乘法相乘计算
					
					Text k = new Text(mapkb);
					Text v = new Text(mapk + "," + result);
					//userid   itemid,最后得分
					context.write(k, v);
				}
			}
		}
	}
}
