/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lurie.mapreduce;

import java.io.IOException;

/**
 *
 * @author lurie
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class RemoveDup {

	public static class RemoveDupMapper extends Mapper<Object, Text, Text, NullWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
			// System.out.println("map: key=" + key + ",value=" + value);
		}

	}

	public static class RemoveDupReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
			// System.out.println("reduce: key=" + key);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: RemoveDup <in> [<in>...] <out>");
			System.exit(2);
		}

		// 删除输出目录(可选,省得多次运行时,总是报OUTPUT目录已存在)
		// HDFSUtil.deleteFile(conf, otherArgs[otherArgs.length - 1]);

		Job job = Job.getInstance(conf, "RemoveDup");
		job.setJarByClass(RemoveDup.class);
		job.setMapperClass(RemoveDupMapper.class);
		job.setCombinerClass(RemoveDupReducer.class);
		job.setReducerClass(RemoveDupReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
