package parallel.nmf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.lang.Object;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import sequential.nmf.*;

public class MapRedNMF2 {

//	private final static int NUM_FEATURE = Config.NUM_FEATURE;
//	private final static String NMF_FILE = Config.NMF_FILE;
//	
//	private static NMF nmf = new NMF(NUM_FEATURE,NMF_FILE);
//
//	private static Matrix W = nmf.getW();
//	private static Matrix H = nmf.getH();
//	
//	public static class NMFMapper extends
//		Mapper<LongWritable, Text, Text, Text> {
//		int featureId = 0;
//
//		public void map(LongWritable key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String[] token = value.toString().split("\\s+|,");
//			
////			int userId = (int)Double.parseDouble(token[0]);
////			int artistId = (int)Double.parseDouble(token[1]) - 1000000;
////			double rating = Double.parseDouble(token[2]);
//
//			int l = token.length;
//			if(l==nmf.getArtist()){
//				for(int a = 0;a<l;a++){
//					for(int j = 0;j<nmf.getUser();j++){
//						//key-value pairs for H update
//						Text outputKey = new Text("H" + "," + featureId + "," + j); //(H,feature,user)
//						Text outputValue = new Text(a + "," + "W" + ","+ token[a]); //(artist,Wik)
//						context.write(outputKey, outputValue);
//
//						//key-value pairs for W update
//						outputKey = new Text("W" + "," + a + "," + featureId); //(W,artist,feature)
//						outputValue = new Text(a + "," + "W" + "," + token[a]); //(artist,Wik)
//						context.write(outputKey, outputValue);
//					}
//				}
//			}
//			else {
//				for(int u = 0;u<l;u++){
//					for(int i = 0;i<nmf.getArtist();i++){
//						//key-value pairs for W update
//						Text outputKey = new Text("H" + "," + featureId + "," + u); //(H,feature,user)
//						Text outputValue = new Text(u + "," + "H" + "," + token[u]); //(user,Hkj)
//						context.write(outputKey, outputValue);
//						outputKey = new Text("W" + "," + i + "," + featureId); //(W,artist,feature)
//						outputValue = new Text(u + "," + "H" + "," + token[u]); //(user,Hkj)
//						context.write(outputKey, outputValue);
//					}
//				}
//			}
//			featureId++;
//			if(featureId == NUM_FEATURE - 1) featureId = 0;
//			
//			
//		}
//	}
//		
//	public static class NMFReducer extends
//		Reducer<Text, Text, Text, Text> {
//
////		Matrix WtV = new Matrix(NUM_FEATURE, nmf.getUser());
//		
//		public void reduce(Text key, Iterable<Text> values,
//				Context context) throws IOException, InterruptedException {
////			Iterator<Text> iter = values.iterator();
//			
////			IntermediateMatrices im = new IntermediateMatrices(nmf.getUser(), nmf.getArtist(), nmf.getW(), nmf.getH());
////			while (iter.hasNext()) {
////				IntermediateMatrices thisIm = iter.next();
////				im.accumulate(thisIm);
////			}
////			Text valueOut = new Text();
//			
//			String[] keyToken = key.toString().split(",");
//			String[] valueToken;
////			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>(); //key: (featureId, userId)
////			HashMap<Integer, Double> hashB = new HashMap<Integer, Double>(); //key: (artistId, featureId)
////			for (Text val : values) {
////				valueToken = val.toString().split(",");
////				if (keyToken[0].equals("W")) {
////					hashA.put(Integer.parseInt(valueToken[1]), Double.parseDouble(valueToken[0])); //key-value pair: (artistId, rating)
////				} else {
////					hashB.put(Integer.parseInt(valueToken[1]), Double.parseDouble(valueToken[0])); //key-value pair: (userId, rating)
////				}
////			}
////			double result = 0.0;
////			if(keyToken[0].equals("W")){
////				int featureId = Integer.parseInt(keyToken[1]);
////				int userId = Integer.parseInt(keyToken[2]);
////				for(int i = 0;i<nmf.getArtist();i++){
////					result += Wt.get(featureId, i)*(hashA.containsKey(i)?hashA.get(i):0.0);
////				}
////				nmf.getH().set(featureId, userId, nmf.getH().get(featureId, userId)*result*WtWH.get(featureId, userId));
////			}
////			else {
////				int artistId = Integer.parseInt(keyToken[1]);
////				int featureId = Integer.parseInt(keyToken[2]);
////				for(int i = 0;i<nmf.getUser();i++){
////					result += (hashB.containsKey(i)?hashB.get(i):0.0)*Ht.get(artistId, featureId);
////				}
////				nmf.getW().set(artistId, featureId, nmf.getW().get(artistId, featureId)*result*WHHt.get(artistId, featureId));
////			}
//			
//			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>();
//			HashMap<Integer, Double> hashB = new HashMap<Integer, Double>();
//			for (Text val : values) {
//				valueToken = val.toString().split(",");
//				if (valueToken[1].equals("W")) {
//					hashA.put(Integer.parseInt(valueToken[0]), Double.parseDouble(valueToken[2])); //key-value pair: (artistId, rating)
//				} else {
//					hashB.put(Integer.parseInt(valueToken[0]), Double.parseDouble(valueToken[2])); //key-value pair: (userId, rating)
//				}
//			}
//			if(keyToken[0].equals("W")){
//				double product = 0.0;
//				Matrix d = new Matrix(1, NUM_FEATURE);
//				for(int a = 0;a<nmf.getArtist();a++){
//					product += hashA.containsKey(a)
//				}
////				int featureId = Integer.parseInt(keyToken[1]);
////				for(int i = 0;i<nmf.getUser();i++){
////					for(int j = 0;j<nmf.getArtist();j++){
////						result.set(0, i, result.get(0, i) + Wt.get(featureId, j)*(hashA.containsKey(j + "," + i)?hashA.get(j + "," + i):0.0));
////					}
////				}
////				nmf.getH().setMatrix(featureId, featureId, 0, nmf.getUser(), H.getMatrix(featureId, featureId, 0, nmf.getUser()).times(result).times(WtWH.getMatrix(featureId, featureId, 0, nmf.getUser())));
//			}
//			else {
//				Matrix result = new Matrix(nmf.getArtist(), 1);
//				int featureId = Integer.parseInt(keyToken[1]);
//				for(int i = 0;i<nmf.getArtist();i++){
//					for(int j = 0;j<nmf.getUser();j++){
//						result.set(i, 0, result.get(i, 0) + (hashB.containsKey(i + "," + j)?hashB.get(i + "," + j):0.0)*Ht.get(j, featureId));
//					}
//				}
//				nmf.getW().setMatrix(0, nmf.getArtist(), featureId, featureId, W.getMatrix(0, nmf.getArtist(), featureId, featureId).times(result).times(WHHt.getMatrix(0, nmf.getArtist(), featureId, featureId)));
//			}
//			
//			
//			nmf.saveParameters(NMF_FILE);
//			Text outputValue = new Text("Finished");
//			context.write(key, outputValue);
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
////		BasicConfigurator.configure();
//
//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf, "nmf");
//		job.setJarByClass(MapRedNMF2.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//
//		job.setMapperClass(NMFMapper.class);
////		job.setCombinerClass(NMFReducer.class);
//		job.setReducerClass(NMFReducer.class);
////		job.setNumReduceTasks(1);
//
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
//
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//		job.waitForCompletion(true);
//	}
}
