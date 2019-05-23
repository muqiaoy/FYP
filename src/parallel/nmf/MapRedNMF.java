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

public class MapRedNMF {

	private final static int NUM_FEATURE = Config.NUM_FEATURE;
	private final static String NMF_FILE = Config.NMF_FILE;
	
	private static NMF nmf = new NMF(NUM_FEATURE,NMF_FILE);

	private static Matrix W = nmf.getW();
	private static Matrix H = nmf.getH();
//	private static Matrix WH = W.times(H);
//	private static Matrix Wt = W.transpose();
//	private static Matrix WtWH = Wt.times(WH);
//	private static Matrix Ht = H.transpose();
////	private static Matrix VHt = new Matrix(nmf.getArtist(), NUM_FEATURE);
//	private static Matrix WHHt = W.times(H).times(Ht);
	
	public static class NMFMapper extends
		Mapper<LongWritable, Text, Text, Text> {
//		private static IntWritable keyOut = new IntWritable(1); 

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] token = value.toString().split("\\s+|,");
			
			int userId = (int)Double.parseDouble(token[0]);
			int artistId = (int)Double.parseDouble(token[1]) - 1000000;
			double rating = Double.parseDouble(token[2]);

			
			
//			for(int i = 0;i<NUM_FEATURE;i++){
//				Text outputKey = new Text("W"+ "," + i + ","+ userId);
//				Text outputValue = new Text(rating + "," + artistId);
//				context.write(outputKey, outputValue);
//				outputKey.set("H"+ ","+ artistId + "," + i);
//				outputValue.set(rating + "," + userId);
//				context.write(outputKey, outputValue);
//			}
			
			for(int k = 0;k<NUM_FEATURE;k++){
				//key-value pairs for W update
				Text outputKey = new Text("W" + "," + artistId + "," + k);
				Text outputValue = new Text(userId + ","+ rating);
				context.write(outputKey, outputValue);
				
				//key-value pairs for H update
				outputKey = new Text("H" + "," + k + "," + userId);
				outputValue = new Text(artistId + "," + rating);
				context.write(outputKey, outputValue);
			}
			
			
		}
	}
		
	public static class NMFReducer extends
		Reducer<Text, Text, Text, Text> {

//		Matrix WtV = new Matrix(NUM_FEATURE, nmf.getUser());
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
//			Iterator<Text> iter = values.iterator();
			
//			IntermediateMatrices im = new IntermediateMatrices(nmf.getUser(), nmf.getArtist(), nmf.getW(), nmf.getH());
//			while (iter.hasNext()) {
//				IntermediateMatrices thisIm = iter.next();
//				im.accumulate(thisIm);
//			}
//			Text valueOut = new Text();
			
			String[] keyToken = key.toString().split(",");
			String[] valueToken;
//			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>(); //key: (featureId, userId)
//			HashMap<Integer, Double> hashB = new HashMap<Integer, Double>(); //key: (artistId, featureId)
//			for (Text val : values) {
//				valueToken = val.toString().split(",");
//				if (keyToken[0].equals("W")) {
//					hashA.put(Integer.parseInt(valueToken[1]), Double.parseDouble(valueToken[0])); //key-value pair: (artistId, rating)
//				} else {
//					hashB.put(Integer.parseInt(valueToken[1]), Double.parseDouble(valueToken[0])); //key-value pair: (userId, rating)
//				}
//			}
//			double result = 0.0;
//			if(keyToken[0].equals("W")){
//				int featureId = Integer.parseInt(keyToken[1]);
//				int userId = Integer.parseInt(keyToken[2]);
//				for(int i = 0;i<nmf.getArtist();i++){
//					result += Wt.get(featureId, i)*(hashA.containsKey(i)?hashA.get(i):0.0);
//				}
//				nmf.getH().set(featureId, userId, nmf.getH().get(featureId, userId)*result*WtWH.get(featureId, userId));
//			}
//			else {
//				int artistId = Integer.parseInt(keyToken[1]);
//				int featureId = Integer.parseInt(keyToken[2]);
//				for(int i = 0;i<nmf.getUser();i++){
//					result += (hashB.containsKey(i)?hashB.get(i):0.0)*Ht.get(artistId, featureId);
//				}
//				nmf.getW().set(artistId, featureId, nmf.getW().get(artistId, featureId)*result*WHHt.get(artistId, featureId));
//			}
			
			HashMap<Integer, Double> hashA = new HashMap<Integer, Double>();
			HashMap<Integer, Double> hashB = new HashMap<Integer, Double>();
			for (Text val : values) {
				valueToken = val.toString().split(",");
				if (keyToken[0].equals("W")) {
					hashA.put(Integer.parseInt(valueToken[0])-1, Double.parseDouble(valueToken[1])); //key-value pair: (userId, rating)
				} else {
					hashB.put(Integer.parseInt(valueToken[0])-1, Double.parseDouble(valueToken[1])); //key-value pair: (artistId, rating)
				}
			}			
			if(keyToken[0].equals("W")){
				double result = 0.0;
				int i = Integer.parseInt(keyToken[1])-1;
				int k = Integer.parseInt(keyToken[2]);
				for(int u =0;u<nmf.getUser();u++){
					result += (hashA.containsKey(u)?hashA.get(u):0)*nmf.getH().get(k, u);
				}
				result /= (nmf.getW().getMatrix(i,i,0,NUM_FEATURE-1).times(nmf.getH()).times(nmf.getH().getMatrix(k,k,0,nmf.getUser()-1).transpose())).get(0, 0);
				W.set(i, k, W.get(i,k)*result);
			}
			else {
				double result = 0.0;
				int k = Integer.parseInt(keyToken[1]);
				int j = Integer.parseInt(keyToken[2])-1;	
				for(int a =0;a<nmf.getArtist();a++){
					result += (hashB.containsKey(a)?hashB.get(a):0)*nmf.getW().get(a, k);
				}
				result /= (nmf.getW().getMatrix(0,nmf.getArtist()-1,k,k).transpose().times(nmf.getW()).times(nmf.getH().getMatrix(0,NUM_FEATURE-1,j,j))).get(0, 0);
				H.set(k, j, H.get(k,j)*result);			
			}
			

//			if(keyToken[0].equals("W")){
//				Matrix result = new Matrix(1, nmf.getUser());
//				int featureId = Integer.parseInt(keyToken[1]);
//				for(int i = 0;i<nmf.getUser();i++){
//					for(int j = 0;j<nmf.getArtist();j++){
//						result.set(0, i, result.get(0, i) + Wt.get(featureId, j)*(hashA.containsKey(j + "," + i)?hashA.get(j + "," + i):0.0));
//					}
//				}
//				nmf.getH().setMatrix(featureId, featureId, 0, nmf.getUser(), H.getMatrix(featureId, featureId, 0, nmf.getUser()).times(result).times(WtWH.getMatrix(featureId, featureId, 0, nmf.getUser())));
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
			
			Text outputValue = new Text("Finished");
			context.write(key, outputValue);
		}
		
		@Override
		public void cleanup(Context context){
			nmf.saveParameters(NMF_FILE);
		}
	}
	

	public static void main(String[] args) throws Exception {
//		BasicConfigurator.configure();

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "nmf");
		job.setJarByClass(MapRedNMF.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(NMFMapper.class);
//		job.setCombinerClass(NMFReducer.class);
		job.setReducerClass(NMFReducer.class);
//		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
