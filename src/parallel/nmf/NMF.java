package parallel.nmf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import sequential.nmf.Matrix;


public class NMF {
	
	private int nFeature;
	private int nUser;
	private int nArtist;
//	private Matrix status;
	private Matrix W;
	private Matrix H;
	
	public NMF(){
		nFeature = 0;
	}
	
	public NMF(int num, String nmfFile){
		this.nFeature = num;
		this.nUser = 0;
		this.nArtist = 0;
		try {
			loadParameters(nmfFile);
		} catch (IOException e) {
			System.out.println("File " + nmfFile + " not found");
		}
		
//		Scanner scanner = null;
//		try {
//			scanner = new Scanner(new File(nmfFile));
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		int i = 0;
//		String line;
		
		Path pt = new Path(nmfFile);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String line;
		try {
			line = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int i = 0;
		try {
			while((line=br.readLine())!=null && line.length()!=0){
//		while((line = scanner.nextLine())!=null && line.length()!=0){
				String[] token = line.split("\\s+|,");
				if(i == 0) {
					nArtist = token.length;
					this.W = new Matrix(nArtist, nFeature);
				}
				else if(i == nFeature) {
					nUser = token.length;
					this.H = new Matrix(nFeature, nUser);
				}
				i++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void loadParameters(String nmfFile) throws IOException {
//		Scanner scanner = null;
//		try {
//			scanner = new Scanner(new File(nmfFile));
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		int i = 0;
//		String line;
//		while((line = scanner.nextLine())!=null && line.length()!=0){
//			String[] token = line.split(" ");
//			if(i < nFeature){
//				for(int j = 0;j<nArtist;j++){
//					W.set(j, i, Double.parseDouble(token[j]));
//				}
//			}
//			else if(i>=nFeature && i<nFeature*2){
//				for(int j = 0;j<nUser;j++){
//					H.set(i-nFeature, j, Double.parseDouble(token[j]));
//				}
//			}
//			else break;
//			i++;
//		}
		
		Path pt = new Path(nmfFile);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line = br.readLine();
		int i = 0;
		while((line=br.readLine())!=null && line.length()!=0){
			String[] token = line.split(" ");
			if(i < nFeature){
				for(int j = 0;j<nArtist;j++){
					W.set(j, i, Double.parseDouble(token[j]));
				}
			}
			else if(i>=nFeature && i<nFeature*2){
				for(int j = 0;j<nUser;j++){
					H.set(i-nFeature, j, Double.parseDouble(token[j]));
				}
			}
			else break;
			i++;
		}
		
		
		//reference
//		Path pt = new Path(nmfFile);
//		FileSystem fs;
//		fs = FileSystem.get(new Configuration());
//		BufferedReader br = new BufferedReader(new InputStreamReader(
//				fs.open(pt)));
//		String line = br.readLine();
//		String[] token = line.split(" ");
//		for (int i = 0; i < nArtist; i++) {
//			line = br.readLine();
//			token = line.split(" ");
//			for (int j = 0; j < nUser; j++) {
//				status.set(i, j, Double.parseDouble(token[j]));
//			}
//		}
//		for (int i = 0; i < nArtist; i++) {
//			line = br.readLine();
//			token = line.split(" ");
//			for (int j = 0; j < nUser; j++) {
//				V.set(i, j, Double.parseDouble(token[j]));
//			}
//		}
//		br.close();
	}
	public void saveParameters(String nmfFile) {
		try {
			PrintWriter out = new PrintWriter(new File(nmfFile));
			try {
				out.println(this.toString());
			} finally {
				out.close();
			}
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
	}
	

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Matrix Wt = W.transpose();
		for (int i=0; i<Wt.getRowDimension(); i++) {
			for (int j=0; j<Wt.getColumnDimension(); j++) {
				sb.append(String.format("%.5f ", Wt.get(i, j)));
			}
			sb.append("\n");
		}
		for (int i=0; i<H.getRowDimension(); i++) {
			for (int j=0; j<H.getColumnDimension(); j++) {
				sb.append(String.format("%.5f ", H.get(i, j)));
			}
			sb.append("\n");
		}
		return(sb.toString());
	}
	
//	public void train(IntermediateMatrices im){
//		
//		//update feature matrix
//		Matrix WtV = im.getWtV();
//		Matrix WtWH = im.getWtWH();
//		H.arrayTimesEquals(WtV.arrayTimes(WtWH.dotInverseEquals()));
//		
//		//update weight matrix
//		Matrix VHt = im.getVHt();
//		Matrix WHHt = im.getWHHt();
//		W.arrayTimesEquals(VHt.arrayTimes(WHHt.dotInverseEquals()));
//	}	

	public Matrix getW(){
		return W;
	}
	
	public Matrix getH(){
		return H;
	}
	
	public int getUser(){
		return nUser;
	}
	
	public int getArtist(){
		return nArtist;
	}

	public void setW(Matrix W){
		this.W = W;
	}
	public void setH(Matrix H){
		this.H = H;
	}
	
//	public static void main(String[] args) throws IOException {
//		String nmfFile = args[0];
//		NMF nmf = new NMF(100, nmfFile);
//		
//		System.out.println("hello");
//	}
	
}