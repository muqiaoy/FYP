package parallel.nmf;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Scanner;

import sequential.nmf.Matrix;

public class Recommend {
	
	public static void recommend(int userId, String dataFile, String nameFile){
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(dataFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int nFeature = 100;
		double[][] wt = new double[nFeature][];
		double[] hj = new double[nFeature];
		int k = 0;
		String line;
		while((line = scanner.nextLine())!=null && line.length()!=0){
			String[] token = line.split("\\s+|,");
			if(k < nFeature){
				wt[k] = new double[token.length];
				for(int i = 0;i<token.length;i++){
					wt[k][i] = Double.parseDouble(token[i]);
				}
			}
			else if(k>=nFeature && k<nFeature*2){
				for(int j = 0;j<line.length();j++){
					if(j==userId)
						hj[k-nFeature] = Double.parseDouble(token[j]);
				}
			}
			else break;
			k++;
		}
		Matrix Wt = new Matrix(wt);
		Matrix Hj = new Matrix(hj,nFeature);
		Matrix W = Wt.transpose();
		int nArtist = W.getRowDimension();
		Matrix rating = W.times(Hj);
		W = null;
		Hj = null;
		Wt = null;
		double maxRating = 0;
		int maxArtist = -1;
		for(int i = 0;i<nArtist;i++){
			if(maxRating < rating.get(i, 0)){
				maxRating = rating.get(i, 0);
				maxArtist = i;
			}
		}
		
		scanner = null;
		try {
			scanner = new Scanner(new File(nameFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		while(scanner.hasNextLine()){
			line = scanner.nextLine();
			String[] token = line.split("\\s+|,");
			if(maxArtist+1000001 == Integer.parseInt(token[0])) {
				String[] output = new String[token.length-1];
				for(int l = 0;l<token.length-1;l++){
					output[l] = token[l+1];
				}
			}
		}
		System.out.println("The most recommended artist is " + maxArtist);
	}
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: java parallel.nmf.Recommend <userID> <data file> <name file>");
			System.out.println("Example: java parallel.nmf.Recommend 10 ../matlab/nmf.txt ../matlab/name.txt");
			return;
		}
		int userId = Integer.parseInt(args[0]);
		String dataFile = args[1];
		String nameFile = args[2];
		recommend(userId, dataFile, nameFile);
	}
}
