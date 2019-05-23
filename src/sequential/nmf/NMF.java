package sequential.nmf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;



public class NMF {
	
	private final int nFeature;
	private Matrix V;
//	private Matrix status;
	private Matrix W;
	private Matrix H;
	
	public NMF(int nFeature, String DataFile){
		this.nFeature = nFeature;
		this.loadData(DataFile);
		this.W = Matrix.random(V.getRowDimension(), this.nFeature);
		this.H = Matrix.random(this.nFeature, V.getColumnDimension());
	}
	
	public void loadData(String datafile) {
		Scanner scanner0 = null;
		try {
			scanner0 = new Scanner(new File(datafile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		int nUser = 0;
		while(scanner0.hasNextLine()){
			String line = scanner0.nextLine();
			String[] token = line.split("\\s+|,");		// Either space or ',' as delimiter
			nUser = Math.max(nUser, (int)Double.parseDouble(token[0]));
		}
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(datafile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		ArrayList<double[]> ratingList = new ArrayList<double[]>();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String[] token = line.split("\\s+|,");		// Either spacee or ',' as delimiter
			int userId = (int)Double.parseDouble(token[0]);
			int artistId = (int)Double.parseDouble(token[1]) - 1000000;
			double rating = Double.parseDouble(token[2]);
			
			while (ratingList.size() < artistId) {
				double x[] = new double[nUser];
				ratingList.add(x);
			}
			
			double[] newRating = ratingList.get(artistId - 1);
			newRating[userId - 1] = rating;
			ratingList.set(artistId - 1, newRating);
		}
		double[][] ratingData = new double[ratingList.size()][];
		for (int t=0; t<ratingList.size(); t++) {
			ratingData[t] = ratingList.get(t);
		}
		this.V = new Matrix(ratingData);
//		return new Matrix[]{ratingMatrix, statusMatrix};
	}
	
	public void train(int iter){
//		long startTime = System.currentTimeMillis();
//		long currentTime;
		
//		Matrix Y = H;
//		double b = 1;
//		Matrix X = W;
//		double a = 1;
//		Matrix S2 = Matrix.ones(nFeature, nFeature).times(smooth/nFeature);
//		Matrix S = Matrix.identity(nFeature, nFeature).times(1-smooth).plus(S2);
		for (int i = 0;i<iter;i++){
			Matrix WH = W.times(H);
//			double cost = calCost(V, WH);
			
			//update feature matrix
			Matrix Wt = W.transpose();
			Matrix WtV = Wt.times(V);
			Matrix WtWH = Wt.times(WH);
			H.arrayTimesEquals(WtV.arrayTimes(WtWH.dotInverseEquals()));
			
			//update weight matrix
			Matrix Ht = H.transpose();
			Matrix VHt = V.times(Ht);
			Matrix WHHt = W.times(H).times(Ht);
			W.arrayTimesEquals(VHt.arrayTimes(WHHt.dotInverseEquals()));
			
//			WtV.setMatrix(0, nFeature-1, userId-1, userId-1, 
//					WtV.getMatrix(0, nFeature-1, userId-1, userId-1).plus(W.getMatrix(artistId-1, artistId-1, 0, nFeature-1).transpose().times(rating)));

//			Matrix tempH = H;
//			double tempb = b;
//			Matrix tempW = W;
//			double tempa = a;
//			double L = S.transpose().times(tempW.transpose()).times(tempW).times(S).norm2();
//			Matrix StWtWSH = S.transpose().times(tempW.transpose()).times(tempW).times(S).times(tempH);
//			Matrix StWtV = S.transpose().times(tempW.transpose()).times(V);
//			H = posi(Y.minus((StWtWSH.minus(StWtV)).times(1/L)));
//			b = (1+Math.pow(4*Math.pow(tempb, 2)+1, 0.5))/2;
//			Y = H.plus(H.minus(tempH).times((tempb-1)/b));
//
//			double N = S.times(tempH).times(tempH.transpose()).times(S.transpose()).norm2();
//			Matrix WSHHtSt = tempW.times(S).times(tempH).times(tempH.transpose()).times(S.transpose());
//			Matrix VHtSt = V.times(tempH.transpose()).times(S.transpose());
//			W = posi(X.minus((WSHHtSt.minus(VHtSt)).times(1/N)));
//			a = (1+Math.pow(4*Math.pow(tempa, 2)+1, 0.5))/2;
//			X = W.plus(W.minus(tempW).times((tempa-1)/a));
			
			
			
//			for(int l = 0;l<H.getRowDimension();l++){
//				for(int j = 0;j<H.getColumnDimension();j++){
//					double Wl = check(W,H,l,j);
//					if(Wl == 0);
//					else H.set(l, j, Math.max(0, H.get(l, j) - 1/Wl));
//				}
//			}
			
//			currentTime = System.currentTimeMillis();
//			System.out.println(cost + " " + (currentTime-startTime)/1000.0);
		}
//		return new Matrix[]{W,H};
	}
	
	
//	public double calCost(Matrix A, Matrix B){
//		if(A.getRowDimension()!=B.getRowDimension() || A.getColumnDimension()!=B.getColumnDimension())
//            throw new RuntimeException("Illegal matrix dimensions.");
//		double cost = 0;
//		double vF = 0;
//		for(int i = 0; i<A.getRowDimension();i++){
//			for(int j = 0;j<A.getColumnDimension();j++){
//				if(status.get(i,j) != 0)
//					cost += Math.pow(A.get(i, j) - B.get(i, j), 2);
//					vF += Math.pow(A.get(i, j), 2);
//			}
//		}
//		
//		return cost/vF;
//	}
	
	public Matrix posi(Matrix A){
		for(int i = 0;i<A.getRowDimension();i++)
			for(int j = 0;j<A.getColumnDimension();j++)
				if(A.get(i, j)<0) A.set(i, j, 0);
		return A;
	}

	public double check(Matrix A, Matrix B, int l, int j){
		if(B.get(l, j)!=0) return 0;
		double magn = 0;
		Matrix zeros = A.getMatrix(0, A.getRowDimension(), l, l);
		for(int i = 0;i<A.getRowDimension();i++){
			if(zeros.get(i, 1)!=0) return 0;
			else magn += Math.pow(zeros.get(i, 1), 2);
		}
		return magn;
	}
	
	public double check(int i, int k, Matrix A, Matrix B){
		if(A.get(i, k)!=0) return 0;
		double magn = 0;
		Matrix zeros = A.getMatrix(k, k, 0, B.getColumnDimension());
		for(int j = 0;j<B.getColumnDimension();j++){
			if(zeros.get(1, j)!=0) return 0;
			else magn += Math.pow(zeros.get(1, j), 2);
		}
		return magn;
	}
	

	/*
	 * Save NMF parameters to file
	 */
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
//		for (int i=0; i<status.getRowDimension(); i++) {
//			for (int j=0; j<status.getColumnDimension(); j++) {
//				sb.append(String.format("%.5f ", status.get(i, j)));
//			}
//			sb.append("\n");
//		}
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
	
//	public static void main(String[] args) {
//		String dataFile = args[0];
//		NMF nmf = new NMF();
//		nmf.loadData(dataFile);
//		
//		//first parameter means the number of hidden features, second means number of iterations, third means smooth parameter if using nsNMF
//		nmf.train(100,100,0.2);
//		
////		nmf.V = new Matrix(new double[][]{
////            {22,28},
////            {49,64}
////        });
////		nmf.status = new Matrix(new double[][]{
////            {1,1},
////            {1,1}
////        });
////		nmf.train(3, 100);
//		
//	}
	

	public static void main(String[] args) {
		if (args.length < 3 || args.length > 4) {
			System.out.println("Usage: java sequential.nmf.NMF <No. of nFeature dimension> <No. of iters> <data file> [output file]");
			System.out.println("Example: java sequential.nmf.NMF 100 100 ../matlab/2D_data.txt ../matlab/nmf.txt");
			return;
		}
		int nFeature = Integer.parseInt(args[0]);
		int nIters = Integer.parseInt(args[1]);
		String dataFile = args[2];
		NMF nmf = new NMF(nFeature, dataFile);
		nmf.train(nIters);
		if (args.length == 4) {
			String paraFile = args[3];
			System.out.println("Saving parameter file " + paraFile);
			nmf.saveParameters(paraFile);
		}
	}
	
}