package qsc.hyper;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

public class MatrixUtilPlus {
    public static RealMatrix tile(RealMatrix matrix, int dim, int colTimes) {

        if (dim != 0 && dim != 1) {
            throw new IllegalArgumentException("Dimension must be 1 or 2.");
        }

        if (dim == 0) {

            RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension(), matrix.getColumnDimension() * colTimes);

            for (int i = 0; i < matrix.getRowDimension(); i++) {
                for (int j = 0; j < colTimes; j++) {
                    for (int k = 0; k < matrix.getColumnDimension(); k++) {
                        result.setEntry(i, j * matrix.getColumnDimension() + k, matrix.getEntry(i, k));
                    }
                }
            }

            return result;
        }

        else {

            RealMatrix result = new Array2DRowRealMatrix(matrix.getRowDimension() * colTimes, matrix.getColumnDimension());

            for (int i = 0; i < colTimes; i++) {
                for (int j = 0; j < matrix.getRowDimension(); j++) {
                    for (int k = 0; k < matrix.getColumnDimension(); k++) {
                        result.setEntry(i * matrix.getRowDimension() + j, k, matrix.getEntry(j, k));
                    }
                }
            }

            return result;
        }
    }

    public static RealMatrix ebeMultiply(RealMatrix matrixA, RealMatrix matrixB) {

        if (matrixA.getRowDimension() != matrixB.getRowDimension() || matrixA.getColumnDimension() != matrixB.getColumnDimension()) {
            throw new IllegalArgumentException("Matrices must have the same dimensions for element-wise multiplication.");
        }

        RealMatrix result = new Array2DRowRealMatrix(matrixA.getRowDimension(), matrixA.getColumnDimension());

        for (int i = 0; i < matrixA.getRowDimension(); i++) {
            for (int j = 0; j < matrixA.getColumnDimension(); j++) {
                result.setEntry(i, j, matrixA.getEntry(i, j) * matrixB.getEntry(i, j));
            }
        }

        return result;
    }
    public static RealVector sqrtVector(RealVector vector) {  

        int dimension = vector.getDimension();  

        RealVector sqrtVector = new ArrayRealVector(dimension);  

        for (int i = 0; i < dimension; i++) {  
            double value = vector.getEntry(i); 
            if(value<=0){
                value=1e-6;
                sqrtVector.setEntry(i, 0);
            }else{
                sqrtVector.setEntry(i, Math.sqrt(value));  
            }
        }  

        return sqrtVector;  
    }  
    public static RealVector getdiagVector(RealMatrix matrix) {

        if (matrix.getRowDimension() != matrix.getColumnDimension()) {
            throw new IllegalArgumentException("Matrix must be square to extract diagonal.");
        }

        RealVector diag = new ArrayRealVector(matrix.getRowDimension());

        for (int i = 0; i < matrix.getRowDimension(); i++) {
            diag.setEntry(i, matrix.getEntry(i, i));
        }

        return diag;
    }
    public static RealMatrix sum(RealMatrix matrix, int dimension) {

        RealMatrix result = null;

        if (dimension == 0) {
            result = new Array2DRowRealMatrix(1, matrix.getColumnDimension());
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                double sum = 0;
                for (int i = 0; i < matrix.getRowDimension(); i++) {
                    sum += matrix.getEntry(i, j);
                }
                result.setEntry(0, j, sum);
            }
        }

        else if (dimension == 1) {
            result = new Array2DRowRealMatrix(matrix.getRowDimension(), 1);
            for (int i = 0; i < matrix.getRowDimension(); i++) {
                double sum = 0;
                for (int j = 0; j < matrix.getColumnDimension(); j++) {
                    sum += matrix.getEntry(i, j);
                }
                result.setEntry(i, 0, sum);
            }
        }

        else {
            throw new IllegalArgumentException("Dimension must be 1 or 2.");
        }

        return result;
    }
}