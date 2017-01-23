package org.heigit.hosm.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;

import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by Jiaoyan on 1/16/17.
 * Some examples to use MLlib for classification/regression
 * They have been tested with the local model of Spark 1.5.2
 *
 * Updated in 1/23/17
 */
public class MLlib_Examples {
    public static void main(String args[]) throws URISyntaxException {
        String dir = null;
        SparkConf sparkConf = null;
        if (args.length == 0) {
            dir = MLlib_Examples.class.getResource("/").toURI().getPath() + "../../src/main/resources/";
            sparkConf = new SparkConf().setAppName("SVM Classifier Example").setMaster("local");
        } else {
            dir = args[0];
            sparkConf = new SparkConf().setAppName("SVM Classifier Example");
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<double []> l = new ArrayList<>();
        l.add(new double []{2,3,4});
        l.add(new double []{0.2,0.3,0.4});
        JavaRDD<double []> d = jsc.parallelize(l);

        JavaRDD<Vector> parsedData = d.map(
                new Function<double [], Vector>() {
                    public Vector call(double [] v) {
                        return Vectors.dense(v);
                    }
                }
        );
        parsedData.cache();

        RowMatrix mat = new RowMatrix(parsedData.rdd());
        long m = mat.numRows();
        long n = mat.numCols();
        System.out.printf("(%d, %d)",m,n);

        JavaDoubleRDD dd1 = jsc.parallelizeDoubles(Arrays.asList(new Double []{1.1,2.2,2.9} ));
        JavaDoubleRDD dd2 = jsc.parallelizeDoubles(Arrays.asList(new Double []{1.1,3.2,3.1} ));
        Double correlation = Statistics.corr(dd1.srdd(), dd2.srdd(), "pearson");
        System.out.printf("corr: %f", correlation);
        //classification_test1(jsc,dir);
        //classification_test2(jsc,dir);
        //classification_test3(jsc, dir);
        //regression_test1(jsc,dir);
        //data_type_test(jsc,dir);
        jsc.stop();
    }

    /**
     * the example about data type in MLLib
     * @param jsc
     * @param dir
     */
    public static void data_type_test(JavaSparkContext jsc, String dir){
        String path = dir + "lpsa.data";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++)
                            v[i] = Double.parseDouble(features[i]);
                        return Vectors.dense(v);
                    }
                }
        );
        RowMatrix mat = new RowMatrix(parsedData.rdd());
        long m = mat.numRows();
        long n = mat.numCols();
        System.out.printf("(%d, %d)",m,n);
    }

    /**
     * The first example to build and test regression model
     */
    public static void regression_test1(JavaSparkContext jsc, String dir) throws URISyntaxException {
        String path = dir + "lpsa.data";
        JavaRDD<String> data = jsc.textFile(path);
        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");
                        String[] features = parts[1].split(" ");
                        double[] v = new double[features.length];
                        for (int i = 0; i < features.length - 1; i++)
                            v[i] = Double.parseDouble(features[i]);
                        return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
                    }
                }
        );

        parsedData.cache();
        int numIterations = 100;
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<Double, Double>(prediction, point.label());
                    }
                }
        );

        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        System.out.println(pair._1() - pair._2());
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);

    }

    /**
     * Example to build and test classification model
     * @param jsc
     * @param dir
     * @throws URISyntaxException
     */
    public static void classification_test2(JavaSparkContext jsc, String dir) throws URISyntaxException {
        String path = dir + "sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        // Split initial RDD into two... [60% training data, 40% testing data].
        JavaRDD<LabeledPoint> training = data.sample(false, 0.7, 11L);
        training.cache();
        JavaRDD<LabeledPoint> test = data.subtract(training);

        // Run training algorithm to build the model.
        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);

        // Clear the default threshold.
        model.clearThreshold();

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        return new Tuple2<Object, Object>(score, p.label());
                    }
                }
        );

        // Get evaluation metrics.
        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auROC = metrics.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

        // Save and load model
//        model.save(jsc.sc(), "file:///Users/John/Downloads/myModel");
//        SVMModel sameModel = SVMModel.load(jsc.sc(), "file:///Users/John/Downloads/myModel");
    }

    public static void classification_test1(JavaSparkContext jsc, String dir) throws URISyntaxException {
        String path = dir + "sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(10)
                .run(training.rdd());

        // Compute raw scores on the test set.
        JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double prediction = model.predict(p.features());
                        return new Tuple2<Object, Object>(prediction, p.label());
                    }
                }
        );

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double precision = metrics.precision();
        double recall = metrics.recall();
        System.out.println("Precision = " + precision);
        System.out.println("Recall = " + recall);

        // Save and load model
        //model.save(jsc.sc(), dir + "model");
        //LogisticRegressionModel sameModel = LogisticRegressionModel.load(jsc.sc(),
        //             dir + "model");
    }

    public static void classification_test3(JavaSparkContext jsc, String dir) throws URISyntaxException {
        String path = dir + "sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;

        // Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification tree model:\n" + model.toDebugString());

    }
}

