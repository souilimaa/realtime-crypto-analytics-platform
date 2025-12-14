package ma.ensam.machine_learning_service.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.lead;

@Service
public class PricePredictionModel {

    public RandomForestRegressionModel trainRandomForest(Dataset<Row> trainingData) {

        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("future_price")
                .setFeaturesCol("features")
                .setNumTrees(100)
                .setMaxDepth(10)
                .setMaxBins(32)
                .setMinInstancesPerNode(1)
                .setSeed(42);

        return rf.fit(trainingData);
    }

    public GBTRegressionModel trainGradientBoosting(Dataset<Row> trainingData) {
        GBTRegressor gbt = new GBTRegressor()
                .setLabelCol("future_price")
                .setFeaturesCol("features")
                .setMaxIter(100)
                .setMaxDepth(5)
                .setStepSize(0.1)
                .setSeed(42);

        return gbt.fit(trainingData);
    }

    public LinearRegressionModel trainLinearRegression(Dataset<Row> trainingData) {

        LinearRegression lr = new LinearRegression()
                .setLabelCol("future_price")
                .setFeaturesCol("features")
                .setMaxIter(100)
                .setRegParam(0.1)
                .setElasticNetParam(0.5);
        return lr.fit(trainingData);
    }

    public CrossValidatorModel trainWithCrossValidation(Dataset<Row> trainingData) {

        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("future_price")
                .setFeaturesCol("features");

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{rf});

        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(rf.numTrees(), new int[]{50, 100, 150})
                .addGrid(rf.maxDepth(), new int[]{5, 10, 15})
                .build();

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("future_price")
                .setPredictionCol("prediction")
                .setMetricName("rmse");

        CrossValidator cv = new CrossValidator()
                .setEstimator(pipeline)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramGrid)
                .setNumFolds(5)
                .setParallelism(4);

        return cv.fit(trainingData);
    }

    public void evaluateModel(Dataset<Row> predictions) {

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("future_price")
                .setPredictionCol("prediction");

        double rmse = evaluator.setMetricName("rmse").evaluate(predictions);
        double mae = evaluator.setMetricName("mae").evaluate(predictions);
        double r2 = evaluator.setMetricName("r2").evaluate(predictions);
        double mse = evaluator.setMetricName("mse").evaluate(predictions);

        System.out.println("===== Model Evaluation Metrics =====");
        System.out.println("Root Mean Squared Error (RMSE): " + rmse);
        System.out.println("Mean Absolute Error (MAE): " + mae);
        System.out.println("R² Score: " + r2);
        System.out.println("Mean Squared Error (MSE): " + mse);
        System.out.println("====================================");
    }

    public Dataset<Row> createTargetVariable(Dataset<Row> data, int periodsAhead) {
        return data.withColumn("future_price",
                        lead("price", periodsAhead)
                                .over(org.apache.spark.sql.expressions.Window
                                        .partitionBy("pair")
                                        .orderBy("time")))
                .na().drop();
    }
}
