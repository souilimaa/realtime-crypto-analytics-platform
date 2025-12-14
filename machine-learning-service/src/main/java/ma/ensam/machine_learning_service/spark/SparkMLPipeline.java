package ma.ensam.machine_learning_service.spark;


import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SparkMLPipeline {

    @Autowired
    private SparkSession spark;

    @Autowired
    private CryptoDataLoader dataLoader;

    @Autowired
    private FeatureEngineeringService featureService;

    @Autowired
    private PricePredictionModel predictionModel;

    @Autowired
    //private TrendClassificationModel classificationModel;

    public void runFullPipeline(String dataPath) {

        System.out.println("===== Starting ML Pipeline =====");

        // 1. Load data
        System.out.println("Loading data...");
        Dataset<Row> rawData = dataLoader.loadFromJson(dataPath);
        rawData.show(5);

        // 2. Feature engineering
        System.out.println("Creating features...");
        Dataset<Row> featuredData = featureService.createFeatures(rawData);
        Dataset<Row> mlData = featureService.prepareMLFeatures(featuredData);

        // 3. Create target variable (predict price 5 periods ahead)
        System.out.println("Creating target variable...");
        Dataset<Row> labeledData = predictionModel.createTargetVariable(mlData, 5);

        // 4. Split data
        System.out.println("Splitting data into train/test...");
        Dataset<Row>[] splits = labeledData.randomSplit(new double[]{0.8, 0.2}, 42);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        System.out.println("Training samples: " + trainingData.count());
        System.out.println("Test samples: " + testData.count());

        // 5. Train model
        System.out.println("Training Random Forest model...");
        RandomForestRegressionModel model = predictionModel.trainRandomForest(trainingData);

        // 6. Make predictions
        System.out.println("Making predictions...");
        Dataset<Row> predictions = model.transform(testData);

        // 7. Evaluate
        System.out.println("Evaluating model...");
        predictionModel.evaluateModel(predictions);

        // 8. Show sample predictions
        predictions.select("pair", "time", "price", "future_price", "prediction")
                .show(20, false);

        // 9. Save model
        String modelPath = "/models/crypto-rf-model";
        System.out.println("Saving model to: " + modelPath);
        try {
            model.write().overwrite().save(modelPath);
            System.out.println("Model saved successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("===== Pipeline Complete =====");
    }

    // Load and predict with saved model
    public Dataset<Row> predictWithSavedModel(String modelPath, String dataPath) {

        // Load model
        RandomForestRegressionModel model = RandomForestRegressionModel.load(modelPath);

        // Load and prepare data
        Dataset<Row> rawData = dataLoader.loadFromJson(dataPath);
        Dataset<Row> featuredData = featureService.createFeatures(rawData);
        Dataset<Row> mlData = featureService.prepareMLFeatures(featuredData);

        // Make predictions
        return model.transform(mlData);
    }

}
