package ma.ensam.machine_learning_service.spark;

import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.stereotype.Service;
import static org.apache.spark.sql.functions.*;
@SuppressWarnings("unused")


@Service
public class FeatureEngineeringService {

    public Dataset<Row> createFeatures(Dataset<Row> rawData) {

        // Sort by pair and time
        Dataset<Row> sortedData = rawData.orderBy("pair", "time");

        // Define window specifications
        WindowSpec pairWindow = Window.partitionBy("pair").orderBy("time");
        WindowSpec movingWindow5 = Window.partitionBy("pair")
                .orderBy("time")
                .rowsBetween(-4, 0); // 5 period window
        WindowSpec movingWindow20 = Window.partitionBy("pair")
                .orderBy("time")
                .rowsBetween(-19, 0); // 20 period window

        // Create features
        Dataset<Row> featuredData = sortedData
                // Basic price features
                .withColumn("spread", col("ask").minus(col("bid")))
                .withColumn("mid_price", col("ask").plus(col("bid")).divide(2))
                .withColumn("spread_pct",
                        col("spread").divide(col("mid_price")).multiply(100))

                // Volume features
                .withColumn("volume_ratio",
                        when(col("rfq_volume").gt(0), col("volume").divide(col("rfq_volume")))
                                .otherwise(0))
                .withColumn("trade_volume", col("size").multiply(col("price")))

                // Price changes
                .withColumn("price_lag1", lag("price", 1).over(pairWindow))
                .withColumn("price_lag5", lag("price", 5).over(pairWindow))
                .withColumn("price_lag10", lag("price", 10).over(pairWindow))
                .withColumn("price_change",
                        col("price").minus(col("price_lag1")))
                .withColumn("price_change_pct",
                        when(col("price_lag1").gt(0),
                                col("price_change").divide(col("price_lag1")).multiply(100))
                                .otherwise(0))

                // Moving averages
                .withColumn("sma_5", avg("price").over(movingWindow5))
                .withColumn("sma_20", avg("price").over(movingWindow20))
                .withColumn("ema_5",
                        avg("price").over(movingWindow5)) // Simplified EMA
                .withColumn("ema_20",
                        avg("price").over(movingWindow20))

                // Volume moving averages
                .withColumn("volume_sma_5", avg("volume").over(movingWindow5))
                .withColumn("volume_sma_20", avg("volume").over(movingWindow20))
                .withColumn("volume_change",
                        col("volume").minus(lag("volume", 1).over(pairWindow)))
                .withColumn("volume_change_pct",
                        when(lag("volume", 1).over(pairWindow).gt(0),
                                col("volume_change").divide(lag("volume", 1).over(pairWindow)).multiply(100))
                                .otherwise(0))

                // Volatility (standard deviation)
                .withColumn("volatility_5", stddev("price").over(movingWindow5))
                .withColumn("volatility_20", stddev("price").over(movingWindow20))

                // High/Low over windows
                .withColumn("high_5", max("price").over(movingWindow5))
                .withColumn("low_5", min("price").over(movingWindow5))
                .withColumn("high_20", max("price").over(movingWindow20))
                .withColumn("low_20", min("price").over(movingWindow20))

                // Price position in range
                .withColumn("price_position_5",
                        when(col("high_5").minus(col("low_5")).gt(0),
                                col("price").minus(col("low_5"))
                                        .divide(col("high_5").minus(col("low_5"))))
                                .otherwise(0.5))
                .withColumn("price_position_20",
                        when(col("high_20").minus(col("low_20")).gt(0),
                                col("price").minus(col("low_20"))
                                        .divide(col("high_20").minus(col("low_20"))))
                                .otherwise(0.5))

                // Bollinger Bands
                .withColumn("bb_upper_20", col("sma_20").plus(col("volatility_20").multiply(2)))
                .withColumn("bb_lower_20", col("sma_20").minus(col("volatility_20").multiply(2)))
                .withColumn("bb_position",
                        when(col("bb_upper_20").minus(col("bb_lower_20")).gt(0),
                                col("price").minus(col("bb_lower_20"))
                                        .divide(col("bb_upper_20").minus(col("bb_lower_20"))))
                                .otherwise(0.5))

                // RSI (Relative Strength Index)
                .withColumn("gain",
                        when(col("price_change").gt(0), col("price_change")).otherwise(0))
                .withColumn("loss",
                        when(col("price_change").lt(0), abs(col("price_change"))).otherwise(0))
                .withColumn("avg_gain_14", avg("gain").over(pairWindow.rowsBetween(-13, 0)))
                .withColumn("avg_loss_14", avg("loss").over(pairWindow.rowsBetween(-13, 0)))
                .withColumn("rs",
                        when(col("avg_loss_14").gt(0), col("avg_gain_14").divide(col("avg_loss_14")))
                                .otherwise(100))
                .withColumn("rsi", lit(100).minus(lit(100).divide(lit(1).plus(col("rs")))))

                // MACD
                .withColumn("macd", col("ema_5").minus(col("ema_20")))
                .withColumn("macd_signal", avg("macd").over(pairWindow.rowsBetween(-8, 0)))
                .withColumn("macd_histogram", col("macd").minus(col("macd_signal")))

                // Momentum
                .withColumn("momentum_5",
                        when(col("price_lag5").gt(0),
                                col("price").minus(col("price_lag5")))
                                .otherwise(0))
                .withColumn("momentum_10",
                        when(col("price_lag10").gt(0),
                                col("price").minus(col("price_lag10")))
                                .otherwise(0))

                // Rate of change
                .withColumn("roc_5",
                        when(col("price_lag5").gt(0),
                                col("price").minus(col("price_lag5"))
                                        .divide(col("price_lag5")).multiply(100))
                                .otherwise(0))
                .withColumn("roc_10",
                        when(col("price_lag10").gt(0),
                                col("price").minus(col("price_lag10"))
                                        .divide(col("price_lag10")).multiply(100))
                                .otherwise(0))

                // Ask/Bid features
                .withColumn("ask_lag1", lag("ask", 1).over(pairWindow))
                .withColumn("bid_lag1", lag("bid", 1).over(pairWindow))
                .withColumn("ask_change", col("ask").minus(col("ask_lag1")))
                .withColumn("bid_change", col("bid").minus(col("bid_lag1")))

                // Trade ID momentum (trading activity)
                .withColumn("trade_id_lag1", lag("trade_id", 1).over(pairWindow))
                .withColumn("trade_count", col("trade_id").minus(col("trade_id_lag1")))

                // Size features
                .withColumn("size_sma_5", avg("size").over(movingWindow5))
                .withColumn("size_sma_20", avg("size").over(movingWindow20))

                // Time features
                .withColumn("hour", hour(col("time")))
                .withColumn("day_of_week", dayofweek(col("time")))
                .withColumn("day_of_month", dayofmonth(col("time")))
                .withColumn("minute", minute(col("time")))

                // Time-based patterns
                .withColumn("is_weekend",
                        when(col("day_of_week").isin(1, 7), 1).otherwise(0))
                .withColumn("is_business_hours",
                        when(col("hour").between(9, 17), 1).otherwise(0));

        // Drop rows with nulls (first few rows from lag/window operations)
        return featuredData.na().drop();
    }

    public Dataset<Row> prepareMLFeatures(Dataset<Row> featuredData) {

        // Select features for ML (all numeric features)
        String[] featureCols = {
                // Price features
                "spread", "spread_pct", "mid_price",
                "price_change", "price_change_pct",

                // Volume features
                "volume", "volume_ratio", "volume_change", "volume_change_pct",
                "rfq_volume", "size", "trade_volume",

                // Moving averages
                "sma_5", "sma_20", "ema_5", "ema_20",
                "volume_sma_5", "volume_sma_20",
                "size_sma_5", "size_sma_20",

                // Volatility
                "volatility_5", "volatility_20",

                // Price positions
                "price_position_5", "price_position_20", "bb_position",

                // Technical indicators
                "rsi", "macd", "macd_histogram",
                "momentum_5", "momentum_10",
                "roc_5", "roc_10",

                // Ask/Bid
                "ask", "bid", "ask_change", "bid_change",

                // Trading activity
                "trade_count",

                // Time features
                "hour", "day_of_week", "day_of_month", "minute",
                "is_weekend", "is_business_hours"
        };
        // Assemble features into vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("features_raw")
                .setHandleInvalid("skip");

        Dataset<Row> assembledData = assembler.transform(featuredData);

        // Scale features
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features_raw")
                .setOutputCol("features")
                .setWithStd(true)
                .setWithMean(true);

        return scaler.fit(assembledData).transform(assembledData);
    }

    public String[] getFeatureNames() {
        return new String[]{
                "spread", "spread_pct", "mid_price",
                "price_change", "price_change_pct",
                "volume", "volume_ratio", "volume_change", "volume_change_pct",
                "rfq_volume", "size", "trade_volume",
                "sma_5", "sma_20", "ema_5", "ema_20",
                "volume_sma_5", "volume_sma_20",
                "size_sma_5", "size_sma_20",
                "volatility_5", "volatility_20",
                "price_position_5", "price_position_20", "bb_position",
                "rsi", "macd", "macd_histogram",
                "momentum_5", "momentum_10",
                "roc_5", "roc_10",
                "ask", "bid", "ask_change", "bid_change",
                "trade_count",
                "hour", "day_of_week", "day_of_month", "minute",
                "is_weekend", "is_business_hours"
        };
    }

}
