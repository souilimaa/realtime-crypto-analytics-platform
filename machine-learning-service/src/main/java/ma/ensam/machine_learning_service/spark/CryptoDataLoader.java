package ma.ensam.machine_learning_service.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import ma.ensam.machine_learning_service.feign.CryptoFeign;
import ma.ensam.machine_learning_service.models.Crypto;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@SuppressWarnings("unused")

@Component
public class CryptoDataLoader {

    @Autowired
    private SparkSession spark;

    @Autowired
    private CryptoFeign cryptoFeign;

    @Autowired
    private ObjectMapper objectMapper;

    // Load from PostgreSQL
    public Dataset<Row> loadFromMicroservice() {
        // Fetch data from microservice
        List<Crypto> data = cryptoFeign.findAll();

        if (data == null || data.isEmpty()) {
            throw new RuntimeException("No data received from microservice");
        }

        // Convert DTOs to JSON strings
        List<String> jsonStrings = data.stream()
                .map(this::convertDtoToJson)
                .toList();

        // Create Spark Dataset from JSON using JavaRDD
        return spark.read()
                .json(spark.createDataset(jsonStrings,
                        org.apache.spark.sql.Encoders.STRING()));
    }

    private String convertToJsonString(List<Crypto> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert list to JSON", e);
        }
    }

    private String convertDtoToJson(Crypto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert DTO to JSON", e);
        }
    }

    public Dataset<Row> loadFromJson(String path) {
        return spark.read()
                .option("multiline", "true")
                .json(path);
    }

}
