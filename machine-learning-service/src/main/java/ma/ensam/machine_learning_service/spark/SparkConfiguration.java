package ma.ensam.machine_learning_service.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.spark.sql.SparkSession;



@Configuration
public class SparkConfiguration {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("CryptoMLService")
                .master("local[*]") // Change to "spark://host:port" for cluster mode
                .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "2g")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate();
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
