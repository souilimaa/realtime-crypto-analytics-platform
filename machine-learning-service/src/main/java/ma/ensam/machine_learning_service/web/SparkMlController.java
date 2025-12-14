package ma.ensam.machine_learning_service.web;

import org.springframework.web.bind.annotation.RestController;

@RestController
public class SparkMlController {

//    @Autowired
//    private SparkMLPipeline mlPipeline;
//
//    @Autowired
//    private CryptoDataLoader dataLoader;
//
//    @PostMapping("/train")
//    public ResponseEntity<Map<String, String>> trainModel() {
//        try {
//            // Load data from microservice
//            Dataset<Row> data = dataLoader.loadFromMicroservice();
//
//            // Save to temporary location for pipeline
//            String tempPath = "/tmp/crypto-data-" + System.currentTimeMillis();
//            data.write().mode("overwrite").json(tempPath);
//
//            // Run training pipeline
//            mlPipeline.runFullPipeline(tempPath);
//
//            Map<String, String> response = new HashMap<>();
//            response.put("status", "success");
//            response.put("message", "Model trained successfully");
//
//            return ResponseEntity.ok(response);
//        } catch (Exception e) {
//            Map<String, String> response = new HashMap<>();
//            response.put("status", "error");
//            response.put("message", e.getMessage());
//            return ResponseEntity.internalServerError().body(response);
//        }
//    }
}
