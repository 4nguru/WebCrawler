import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class CrawlRecordManager {
    private final AmazonDynamoDB dynamoDB;
    private final String tableName;
    
    public CrawlRecordManager(String tableName) {
        this.dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        this.tableName = tableName;
        createTableIfNotExists();
    }
    
    private void createTableIfNotExists() {
        try {
            dynamoDB.describeTable(tableName);
        } catch (ResourceNotFoundException e) {
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(new KeySchemaElement("url", KeyType.HASH))
                    .withAttributeDefinitions(new AttributeDefinition("url", ScalarAttributeType.S))
                    .withBillingMode(BillingMode.PAY_PER_REQUEST);
            dynamoDB.createTable(request);
        }
    }
    
    public String getStoredHash(String url) {
        GetItemRequest request = new GetItemRequest()
                .withTableName(tableName)
                .withKey(Map.of("url", new AttributeValue(url)));
        
        GetItemResult result = dynamoDB.getItem(request);
        if (result.getItem() != null && result.getItem().containsKey("hash")) {
            return result.getItem().get("hash").getS();
        }
        return null;
    }
    
    public void saveCrawlRecord(String url, String s3Key, String hash) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("url", new AttributeValue(url));
        item.put("s3Key", new AttributeValue(s3Key));
        item.put("hash", new AttributeValue(hash));
        item.put("timestamp", new AttributeValue(LocalDateTime.now().toString()));
        
        PutItemRequest request = new PutItemRequest()
                .withTableName(tableName)
                .withItem(item);
        dynamoDB.putItem(request);
    }
}