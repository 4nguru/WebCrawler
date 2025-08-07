import java.io.IOException;
import com.amazonaws.services.s3.model.Bucket;

public class Main {
    public static void main(String[] args) throws IOException {
        S3Manager s3Manager = new S3Manager();
        String bucketName = "webcrawler-bucket-gururan-2024";
        
        try {
            if (!s3Manager.bucketExists(bucketName)) {
                Bucket bucket = s3Manager.createS3Bucket(bucketName);
                System.out.println("Created new bucket: " + bucketName);
            } else {
                System.out.println("Using existing bucket: " + bucketName);
            }
        } catch (Exception e) {
            System.err.println("Bucket creation failed: " + e.getMessage());
            return;
        }
        String queueName = "webcrawler-queue";
        String tableName = "webcrawler-records";
        
        // Initialize queue with seed URL
        QueueManager queueManager = new QueueManager(queueName);
        queueManager.addUrl("http://www.sasken.com");
        
        Crawler crawler = new Crawler(queueName, tableName);
        try {
            crawler.crawlNext(bucketName);
            System.out.printf("Crawler successful!");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}