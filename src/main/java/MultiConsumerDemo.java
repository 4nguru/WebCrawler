import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiConsumerDemo {
    public static void main(String[] args) throws InterruptedException {
        String bucketName = "webcrawler-bucket-gururan-2024";
        String queueName = "webcrawler-queue";
        String tableName = "webcrawler-records";
        
        // Initialize S3 bucket
        S3Manager s3Manager = new S3Manager();
        if (!s3Manager.bucketExists(bucketName)) {
            s3Manager.createS3Bucket(bucketName);
        }
        
        // Add seed URLs to queue
        QueueManager queueManager = new QueueManager(queueName);
        queueManager.addUrl("http://www.google.com");
        queueManager.addUrl("https://www.ndtv.com");
        queueManager.addUrl("https://www.rediff.com");
        
        // Create multiple consumers (simulating Lambda instances)
        ExecutorService executor = Executors.newFixedThreadPool(3);
        
        // Consumer 1
        executor.submit(() -> runConsumer("Consumer-1", queueName, tableName, bucketName));
        
        // Consumer 2  
        executor.submit(() -> runConsumer("Consumer-2", queueName, tableName, bucketName));
        
        // Consumer 3
        executor.submit(() -> runConsumer("Consumer-3", queueName, tableName, bucketName));
        
        // Let consumers run for 30 seconds
        Thread.sleep(30000);
        executor.shutdownNow();
    }
    
    private static void runConsumer(String consumerName, String queueName, String tableName, String bucketName) {
        Crawler crawler = new Crawler(queueName, tableName);
        
        while (!Thread.currentThread().isInterrupted()) {
            try {
                System.out.println(consumerName + " checking for URLs...");
                crawler.crawlNext(bucketName);
                Thread.sleep(2000); // 2 second delay between checks
            } catch (IOException e) {
                System.err.println(consumerName + " error: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        System.out.println(consumerName + " stopped");
    }
}