import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.jsoup.nodes.Document;

public class S3Manager {
    
    public boolean bucketExists(String bucketName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        return s3Client.doesBucketExistV2(bucketName);
    }
    
    public Bucket createS3Bucket(String bucketName) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        
        if (s3Client.doesBucketExistV2(bucketName)) {
            return s3Client.listBuckets().stream()
                    .filter(bucket -> bucket.getName().equals(bucketName))
                    .findFirst().orElse(null);
        }
        return s3Client.createBucket(bucketName);
    }
    
    public void writeDocumentToS3(String bucketName, String key, Document document) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(document.html().getBytes());
        PutObjectRequest request = new PutObjectRequest(bucketName, key, inputStream, null);
        s3Client.putObject(request);
    }
    
    public String generateKey(String url) {
        String sanitized = url.replaceAll("https?://", "")
                             .replaceAll("[^a-zA-Z0-9.-]", "_")
                             .replaceAll("_+", "_");
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return sanitized + "_" + timestamp + ".html";
    }
    
    public String getDocumentHash(Document document) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(document.html().getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            return null;
        }
    }
}