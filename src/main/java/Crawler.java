import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Crawler {
    private final S3Manager s3Manager = new S3Manager();
    private final QueueManager queueManager;
    private final CrawlRecordManager recordManager;
    
    public Crawler(String queueName, String tableName) {
        this.queueManager = new QueueManager(queueName);
        this.recordManager = new CrawlRecordManager(tableName);
    }

    public void crawlNext(String bucketName) throws IOException {
        String url = queueManager.getNextUrl();
        if (url == null) {
            System.out.println("No URLs to crawl");
            return;
        }
        
        crawlURL(url, bucketName);
    }
    
    public void crawlURL(String url, String bucketName) throws IOException {
        try {
            Document doc = Jsoup.connect(url).get();
            String newHash = s3Manager.getDocumentHash(doc);
            
            String storedHash = recordManager.getStoredHash(url);
            
            if (storedHash != null && storedHash.equals(newHash)) {
                System.out.println("URL content unchanged: " + url);
                return;
            }
            
            String key = s3Manager.generateKey(url);
            s3Manager.writeDocumentToS3(bucketName, key, doc);
            recordManager.saveCrawlRecord(url, key, newHash);
            
            // Extract and queue new URLs from the page
            Elements links = doc.select("a[href]");
            for (Element link : links) {
                String href = link.attr("abs:href");
                if (href.startsWith("http") && recordManager.getStoredHash(href) == null) {
                    queueManager.addUrl(href);
                }
            }
            
            if (storedHash == null) {
                System.out.println("New URL crawled: " + url + " -> " + key);
            } else {
                System.out.println("URL updated: " + url + " -> " + key);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    

    private static void print(String msg, Object... args) {
            System.out.println(String.format(msg, args));
    }
}
