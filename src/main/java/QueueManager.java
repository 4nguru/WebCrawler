import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import java.util.List;

public class QueueManager {
    private final AmazonSQS sqs;
    private final String queueUrl;
    
    public QueueManager(String queueName) {
        this.sqs = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        this.queueUrl = getOrCreateQueue(queueName);
    }
    
    private String getOrCreateQueue(String queueName) {
        try {
            return sqs.getQueueUrl(queueName).getQueueUrl();
        } catch (QueueDoesNotExistException e) {
            CreateQueueRequest request = new CreateQueueRequest(queueName);
            return sqs.createQueue(request).getQueueUrl();
        }
    }
    
    public void addUrl(String url) {
        sqs.sendMessage(new SendMessageRequest(queueUrl, url));
    }
    
    public String getNextUrl() {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(1)
                .withWaitTimeSeconds(1)
                .withVisibilityTimeout(30); // 30 seconds visibility timeout
        
        List<Message> messages = sqs.receiveMessage(request).getMessages();
        if (!messages.isEmpty()) {
            Message message = messages.get(0);
            String url = message.getBody();
            sqs.deleteMessage(queueUrl, message.getReceiptHandle());
            System.out.println("Retrieved URL from queue: " + url);
            return url;
        }
        return null;
    }
    
    public int getQueueDepth() {
        GetQueueAttributesRequest request = new GetQueueAttributesRequest(queueUrl)
                .withAttributeNames("ApproximateNumberOfMessages");
        return Integer.parseInt(sqs.getQueueAttributes(request)
                .getAttributes().get("ApproximateNumberOfMessages"));
    }
}