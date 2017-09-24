package how.hollow.consumer.infrastructure;

public class BlobNotFoundException extends RuntimeException {
    BlobNotFoundException() {
        super("not found");
    }
}

