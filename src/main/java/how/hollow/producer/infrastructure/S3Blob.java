package how.hollow.producer.infrastructure;

import static how.hollow.producer.infrastructure.S3Blob.Kind.SNAPSHOT;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newInputStream;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.size;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.core.memory.encoding.HashCodes;

public final class S3Blob implements HollowProducer.Blob {

    private final S3Blob.Kind kind;
    private final String namespace;
    final long fromVersion;
    final long toVersion;
    final Path product;
    private OutputStream out;
    private BufferedInputStream in;

    S3Blob(S3Blob.Kind kind, String namespace, Path parent, long fromVersion, long toVersion) {
        this.kind = kind;
        this.namespace = namespace;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
        this.product = kind.getProductPath(parent, namespace, fromVersion, toVersion);
    }

    @Override
    public OutputStream getOutputStream() {
        try {
            createDirectories(product.getParent());
            out = new BufferedOutputStream(newOutputStream(product));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
        return out;
    }

    @Override
    public InputStream getInputStream() {
        try {
            in = new BufferedInputStream(newInputStream(product));
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
        return in;
    }

    @Override
    public void close() {
        try {
            // FIXME: timt: failure while closing `out` will leave `in` opened
            if(out != null) out.close();
            out = null;
            if(in != null) in.close();
            in = null;
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
        // FIXME: timt: we products lingering a bit longer so that we can round-trip them into read states
        //    without having to fetch the blobs we just published
        try {
            deleteIfExists(product);
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }

    boolean isSnapshot() {
        return kind == SNAPSHOT;
    }

    String getS3ObjectName() {
        return kind.getS3ObjectName(namespace, toVersion);
    }

    ObjectMetadata getS3ObjectMetadata() {
        try {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader("Content-Length", size(product));
            kind.populateObjectMetadata(fromVersion, toVersion, metadata);
            return metadata;
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static enum Kind {

        SNAPSHOT("snapshot"),
        DELTA("delta"),
        REVERSE_DELTA("reversedelta");

        private final String prefix;

        Kind(String prefix) {
            this.prefix = prefix;
        }

        public void populateObjectMetadata(long fromVersion, long toVersion, ObjectMetadata metadata) {
            switch(this) {
            case SNAPSHOT:
                metadata.addUserMetadata("to_state", String.valueOf(toVersion));
                break;
            case DELTA:
                populateDeltaMetadata(fromVersion, toVersion, metadata);
                break;
            case REVERSE_DELTA:
                populateDeltaMetadata(toVersion, fromVersion, metadata);
                break;
            default:
                throw new IllegalStateException("unknown kind, kind=" + this);
            }
        }

        private void populateDeltaMetadata(long origin, long destination, ObjectMetadata metadata) {
            metadata.addUserMetadata("from_state", String.valueOf(origin));
            metadata.addUserMetadata("to_state", String.valueOf(destination));
        }

        private Path getProductPath(Path parent, String namespace, long fromVersion, long toVersion) {
            switch(this) {
            case SNAPSHOT:
                return parent.resolve(String.format("%s-%s-%d", namespace, prefix, toVersion));
            case DELTA:
                return parent.resolve(String.format("%s-%s-%d-%d", namespace, prefix, fromVersion, toVersion));
            case REVERSE_DELTA:
                return parent.resolve(String.format("%s-%s-%d-%d", namespace, prefix, toVersion, fromVersion));
            default:
                throw new IllegalStateException("unknown kind, kind=" + this);
            }
        }

        public String getS3ObjectPrefix(String blobNamespace) {
            return new StringBuilder(blobNamespace)
                    .append("/")
                    .append(prefix)
                    .append("/")
                    .toString();
        }

        public String getS3ObjectName(String blobNamespace, long lookupVersion) {
            return new StringBuilder(getS3ObjectPrefix(blobNamespace))
                    .append(Integer.toHexString(HashCodes.hashLong(lookupVersion)))
                    .append('-')
                    .append(lookupVersion)
                    .toString();
        }
    }

}