package how.hollow.producer.infrastructure;

import static how.hollow.producer.infrastructure.S3Blob.Kind.SNAPSHOT;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.netflix.hollow.api.producer.HollowProducer;
import com.netflix.hollow.core.memory.encoding.HashCodes;

public final class S3Blob implements HollowProducer.Blob {

    private final S3Blob.Kind kind;
    private final String namespace;
    final HollowProducer.Transition transition;
    final File product;
    private OutputStream out;

    S3Blob(S3Blob.Kind kind, String namespace, File parent, HollowProducer.Transition transition) {
        this.kind = kind;
        this.namespace = namespace;
        this.transition = transition;
        this.product = kind.getScratchFile(parent, namespace, transition);
    }

    @Override
    public OutputStream getOutputStream() {
        try {
            File parent = product.getParentFile();
            if(!parent.exists()) parent.mkdirs();
            out = new BufferedOutputStream(new FileOutputStream(product));
        } catch(FileNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return out;
    }

    @Override
    public void close() {
        closeOutputStream();
        product.delete();
    }

    boolean isSnapshot() {
        return kind == SNAPSHOT;
    }

    String getS3ObjectName() {
        return kind.getS3ObjectName(namespace, transition);
    }

    ObjectMetadata getS3ObjectMetadata() {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("Content-Length", product.length());
        kind.populateObjectMetadata(transition, metadata);
        return metadata;
    }

    private void closeOutputStream() {
        if(out != null) {
            try {
                out.close();
                out = null;
            } catch(IOException ex) {
                throw new RuntimeException(ex);
            }
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

        public void populateObjectMetadata(HollowProducer.Transition transition, ObjectMetadata metadata) {
            switch(this) {
            case SNAPSHOT:
                metadata.addUserMetadata("to_state", String.valueOf(transition.getToVersion()));
                break;
            case DELTA:
                populateDeltaMetadata(transition, metadata);
                break;
            case REVERSE_DELTA:
                populateDeltaMetadata(transition.reverse(), metadata);
                break;
            default:
                throw new IllegalStateException("unknown kind, kind=" + this);
            }
        }

        private void populateDeltaMetadata(HollowProducer.Transition transition, ObjectMetadata metadata) {
            metadata.addUserMetadata("from_state", String.valueOf(transition.getFromVersion()));
            metadata.addUserMetadata("to_state", String.valueOf(transition.getToVersion()));
        }

        private File getScratchFile(File parent, String namespace, HollowProducer.Transition transition) {
            String pattern;
            switch(this) {
            case SNAPSHOT:
                pattern = "%s-%s-%d";
                break;
            case DELTA:
                pattern = "%s-%s-%d-%d";
                break;
            case REVERSE_DELTA:
                pattern = "%s-%s-%d-%d";
                transition = transition.reverse();
                break;
            default:
                throw new IllegalStateException("unknown kind, kind=" + this);
            }
            String filename = String.format(pattern, namespace, prefix, transition.getFromVersion(), transition.getToVersion());
            return new File(parent, filename);
        }

        public String getS3ObjectPrefix(String blobNamespace) {
            return new StringBuilder(blobNamespace)
                    .append("/")
                    .append(prefix)
                    .append("/")
                    .toString();
        }

        public String getS3ObjectName(String blobNamespace, HollowProducer.Transition transition) {
            return getS3ObjectName(blobNamespace, transition.getToVersion());
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