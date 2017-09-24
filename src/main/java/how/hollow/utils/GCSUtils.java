package how.hollow.utils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.IOUtils;
import com.google.api.services.storage.Storage;
import com.netflix.hollow.core.memory.encoding.HashCodes;
import com.netflix.hollow.core.memory.encoding.VarInt;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class GCSUtils {

    /**
     * This will be replaced in the namespace based on the "user.name" system property
     */
    private static final String USERNAME_PATTERN = "<USERNAME>";

    /**
     * This will be replaced in the namespace with a date formatted with the pattern "yyyyMMddHHmmss"
     */
    private static final String TIMESTAMP_PATTERN = "<TIMESTAMP>";

    public static String getFolder(String domain, String namespace) {
        return domain + File.separator + namespace
                .replaceAll(USERNAME_PATTERN, System.getProperty("user.name"))
                .replaceAll(TIMESTAMP_PATTERN, new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
    }

    public static Storage getStorage(String applicationName) {
        try {
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            List<String> scopes = Collections.singletonList("https://www.googleapis.com/auth/devstorage.read_write");
            GoogleCredential credential = GoogleCredential.getApplicationDefault().createScoped(scopes);
            return new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(applicationName).build();
        } catch (Exception e) {
            throw new RuntimeException("error initializing", e);
        }
    }

    public static String getBlobName(String folder, String fileType, long lookupVersion) {
        return getGCSObjectPrefix(folder, fileType) +
                Integer.toHexString(HashCodes.hashLong(lookupVersion)) +
                "-" +
                lookupVersion;
    }

    public static String getGCSObjectPrefix(String folder, String fileType) {
        return folder + File.separator + fileType + File.separator;
    }


    /*
    * We need an index over the available state versions for which snapshot blobs are available.
    * The GCSPublisher stores that index as an object with a known key in S3.
    * The remainder of this class deals with maintaining that index.
    */
    public static String getSnapshotIndexObjectName(String folder) {
        return folder + File.separator + "snapshot.index";
    }

    public static long findNextHighestVersion(long desiredVersion, InputStream inputStream) throws IOException {
        File tempFile;
        try {
            tempFile = File.createTempFile("snapshot", "index");
        } catch (IOException e) {
            throw new RuntimeException("error creating temp file", e);
        }

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            IOUtils.copy(inputStream, outputStream);
        }

        long snapshotIdxLength = tempFile.length();
        long pos = 0;
        long currentSnapshotStateId = 0;

        try (InputStream is = new BufferedInputStream(new FileInputStream(tempFile))) {
            while (pos < snapshotIdxLength) {
                long nextGap = VarInt.readVLong(is);

                if (currentSnapshotStateId + nextGap > desiredVersion) {
                    if (currentSnapshotStateId == 0) {
                        return Long.MIN_VALUE;
                    }

                    return currentSnapshotStateId;
                }

                currentSnapshotStateId += nextGap;
                pos += VarInt.sizeOfVLong(nextGap);
            }

            if (currentSnapshotStateId != 0) {
                return currentSnapshotStateId;
            }
        } finally {
            tempFile.delete();
        }

        return Long.MIN_VALUE;
    }
}
