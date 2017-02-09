package how.hollow.producer.util;

import static java.lang.System.getProperty;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ScratchPaths {

    public static final Path TEMP = Paths.get(getProperty("java.io.tmpdir"));

    public static Path makeProductDir(String namespace) {
        return makeScratchDir(namespace, "temp");
    }

    public static Path makePublishDir(String namespace) {
        return makeScratchDir(namespace, "published");
    }

    public static Path makeScratchDir(String namespace, String name) {
        try {
            Path dir = TEMP.resolve(namespace).resolve(name);
            Files.createDirectories(dir);
            return dir;
        } catch(IOException ex) {
            throw new RuntimeException(ex);
        }
    }

}
