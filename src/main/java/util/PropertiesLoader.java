package util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class PropertiesLoader {
    public static Properties load(String path) {
        Properties properties = new Properties();
        if (path == null) {
            return properties;
        }
        try (var in = Files.newInputStream(Path.of(path))) {
            properties.load(in);
        } catch (IOException e) {
            System.out.println("Could not read config file " + path + ": " + e.getMessage());
        }
        return properties;
    }
}
