package itba.pod.client.utils;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

public class GetPropertyValues {
    InputStream inputStream;
    Properties prop;

    public GetPropertyValues() {
        try {
            prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
            inputStream.close();
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }

    public String getPropValue(String key) {
        return prop.getProperty(key);
    }
}
