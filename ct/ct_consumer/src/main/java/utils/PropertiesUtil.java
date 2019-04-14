package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Create by fengqijie
 * 2019/3/2 14:41
 */
public class PropertiesUtil {

    public static Properties properties = null;

    static {
        InputStream is = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return (String)properties.get(key);
    }


}

















