package kafka;

import hbase.HBaseDAO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;

/**
 * Create by fengqijie
 * 2019/3/2 14:33
 */
public class HBaseConsumer {


    public static final String kafkaTopics = "kafka.topics";


    public static void main(String[] args) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty(kafkaTopics)));

        HBaseDAO hd = new HBaseDAO();
        int i = 1;
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> cr : records) {
                String oriValue = cr.value();
                System.out.println(i + "   " + oriValue);
                i++;
                hd.put(oriValue);
            }
        }

    }

}
