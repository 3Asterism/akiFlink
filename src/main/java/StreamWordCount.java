import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        //获取路径
        String filePath = "E:\\demo\\akiFlink\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputDataStream = env.readTextFile(filePath);
        //基于数据流进行转化计算
    }
}
