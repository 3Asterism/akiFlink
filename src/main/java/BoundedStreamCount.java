import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundedStreamCount {
    public static void main(String[] args) throws Exception {
        //1.创建流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("E:\\demo\\akiFlink\\src\\main\\resources\\hello.txt");
        //3.转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        wordAndOne.keyBy(data -> data.f0).sum(1).print();
        //启动执行
        env.execute();
    }
}
