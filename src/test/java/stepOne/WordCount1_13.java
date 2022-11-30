package stepOne;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount1_13 {
    public static void main(String[] args) throws Exception {
        //执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> stringDataSource = env.readTextFile("E:\\demo\\akiFlink\\src\\main\\resources\\hello.txt");
        //处理逻辑
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = stringDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            // 将每个单词转换为二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //按照word进行分组
        wordAndOne.print();
        System.out.println("------结束1------");
        UnsortedGrouping<Tuple2<String, Long>> sum1 = wordAndOne.groupBy(0);
        //分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = sum1.sum(1);
        sum.print();
        System.out.println("------结束2------");
        wordAndOne.groupBy(0).sum(1).print();
        System.out.println("------结束3------");
    }
}
