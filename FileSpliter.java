import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileSpliter {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, CorpusOccurrences> {
        private static Pattern Hebrew = Pattern.compile("(?<trigram>[א-ת]+ [א-ת]+ [א-ת]+)\\t\\d{4}\\t(?<occurrences>\\d+).*");
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            Matcher m = Hebrew.matcher(line.toString());
            if (m.matches()) {
                //0-2 trigram 3 year 4 occurrences 5 pages 6 books
                context.write(new Text(m.group("trigram")), new CorpusOccurrences((lineId.get() % 2 == 0), Long.parseLong(m.group("occurrences"))));
            }
        }
    }

    public static class CombinerClass extends Reducer<Text,CorpusOccurrences,Text,CorpusOccurrences> {
        private long r1;
        private long r2;
        private String t;

        public void setup(Context context){
            t="";
            r1=0;
            r2=0;
        }
        public void reduce(Text key, Iterable<CorpusOccurrences> values, Context context) throws IOException,  InterruptedException {
            for (CorpusOccurrences value : values) {
                if(!key.toString().equals(t)){
                    t=key.toString();
                    r1=0;
                    r2=0;
                }
                if (value.getCorpus()){
                    r1=r1+value.getOcc();
                }
                else{
                    r2=r2+value.getOcc();
                }
            }
            context.write(new Text(t),new CorpusOccurrences(true,r1));
            context.write(new Text(t),new CorpusOccurrences(false,r2));
        }
        public void cleanup(Context context)  {}
    }

    public static class ReducerClass extends Reducer<Text,CorpusOccurrences,Text,Text> {
        enum Counter{
            N_COUNTER
        }
        private long r1;
        private long r2;
        private String t;

        public void setup(Context context){
            t="";
            r1=0;
            r2=0;
        }
        public void reduce(Text key, Iterable<CorpusOccurrences> values, Context context) throws IOException,  InterruptedException {
            for (CorpusOccurrences value : values) {
                context.getCounter(Counter.N_COUNTER).increment(value.getOcc());
                if(!key.toString().equals(t)){
                    t=key.toString();
                    r1=0;
                    r2=0;
                }
                if (value.getCorpus()){
                    r1=r1+value.getOcc();
                }
                else{
                    r2=r2+value.getOcc();
                }
            }
            context.write(new Text(t), new Text(r1+" "+r2));
        }
        public void cleanup(Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<Text,CorpusOccurrences> {
        public int getPartition(Text key, CorpusOccurrences value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

}
