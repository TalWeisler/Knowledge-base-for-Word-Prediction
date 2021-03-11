import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ArrangingTheResult {
    public static class MapperClass extends Mapper<LongWritable, Text, Probability, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] arr = line.toString().split("\\s+");
            if(arr.length == 4){
                //0-2 trigram 3 probability
                context.write(new Probability(arr[0],arr[1],Double.parseDouble(arr[3])), new Text(arr[2]));
            }
            else {
                System.out.println("problem in the mapper of ArrangingTheResult - incorrect number of words");
            }
        }
    }

    public static class ReducerClass extends Reducer<Probability,Text,Text, Text> {
        private MultipleOutputs m;

        public void setup(Context context){
            m= new MultipleOutputs(context);
        }
        public void reduce(Probability key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
               m.write("Result",new Text(key.getTwoWords()+" "+value.toString()),new Text(key.getProbabilityString()));
            }
        }
        public void cleanup(Context context)  {
            try {
                m.close();
            } catch (IOException | InterruptedException e) {
                System.out.println("Problem in the reduce of trigramSpliter");
                e.printStackTrace();
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Probability,Text> {
        public int getPartition(Probability key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
