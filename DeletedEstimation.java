import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DeletedEstimation {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String [] arr = line.toString().split("\\s+");
            if(arr.length == 5){
                //0-2 trigram 3 Nr 4 Tr
                context.write(new Text(arr[0]+" "+arr[1]+" "+arr[2]), new Text(arr[3]+" "+arr[4]));
            }
            else {
                System.out.println("problem in the mapper of DeletedEstimation - incorrect number of words");
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text, DoubleWritable> {
        private MultipleOutputs m;
        private double Nr1;
        private double Nr2;
        private double Tr1;
        private double Tr2;
        private String t;
        private double N;

        public void setup(Context context){
            m= new MultipleOutputs(context);
            t="";
            Nr1=1;
            Nr2=1;
            Tr1=0;
            Tr2=0;
            N= (double) context.getConfiguration().getLong("N",1);
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
                String [] arr = value.toString().split("\\s+");
                if(!key.toString().equals(t)){
                    t=key.toString();
                    Nr1=Double.parseDouble(arr[0]);
                    Tr1=Float.parseFloat(arr[1]);
                }
                else{
                    Nr2=Double.parseDouble(arr[0]);
                    Tr2=Double.parseDouble(arr[1]);
                    double prob= ((Tr1+Tr2)/((N)*(Nr1+Nr2)));
                    m.write("probs",t,new DoubleWritable(prob));
                }
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

    public static class PartitionerClass extends Partitioner<Text,Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
