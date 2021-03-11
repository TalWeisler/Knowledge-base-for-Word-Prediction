import com.amazonaws.services.dynamodbv2.xspec.S;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class NrTrMaker {
    public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Sum> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            //0-2 words 3 myR 4 otherR
            String [] arr = line.toString().split("\\s+");
            if(arr.length == 5){
               context.write(new LongWritable(Long.parseLong(arr[3])), new Sum(1,1,Long.parseLong(arr[4])));
               context.write(new LongWritable(Long.parseLong(arr[4])), new Sum(2,1,Long.parseLong(arr[3])));
            }
            else {
                System.out.println("problem in the mapper of NrTrMaker - incorrect number of words");
            }
        }
    }

    public static class ReducerClass extends Reducer<LongWritable, Sum,LongWritable,Sum> {
        private long Nr_corpus1;
        private long Tr_corpus1;
        private long Nr_corpus2;
        private long Tr_corpus2;
        private long r;

        public void setup(Context context){
            Nr_corpus1=0;
            Tr_corpus1=0;
            Nr_corpus2=0;
            Tr_corpus2=0;
            r=-1;
        }
        public void reduce(LongWritable key, Iterable<Sum> values, Context context) throws IOException,  InterruptedException {
            for (Sum value : values) {
                if(r!=key.get()){
                    r= key.get();
                    Nr_corpus1=0;
                    Tr_corpus1=0;
                    Nr_corpus2=0;
                    Tr_corpus2=0;
                }
                if (value.getCorpus()==1){
                    Nr_corpus1=Nr_corpus1+value.getMyR();
                    Tr_corpus1=Tr_corpus1+value.getOtherR();
                }
                else{
                    Nr_corpus2=Nr_corpus2+value.getMyR();;
                    Tr_corpus2=Tr_corpus2+value.getOtherR();
                }
            }
            context.write(new LongWritable(r), new Sum(1,Nr_corpus1,Tr_corpus1));
            context.write(new LongWritable(r), new Sum(2,Nr_corpus2,Tr_corpus2));
        }

        public void cleanup (Context context)  {}
    }

    public static class PartitionerClass extends Partitioner<LongWritable,Sum> {
        public int getPartition(LongWritable key, Sum value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
