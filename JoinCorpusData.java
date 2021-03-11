import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class JoinCorpusData {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            //Corpus : 0-2 words 3 myR 4 otherR
            //NrTr: 0 myR 1 corpus  2 Nr 3 Tr
            String [] arr = line.toString().split("\\s+");
            if(arr.length == 5){
                context.write(new Text(arr[3]+"_1"), new Text("1 "+arr[0]+" "+arr[1]+" "+arr[2]));
                context.write(new Text(arr[4]+"_1"), new Text("2 "+arr[0]+" "+arr[1]+" "+arr[2]));
            }
            else if(arr.length == 4){
                context.write(new Text(arr[0]+"_0"), new Text(arr[1]+" "+arr[2]+" "+arr[3]));
            }
            else {
                System.out.println("problem in the mapper of NrTrMaker - incorrect number of words");
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text,Text,Text> {
        private String NrTr_corpus1;
        private String NrTr_corpus2;
        private String r;

        public void setup(Context context){
            NrTr_corpus1="0 0";
            NrTr_corpus2="0 0";
            r="";
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
                String [] arr = value.toString().split("\\s+");
                int length= key.toString().length();
                String thisR= key.toString().substring(0,length-2);
                if(!r.equals(thisR)){
                    r= thisR;
                    NrTr_corpus1="0 0";
                    NrTr_corpus2="0 0";
                }
                if(arr.length == 3){
                    if(Integer.parseInt(arr[0])==1)
                    {
                        NrTr_corpus1= arr[1]+" "+arr[2];
                    }
                    else
                    {
                        NrTr_corpus2= arr[1]+" "+arr[2];
                    }
                }
                if(arr.length == 4){
                    if ( Integer.parseInt(arr[0])==1){
                        context.write(new Text(arr[1]+" "+arr[2]+" "+arr[3]),new Text(NrTr_corpus1));
                    }
                    else{
                        context.write(new Text(arr[1]+" "+arr[2]+" "+arr[3]),new Text(NrTr_corpus2));
                    }
                }
            }
        }

        public void cleanup (Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<Text,Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
}
