import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class P_Count {
    public static void main(String[] args) {

        try {
            BasicConfigurator.configure();
            long N;

            //-------------------------------------------------------------
            //       Split the Google Books Ngrams into 2 corpus
            //-------------------------------------------------------------

            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "split corpus");
            job1.setJarByClass(FileSpliter.class);

            job1.setMapperClass(FileSpliter.MapperClass.class);
            job1.setCombinerClass(FileSpliter.CombinerClass.class);
            job1.setPartitionerClass(FileSpliter.PartitionerClass.class);
            job1.setReducerClass(FileSpliter.ReducerClass.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(CorpusOccurrences.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job1,new Path("s3n://ass02/Step1"));
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);

            if (job1.waitForCompletion(true)){
                System.out.println("Split the corpus!");
            }
            Counters cs = job1.getCounters();
            Counter c = cs.findCounter(FileSpliter.ReducerClass.Counter.N_COUNTER);
            N = c.getValue();

            //-------------------------------------------------------------
            //                     Calculate Nr & Tr
            //-------------------------------------------------------------

            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "NrTr");
            job2.setJarByClass(NrTrMaker.class);

            job2.setMapperClass(NrTrMaker.MapperClass.class);
            job2.setPartitionerClass(NrTrMaker.PartitionerClass.class);
            job2.setReducerClass(NrTrMaker.ReducerClass.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Sum.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Sum.class);

            FileInputFormat.addInputPath(job2, new Path("s3n://ass02/Step1"));
            FileOutputFormat.setOutputPath(job2,new Path("s3n://ass02/Step2"));
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            if (job2.waitForCompletion(true)){
                System.out.println("Nr & Tr creation complete!");
            }

            //-------------------------------------------------------------
            //                    Join Trigram Nr Tr
            //-------------------------------------------------------------

            Configuration conf3 = new Configuration();
            Job job3 = Job.getInstance(conf3, "JoinCorpusData");
            job3.setJarByClass(JoinCorpusData.class);

            job3.setMapperClass(JoinCorpusData.MapperClass.class);
            job3.setPartitionerClass(JoinCorpusData.PartitionerClass.class);
            job3.setReducerClass(JoinCorpusData.ReducerClass.class);

            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job3, new Path("s3n://ass02/Step1"),TextInputFormat.class,JoinCorpusData.MapperClass.class);
            MultipleInputs.addInputPath(job3, new Path("s3n://ass02/Step2"),TextInputFormat.class,JoinCorpusData.MapperClass.class);
            FileOutputFormat.setOutputPath(job3,new Path("s3n://ass02/Step3"));
            job3.setOutputFormatClass(TextOutputFormat.class);
            if (job3.waitForCompletion(true)){
                System.out.println("Split the corpus!");
            }

            //-------------------------------------------------------------
            //                 Calculating the Probability
            //-------------------------------------------------------------

            Configuration conf4 = new Configuration();
            conf4.setLong("N",N);
            Job job4 = Job.getInstance(conf4, "probability");
            job4.setJarByClass(DeletedEstimation.class);

            job4.setMapperClass(DeletedEstimation.MapperClass.class);
            job4.setPartitionerClass(DeletedEstimation.PartitionerClass.class);
            job4.setReducerClass(DeletedEstimation.ReducerClass.class);

            job4.setMapOutputKeyClass(Text.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(DoubleWritable.class);

            MultipleInputs.addInputPath(job4, new Path("s3n://ass02/Step3"),TextInputFormat.class,DeletedEstimation.MapperClass.class);
            MultipleOutputs.addNamedOutput(job4,"probs",TextOutputFormat.class,Text.class,DoubleWritable.class);
            FileOutputFormat.setOutputPath(job4,new Path("s3n://ass02/Step4"));
            job4.setOutputFormatClass(TextOutputFormat.class);
            if (job4.waitForCompletion(true)){
                System.out.println("Calculate the Probability!");
            }

            //-------------------------------------------------------------
            //                 Rearrange The Result
            //-------------------------------------------------------------

            Configuration conf5 = new Configuration();
            Job job5 = Job.getInstance(conf5, "Final");
            job5.setJarByClass(ArrangingTheResult.class);

            job5.setMapperClass(ArrangingTheResult.MapperClass.class);
            job5.setPartitionerClass(ArrangingTheResult.PartitionerClass.class);
            job5.setReducerClass(ArrangingTheResult.ReducerClass.class);

            job5.setMapOutputKeyClass(Probability.class);
            job5.setMapOutputValueClass(Text.class);
            job5.setOutputKeyClass(Text.class);
            job5.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job5, new Path("s3n://ass02/Step4"),TextInputFormat.class,ArrangingTheResult.MapperClass.class);
            MultipleOutputs.addNamedOutput(job5,"Result",TextOutputFormat.class,Text.class,Text.class);
            FileOutputFormat.setOutputPath(job5,new Path(args[2]));
            job5.setOutputFormatClass(TextOutputFormat.class);
            if (job5.waitForCompletion(true)){
                System.out.println("Done!!");
            }

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
