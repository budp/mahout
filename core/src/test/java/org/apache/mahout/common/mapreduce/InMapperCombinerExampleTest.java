package org.apache.mahout.common.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class InMapperCombinerExampleTest {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class WordCountMapperWithInMapperCombiner extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable one = new LongWritable(1);
        private final Text word = new Text();
        private final InMapperCombiner combiner = new InMapperCombiner<Text, LongWritable>(
                new CombiningFunction.LongWritableSum()
        );

        @Override
        @SuppressWarnings("unchecked")
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                combiner.write(word, one, context);
            }
        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            combiner.flush(context);
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    @Test
    public void testWordCountMapper() throws Exception {
        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(new WordCountMapper());

        // The ordering does matter, and it's the same as the emitting ordering.
        mapDriver.withInput(new LongWritable(1), new Text("Apple Walnut Avocado Avocado Walnut Apple Avocado"));

        mapDriver.withOutput(new Text("Apple"), new LongWritable(1));
        mapDriver.withOutput(new Text("Walnut"), new LongWritable(1));
        mapDriver.withOutput(new Text("Avocado"), new LongWritable(1));
        mapDriver.withOutput(new Text("Avocado"), new LongWritable(1));
        mapDriver.withOutput(new Text("Walnut"), new LongWritable(1));
        mapDriver.withOutput(new Text("Apple"), new LongWritable(1));
        mapDriver.withOutput(new Text("Avocado"), new LongWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testWordCountMapperWithInMapperCombiner() throws Exception {
        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = new MapDriver<LongWritable, Text, Text, LongWritable>();
        mapDriver.setMapper(new WordCountMapperWithInMapperCombiner());

        mapDriver.withInput(new LongWritable(1), new Text("Apple Walnut Avocado Avocado Walnut Apple Avocado"));

        mapDriver.withOutput(new Text("Walnut"), new LongWritable(2));
        mapDriver.withOutput(new Text("Apple"), new LongWritable(2));
        mapDriver.withOutput(new Text("Avocado"), new LongWritable(3));

        mapDriver.runTest();
    }

    @Test
    public void testWordCountReducer() throws Exception {
        ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
        reduceDriver.setReducer(new WordCountReducer());

        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(3));
        values.add(new LongWritable(7));
        values.add(new LongWritable(11));

        reduceDriver.withInput(new Text("Apple"), values);

        reduceDriver.withOutput(new Text("Apple"), new LongWritable(21));

        reduceDriver.runTest();
    }

    @Test
    public void testWordCountMR() throws Exception {
        MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new WordCountMapper());
        mapReduceDriver.setReducer(new WordCountReducer());
        // mapReduceDriver.setCombiner(new WordCountMR.WordCountReducer());

        mapReduceDriver.withInput(new LongWritable(1), new Text("Apple Walnut Avocado Avocado Walnut Apple Avocado"));

        // Notice that the key is sorted in the following example.
        mapReduceDriver.withOutput(new Text("Apple"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("Avocado"), new LongWritable(3));
        mapReduceDriver.withOutput(new Text("Walnut"), new LongWritable(2));

        mapReduceDriver.runTest();
    }

    @Test
    public void testWordCountMRWithInMapperCombiner() throws Exception {
        MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable> mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
        mapReduceDriver.setMapper(new WordCountMapperWithInMapperCombiner());
        mapReduceDriver.setReducer(new WordCountReducer());
        // mapReduceDriver.setCombiner(new WordCountMR.WordCountReducer());

        mapReduceDriver.withInput(new LongWritable(1), new Text("Apple Walnut Avocado Avocado Walnut Apple Avocado"));

        // Notice that the key is sorted in the following example.
        mapReduceDriver.withOutput(new Text("Apple"), new LongWritable(2));
        mapReduceDriver.withOutput(new Text("Avocado"), new LongWritable(3));
        mapReduceDriver.withOutput(new Text("Walnut"), new LongWritable(2));

        mapReduceDriver.runTest();
    }
}
