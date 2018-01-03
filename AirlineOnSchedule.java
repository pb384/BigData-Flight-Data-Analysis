import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AirlineOnSchedule {

    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(AirlineOnSchedule.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(AirlineOnScheduleMapper.class);
        job.setReducerClass(AirlineOnScheduleReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }
}

class AirlineOnScheduleMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowValues = value.toString().split(",");
        String uniqueCarrier = rowValues[8];
        if (!rowValues[14].equals("NA")) {
            if (Integer.parseInt(rowValues[14]) <= 10) {
                context.write(new Text(uniqueCarrier), new Text("1"));
            } else {
                context.write(new Text(uniqueCarrier), new Text("0"));
            }
        }
    }
}

class AirlineOnScheduleReducer extends Reducer<Text, Text, Text, Text> {

    private static TreeSet<KeyCount> reduceKeySet = new TreeSet<KeyCount>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int noOfOccurances = 0;
        int onScheduleOccurances = 0;
        for (Text value : values) {
            if (Integer.parseInt(value.toString()) == 1) {
                onScheduleOccurances++;
            }
            noOfOccurances++;
        }
        double probability = (double) onScheduleOccurances / (double) noOfOccurances;
        reduceKeySet.add(new KeyCount(key.toString(), probability));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int displayCount = 0;
        Iterator descendingIterator = reduceKeySet.descendingIterator();
        context.write(new Text("3 Airlines With Highest Probability For Being On Schedule:"), null);
        
        while (descendingIterator.hasNext()) {
		if (displayCount < 3) {
                KeyCount finalKeyCount = (KeyCount) descendingIterator.next();
                context.write(new Text(finalKeyCount.key), new Text(String.valueOf(finalKeyCount.count)));
                displayCount++;
            } else 
		break;
        }
        context.write(new Text("----------------------------------------------------"), null);
        context.write(null, null);

        context.write(new Text("3 Airlines With Lowest Probability For Being On Schedule:"), null);
        Iterator ascendingIterator = reduceKeySet.iterator();
        
            while (ascendingIterator.hasNext()) {
		if (displayCount > 0) {
                KeyCount finalKeyCount = (KeyCount) ascendingIterator.next();
                context.write(new Text(finalKeyCount.key), new Text(String.valueOf(finalKeyCount.count)));
                displayCount--;
            } else 
		break;
        }

        context.write(new Text("----------------------------------------------------"), null);
        context.write(null, null);
    }

    class KeyCount implements Comparable<KeyCount> {

        String key;
        double count;

        KeyCount(String key, double count) {
            this.key = key;
            this.count = count;
        }

        @Override
        public int compareTo(KeyCount o) {
        	return (this.count >= o.count) ? 1 : -1;
        }
    }
}
