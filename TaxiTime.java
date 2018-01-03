import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

public class TaxiTime {

    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(TaxiTime.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TaxiTimeMapper.class);
        job.setReducerClass(TaxiTimeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

}

class TaxiTimeMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] flightData = value.toString().split(",");
        if (!flightData[16].equals("NA") && !flightData[20].equals("NA")) {
            Text originAirport = new Text(flightData[16]);
            Text taxiOut = new Text(flightData[20]);
            context.write(originAirport, taxiOut);
        }
        if (!flightData[17].equals("NA") && !flightData[19].equals("NA")) {
            Text destAirport = new Text(flightData[17]);
            Text taxiIn = new Text(flightData[19]);
            context.write(destAirport, taxiIn);
        }
    }
}

class TaxiTimeReducer extends Reducer<Text, Text, Text, Text> {

    private static TreeSet<TaxiTimeAirport> avgTaxiTimeAirport = new TreeSet<TaxiTimeAirport>();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        float taxiTime = 0, total = 0;
        for (Text value : values) {
            total++;
            taxiTime += Float.parseFloat(value.toString());
        }
        float averageTaxiTime = taxiTime / total;
        avgTaxiTimeAirport.add(new TaxiTimeAirport(key.toString(), averageTaxiTime));
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        int displayCount = 0;
        Iterator descendingIterator = avgTaxiTimeAirport.descendingIterator();
        context.write(new Text("3 Airports With the Longest Average Taxi Time Per Flight:"), null);
        while (descendingIterator.hasNext()) {
            if (displayCount < 3) {
                TaxiTimeAirport taxiTimeAirport = (TaxiTimeAirport) descendingIterator.next();
                context.write(new Text(taxiTimeAirport.key), new Text(String.valueOf(taxiTimeAirport.value)));
                displayCount++;
            } else 
                break;
        }
        context.write(new Text("----------------------------------------------------"), null);
        context.write(null, null);
        context.write(new Text("3 Airports With the Shortest Average Taxi Time Per Flight:"), null);
        Iterator ascendingIterator = avgTaxiTimeAirport.iterator();

        while (ascendingIterator.hasNext()) {
            if (displayCount > 0) {
                TaxiTimeAirport taxiTimeAirport = (TaxiTimeAirport) ascendingIterator.next();
                context.write(new Text(taxiTimeAirport.key), new Text(String.valueOf(taxiTimeAirport.value)));
                displayCount--;
            } else 
                break;
        }
        context.write(new Text("----------------------------------------------------"), null);
        context.write(null, null);
    }
}

class TaxiTimeAirport implements Comparable<TaxiTimeAirport> {

    String key;
    float value;

    TaxiTimeAirport(String key, float value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(TaxiTimeAirport element) {
        return (this.value >= element.value) ? 1 : -1;
    }
}
