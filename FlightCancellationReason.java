
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

public class FlightCancellationReason {
    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration());
        job.setJarByClass(FlightCancellationReason.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(FlightCancellationMapper.class);
        job.setReducerClass(FlightCancellationReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}

class FlightCancellationMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] flightData = value.toString().split(",");
        if (flightData[21].equals("1") && !flightData[22].equals(null)) {
            Text cancelledFlight = new Text(flightData[22]);
            context.write(cancelledFlight, new Text("1"));
        }
    }
}

class FlightCancellationReducer extends Reducer<Text, Text, Text, Text> {

    private static TreeSet<CancelReason> cancelledFlights = new TreeSet<CancelReason>();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int cancelCount = 0;
        for (Text value : values) {
            cancelCount += Integer.parseInt(value.toString());
        }
        cancelledFlights.add(new CancelReason(key.toString(), cancelCount));
    }

    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        String cancelReasonCode = cancelledFlights.last().code;
        String message = "";
        message = (cancelReasonCode.equals("A")) ? "carrier" : ((cancelReasonCode.equals("B"))? "weather" : 
                ((cancelReasonCode.equals("C"))? "NAS" : ((cancelReasonCode.equals("D"))? "security" : null )));
        if(message!=null)
            context.write(new Text("The Most Common Reason For Cancellation: "+ message), null);
        else
            context.write(new Text("No Result Found"), null);

        context.write(new Text("----------------------------------------------------"), null);
        context.write(null, null);
    }
}


class CancelReason implements Comparable<CancelReason> {

    String code;
    int value;

    CancelReason(String code, int value) {
        this.code = code;
        this.value = value;
    }

    @Override
    public int compareTo(CancelReason element) {
        return (this.value >= element.value) ? 1 : -1;
    }
}
