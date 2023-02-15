package bigdataman.burani.Gasem;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

public class GasemReducer extends Reducer<Text, DoubleWritable, BSONWritable, DoubleWritable>{

	private BSONWritable reduceResult = new BSONWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, BSONWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String param = conf.get("param");
		String[] args = param.split(",");
		String str;
		str = args[0];
		
		double result = 0;
		double sum = 0;
		int count = 0;
		
		for (DoubleWritable val : values) {
			sum += val.get();
			count ++;
		}
		
		result = sum;
		
		if(args[args.length-1].equals("avg")) {
			result = sum / count;
		}
		
		BSONObject outDoc = BasicDBObjectBuilder
				.start()
				.add(str, key.toString())
				.get();
		this.reduceResult.setDoc(outDoc);
		context.write(this.reduceResult, new DoubleWritable(result));
	}
}

