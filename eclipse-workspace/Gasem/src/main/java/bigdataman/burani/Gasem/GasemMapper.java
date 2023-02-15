package bigdataman.burani.Gasem;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.bson.BSONObject;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;

public class GasemMapper extends Mapper<Object, BSONObject, Text, DoubleWritable>{

	private DoubleWritable count = new DoubleWritable(1);

	public void map(Object key, BSONObject val, Mapper<Object, BSONObject, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException{
		
		Configuration conf = context.getConfiguration();
		String param = conf.get("param");
		String[] args = param.split(",");
		String[] docVal= new String[1];
		boolean emit = true;
		
		
		//SELECT key, SUM(Global_Index)/AVG(Global_Index)
		if(args[args.length-1].equals("sum") || args[args.length-1].equals("avg")) {
			this.count = new DoubleWritable(Double.parseDouble(((BSONObject)val).get("Value").toString()));
			args = (String[])ArrayUtils.subarray(args, 0, args.length-1);
		}
		
		//WHERE <key=value>
		if(args.length>1) {
			
			//OR
			if(args[1].equals("or")) {
				
				emit=false;
				docVal = new String[args.length-2];
				
				for(int i = 0; i< docVal.length;i++) {
					docVal[i] = (String)((BSONObject)val).get(args[i+2].split("=")[0]);
				}
				
				int ecount=0;
				for (int j = 0; j<docVal.length;j++) {
					if(docVal[j].equals(args[j+2].split("=")[1]))
						ecount++;
				}
				if (ecount>0)
					emit=true;
			}
			
			//AND
			else {
				docVal = new String[args.length-1];
				for(int i = 0; i< docVal.length;i++) {
					docVal[i] = (String)((BSONObject)val).get(args[i+1].split("=")[0]);
				}
			
				for (int k = 0; k<docVal.length;k++) {
					if(docVal[k]==null || !docVal[k].equals(args[k+1].split("=")[1])){
						emit=false;
					}
				}
			}
				
		}
		
		String newKey = "";
		if (args[0].split("-").length>1) {
			String[] keys = args[0].split("-");
			newKey = (String)val.get(keys[0])+"-"+(String)val.get(keys[1]);
		}
		else
			newKey = (String)val.get(args[0]);
		
		if (emit) {
			context.write(new Text(newKey), this.count);
		}
	}
}

