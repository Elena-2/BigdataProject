package bigdataman.burani.Gasem;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoConfig;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;

public class Tools extends MongoTool{
	public Tools(String s, String collectionName){		
		JobConf conf = new JobConf(new Configuration());
		conf.set("param", s);

		MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
        MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);

		MongoConfig config = new MongoConfig(conf);

		config.setMapper(GasemMapper.class);
		config.setReducer(GasemReducer.class);		
		config.setMapperOutputKey(Text.class);
		config.setMapperOutputValue(DoubleWritable.class);
		config.setOutputKey(BSONWritable.class);
		config.setOutputValue(DoubleWritable.class);
		config.setInputURI("mongodb://localhost:27017/"+Gasem.getDBName()+"."+Gasem.getDBName());
		config.setOutputURI("mongodb://localhost:27017/"+Gasem.getDBName()+".MapRed_" + collectionName);
		setConf(conf);
		
	}
}
