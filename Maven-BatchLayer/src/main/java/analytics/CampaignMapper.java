package analytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CampaignMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	int budget, ageMax, ageMin;
	
	String productCategory,gender, city;
	
	public void configure(JobConf job) {
		productCategory = job.get("productCategory");
		ageMax=Integer.parseInt(job.get("ageMax"));
		ageMin=Integer.parseInt(job.get("ageMin"));
		gender=job.get("gender");
		budget=Integer.parseInt(job.get("budget"));
		city=job.get("city");
	}

	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> mapper_output, Reporter reporter)
			throws IOException {
			
			String line = value.toString();
			String [] adCampaignRecord= line.split(",");
			
			
			if( (   adCampaignRecord[11].substring(1, adCampaignRecord[11].length()-1).equalsIgnoreCase(productCategory) ) && 
				(  (Integer.parseInt(adCampaignRecord[7].substring(1, adCampaignRecord[7].length()-1))) <= budget ) &&
				(   adCampaignRecord[10].substring(1, adCampaignRecord[10].length()-1).equalsIgnoreCase(gender)  )  &&
				(   Integer.parseInt(adCampaignRecord[9].substring(1, adCampaignRecord[9].length()-1)) >= ageMin  ) &&
				(   Integer.parseInt(adCampaignRecord[8].substring(1, adCampaignRecord[8].length()-1)) <= ageMax  ) &&
				(   adCampaignRecord[13].substring(1, adCampaignRecord[13].length()-1).equalsIgnoreCase(city) )  &&
				(    Integer.parseInt(adCampaignRecord[12].substring(1, adCampaignRecord[12].length()-1)) >= 5  ) )
			
			{
				System.out.println("PC:"+productCategory+" Budget:"+budget+" Gender:"+gender+" ageMin:"+ageMin+" ageMax:"+ageMax+" city:"+city);
				
				mapper_output.collect(new Text(adCampaignRecord[1].substring(1, adCampaignRecord[1].length()-1)), one);
	
			}
	}
	

}


