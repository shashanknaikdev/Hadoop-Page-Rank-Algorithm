import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PageRank {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text word1 = new Text();
		private Text word2 = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer_input = new StringTokenizer(line, " \t "); // breaking the line into tokens
			int numberOfTokens = tokenizer_input.countTokens(); // getting number of tokens so that I can store them using for loop
			String[] arrayOfTokens = new String[numberOfTokens]; // initializing the array of tokens
			for (int i = 0; i < numberOfTokens; i++) {
				arrayOfTokens[i] = tokenizer_input.nextToken(); // inserting the tokens in the String array
			}

			int numberOfOutlinks = arrayOfTokens.length - 2; // number of outlinks = total tokens - 1(source token) -1(PR value)
			String Source = arrayOfTokens[0]; // source is 1st token
			String PR = arrayOfTokens[arrayOfTokens.length - 1]; // PR is the last index
			// System.out.println(numberOfOutlinks+" "+Source+" "+PR);
			float PRfloat = Float.parseFloat(PR) / numberOfOutlinks;

			for (int j = 1; j <= numberOfOutlinks; j++) // for every outlink, we need to send o/p to reduce
			{
				word1.set(arrayOfTokens[j]); // setting word 1 as the outlink
				word2.set(Source + " " + PRfloat); // setting word 2 as the source, which will be same for all the outlinks
				//System.out.println("I am inside the for loop....this is word 1 "+ word1 + " this is word 2 " + word2);
				output.collect(word1, word2);

			}

			word1.set(Source);
			String temp = "";
			for (int j = 1; j <= numberOfOutlinks; j++) {
				if (j == numberOfOutlinks) // checking to make sure I don't add unnecessary space at the end, if it is the last outlink
				{
					temp = temp + arrayOfTokens[j];
				} else {
					temp = temp + arrayOfTokens[j] + " ";
				}

			}

			word2.set(temp);
			//System.out.println("This is the last line of Map Output....this is word 1 "+ word1 + " this is word 2 " + word2);
			output.collect(word1, word2);

		}

	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			final String regex_Dec = "^-?\\d*\\.\\d+$"; //initializing the
			final String regex_Int = "^-?\\d+$";			//regular expressions for Dec or Int
			final String regex_comp = regex_Dec + "|" + regex_Int;
			float PRsum = 0; 	//initializing sum
			String Pages = "";
			while (values.hasNext()) {
				String pageRank = values.next().toString();
				String[] pagesWithRank = pageRank.split(" "); //split on space
				String[] init_Pages = {};
				if (Pages.length() > 0)
					init_Pages = Pages.split(" ");
				String last = pagesWithRank[pagesWithRank.length - 1].trim();
				Float PR = (float) 0.0;
				if (Pattern.matches(regex_comp, last)) {
					PR = Float.valueOf(last);
					PRsum += PR;
					if (init_Pages.length > 0 && Pattern.matches(regex_comp,
									init_Pages[init_Pages.length - 1]))
								Pages = Pages.replace(
								init_Pages[init_Pages.length - 1],
								String.valueOf(PRsum));
					else
						Pages = Pages.concat(String.valueOf(PRsum));
				} else {
					Pages = "";
					for (int i = 0; i < pagesWithRank.length; i++)
						Pages = Pages.concat(pagesWithRank[i]
								+ " ");
					Pages = Pages.concat(String.valueOf(PRsum));
				}

			}
			output.collect(key, new Text(Pages));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PageRank.class);
		conf.setJobName("MyPageRank");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	

		JobClient.runJob(conf);
	}
}
