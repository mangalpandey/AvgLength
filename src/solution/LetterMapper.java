package solution;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

/**
 * To define a map function for your MapReduce job, subclass the Mapper class
 * and override the map method. The class definition requires four parameters:
 * 
 * @param The
 *            data type of the input key - LongWritable
 * @param The
 *            data type of the input value - Text
 * @param The
 *            data type of the output key - Text
 * @param The
 *            data type of the output value - IntWritable
 */
public class LetterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	boolean caseSensitive = true;

	/**
	 * The map method runs once for each line of text in the input file. The
	 * method receives:
	 * 
	 * @param A
	 *            key of type LongWritable
	 * @param A
	 *            value of type Text
	 * @param A
	 *            Context object.
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object, to a String
		 * object.
		 */
		String line = value.toString();

		/*
		 * The line.split("\\W+") call uses regular expressions to split the
		 * line up by non-word characters. If you are not familiar with the use
		 * of regular expressions in Java code, search the web for
		 * "Java Regex Tutorial."
		 */
		System.out.println("My line"+ line+" ");
		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {

				/*
				 * Obtain the first letter of the word
				 */
				String letter;

				if (caseSensitive)
					letter = word.substring(0, 1);
				else
					letter = word.substring(0, 1).toLowerCase();
				System.out.println("My letter"+ letter+" ");
				/*
				 * Call the write method on the Context object to emit a key and
				 * a value from the map method. The key is the letter (in
				 * lower-case) that the word starts with; the value is the
				 * word's length.
				 */
				context.write(new Text(letter), new DoubleWritable(word.length()));
			}
		}
	}

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		caseSensitive = conf.getBoolean("caseSensitive", false);

	}

}