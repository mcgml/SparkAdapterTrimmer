package nhs.genetics.cardiff;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bdgenomics.adam.io.SingleFastqInputFormat;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSink;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadsWriteFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Programme for trimming FASTQs of adapters and converting to uBAM
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2017-06-01
 */

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);
    private static final String VERSION = "1.0.0";
    private static final String PROGRAM = "SparkAdapterTrimmer";

    public static void main(String[] args) {

        CommandLineParser commandLineParser = new BasicParser();
        CommandLine commandLine = null;
        HelpFormatter formatter = new HelpFormatter();
        Options options = new Options();

        //IO
        options.addOption("1", "Read1", true, "Path to input read1 fastq");
        options.addOption("2", "Read2", true, "Path to input read2 fastq");
        options.addOption("A", "Adapter1", true, "First read adapter sequence to trim");
        options.addOption("B", "Adapter2", true, "Second read adapter sequence to trim");
        options.addOption("O", "Output", true, "Path to output SAM/BAM file");
        options.addOption("T", "Threads", true, "Number of threads");

        //RG
        options.addOption("I", "Identifier", true, "RG identifier");
        options.addOption("S", "Sample", true, "RG sample identifier");
        options.addOption("L", "Library", true, "RG library identifier");
        options.addOption("P", "Platform", true, "RG platform");
        options.addOption("U", "Unit", true, "RG platform unit");
        options.addOption("C", "Centre", true, "RG sequencing centre");

        try {
            commandLine = commandLineParser.parse(options, args);

            if (!commandLine.hasOption("1") || !commandLine.hasOption("2") || ! commandLine.hasOption("A") || ! commandLine.hasOption("B") ||! commandLine.hasOption("O")){
                throw new NullPointerException("Check arguments");
            }

        } catch (ParseException | NullPointerException e){
            formatter.printHelp(PROGRAM + " " + VERSION, options);
            logger.error(e.getMessage());
            System.exit(-1);
        }

        //define sam headers
        SAMFileHeader samFileHeader = new SAMFileHeader();
        samFileHeader.setSortOrder(SAMFileHeader.SortOrder.queryname);

        //create read group
        SAMReadGroupRecord samReadGroupRecord = new SAMReadGroupRecord(commandLine.getOptionValue("I"));
        samReadGroupRecord.setSample(commandLine.getOptionValue("S"));
        samReadGroupRecord.setLibrary(commandLine.getOptionValue("L"));
        samReadGroupRecord.setPlatform(commandLine.getOptionValue("P"));
        samReadGroupRecord.setPlatformUnit(commandLine.getOptionValue("U"));
        samReadGroupRecord.setSequencingCenter(commandLine.getOptionValue("C"));

        //add read group to header
        samFileHeader.addReadGroup(samReadGroupRecord);

        //start spark
        int partitions = Integer.parseInt(commandLine.getOptionValue("T"));
        SparkConf sparkConf = new SparkConf().setAppName(Main.PROGRAM).setMaster("local[" + commandLine.getOptionValue("T") + "]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //read fastq
        JavaPairRDD<Void, Text> read1Raw = javaSparkContext.newAPIHadoopFile(commandLine.getOptionValue("1"), SingleFastqInputFormat.class, Void.class, Text.class, new Configuration());
        JavaPairRDD<Void, Text> read2Raw = javaSparkContext.newAPIHadoopFile(commandLine.getOptionValue("2"), SingleFastqInputFormat.class, Void.class, Text.class, new Configuration());

        //map to GATKRead and trim adapters
        JavaRDD<GATKRead> read1Trimmed = read1Raw.values().map(new TrimReadsAndConvertToSAMSpark(samFileHeader, commandLine.getOptionValue("A"), true));
        JavaRDD<GATKRead> read2Trimmed = read2Raw.values().map(new TrimReadsAndConvertToSAMSpark(samFileHeader, commandLine.getOptionValue("B"), false));

        //combine reads
        JavaRDD<GATKRead> readsTrimmed = read1Trimmed.union(read2Trimmed);

        //sort reads by queryname
        JavaRDD<GATKRead> readsSorted = readsTrimmed.sortBy(GATKRead::getName, true, 3);

        //write to uBAM
        try {
            ReadsSparkSink.writeReads(javaSparkContext, commandLine.getOptionValue("O"), null, readsSorted, samFileHeader, ReadsWriteFormat.SINGLE);
        } catch (IOException e){
            logger.error(e.getMessage());
        }

    }

}