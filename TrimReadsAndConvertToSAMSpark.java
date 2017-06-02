package nhs.genetics.cardiff;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMRecordToGATKReadAdapter;

/**
 * Class for trimming read and converting to SAMRecord
 *
 * @author  Matt Lyon
 * @version 1.0
 * @since   2017-06-01
 */

public class TrimReadsAndConvertToSAMSpark implements Function<Text, GATKRead> {

    private SAMFileHeader samFileHeader;
    private String adapter;
    private Boolean firstOfPairFlag;

    public TrimReadsAndConvertToSAMSpark(SAMFileHeader samFileHeader, String adapter, Boolean firstOfPairFlag){
        this.samFileHeader = samFileHeader;
        this.adapter = adapter;
        this.firstOfPairFlag = firstOfPairFlag;
    }

    @Override
    public GATKRead call(Text text) {
        String[] s = text.toString().split("\n");

        //trim header
        String header = s[0].split(" ")[0].replaceAll("^@", "");

        //find adapter
        Integer index = getAdapterIndex(s[1], adapter);

        SAMRecord samRecord = new SAMRecord(samFileHeader);
        samRecord.setReadName(header);

        //trim adapter
        if (index > 35) {
            samRecord.setReadString(s[1].substring(0, index));
            samRecord.setBaseQualityString(s[3].substring(0, index));
        } else if (index > -1) {
            samRecord.setReadString("NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN");
            samRecord.setBaseQualityString("2222222222222222222222222222222222");
        } else {
            samRecord.setReadString(s[1]);
            samRecord.setBaseQualityString(s[3]);
        }

        //set RG
        samRecord.setAttribute("RG", samFileHeader.getReadGroups().get(0).getId());

        //set flags
        samRecord.setReadPairedFlag(true);
        samRecord.setReadUnmappedFlag(true);
        samRecord.setMateUnmappedFlag(true);

        if (firstOfPairFlag) {
            samRecord.setFirstOfPairFlag(true);
        }

        if (!firstOfPairFlag) {
            samRecord.setSecondOfPairFlag(true);
        }

        return SAMRecordToGATKReadAdapter.headerlessReadAdapter(samRecord);
    }

    private int getAdapterIndex(String seq, String query){
        return seq.lastIndexOf(query);
    }
}
