import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorpusOccurrences implements WritableComparable<CorpusOccurrences> {
    private boolean corpus; //true=0 false=1
    private long occ;

    public CorpusOccurrences(){}
    public CorpusOccurrences(boolean c, long o){
            corpus=c;
            occ=o;
    }
    @Override
    public int compareTo(CorpusOccurrences other) {
        return getCorpusValue(corpus)-getCorpusValue(other.getCorpus());
    }

    public boolean getCorpus() {
        return corpus;
    }

    public int getCorpusValue(boolean c){
        if(c){return 0;}
        else{return 1;}
    }

    public long getOcc() {
        return occ;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(corpus);
        dataOutput.writeLong(occ);
    }

    public void readFields(DataInput dataInput) throws IOException {
        corpus = dataInput.readBoolean();
        occ = dataInput.readLong();
    }

    public String toString(){
            return corpus+" "+occ;
        }

}
