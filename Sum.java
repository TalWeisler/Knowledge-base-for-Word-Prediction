import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sum implements WritableComparable<Sum> {
    private int corpus;
    private long MyR;
    private long OtherR;

    public Sum(){}

    public Sum(int c,long mr ,long or){
        corpus = c;
        MyR=mr;
        OtherR=or;
    }

    public int compareTo(Sum other) {
        return this.toString().compareTo(other.toString());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(corpus);
        dataOutput.writeLong(MyR);
        dataOutput.writeLong(OtherR);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        corpus= dataInput.readInt();
        MyR= dataInput.readLong();
        OtherR= dataInput.readLong();
    }

    public String toString(){
        return corpus+" "+MyR+" "+OtherR;
    }

    public int getCorpus(){
        return corpus;
    }

    public long getMyR() {
        return MyR;
    }

    public long getOtherR() {
        return OtherR;
    }

}
