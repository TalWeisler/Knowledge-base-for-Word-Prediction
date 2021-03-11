import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Probability implements WritableComparable<Probability> {
    private String W1;
    private String W2;
    private Double probability;

    public Probability(){}
    public Probability(String w1,String w2, Double p){
        W1=w1;
        W2=w2;
        probability=p;
    }

    @Override
    public int compareTo(Probability other) {
        int check1= getTwoWords().compareTo(other.getTwoWords());
        if(check1==0){
            if (probability== other.probability)
                return 0;
            else if (probability > other.probability)
                return -1;
            else
                return 1;
        }
        return check1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(W1);
        dataOutput.writeUTF(W2);
        dataOutput.writeDouble(probability);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        W1= dataInput.readUTF();
        W2= dataInput.readUTF();
        probability= dataInput.readDouble();
    }

    public String getTwoWords (){
        return W1+" "+W2;
    }

    public double getProbability() {
        return probability;
    }
    public String getProbabilityString() {
        return String.valueOf(probability);
    }

    @Override
    public String toString() {
        return W1+" "+W2+" "+String.valueOf(probability);
    }
}
