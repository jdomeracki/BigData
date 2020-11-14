import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CasualtiesCount implements WritableComparable<CasualtiesCount> {
    IntWritable injured_pedestrians;
    IntWritable injured_cyclists;
    IntWritable injured_motorists;
    IntWritable killed_pedestrians;
    IntWritable killed_cyclists;
    IntWritable killed_motorists;

    public CasualtiesCount() {
        set(new IntWritable(0),new IntWritable(0),new IntWritable(0),new IntWritable(0),new IntWritable(0),new IntWritable(0));
    }

    public CasualtiesCount(Integer injured_pedestrians, Integer injured_cyclists, Integer injured_motorists, Integer killed_pedestrians, Integer killed_cyclists, Integer killed_motorists) {
        set(new IntWritable(injured_pedestrians),new IntWritable(injured_cyclists),new IntWritable(injured_motorists),new IntWritable(killed_pedestrians),new IntWritable(killed_cyclists),new IntWritable(killed_motorists));
    }

    public void set(IntWritable injured_pedestrians, IntWritable injured_cyclists, IntWritable injured_motorists, IntWritable killed_pedestrians, IntWritable killed_cyclists, IntWritable killed_motorists){
        this.injured_pedestrians = injured_pedestrians;
        this.injured_cyclists = injured_cyclists;
        this.injured_motorists = injured_motorists;
        this.killed_pedestrians = killed_pedestrians;
        this.killed_cyclists = killed_cyclists;
        this.killed_motorists = killed_motorists;
    }

    public IntWritable getInjured_pedestrians() {
        return injured_pedestrians;
    }

    public IntWritable getInjured_cyclists() {
        return injured_cyclists;
    }

    public IntWritable getInjured_motorists() {
        return injured_motorists;
    }

    public IntWritable getKilled_pedestrians() {
        return killed_pedestrians;
    }

    public IntWritable getKilled_cyclists() {
        return killed_cyclists;
    }

    public IntWritable getKilled_motorists() {
        return killed_motorists;
    }

    public void addCasualtiesCount(CasualtiesCount obj){
        set(new IntWritable(this.injured_pedestrians.get() + obj.getInjured_pedestrians().get()),
                new IntWritable(this.injured_cyclists.get() + obj.getInjured_cyclists().get()),
                new IntWritable(this.injured_motorists.get() + obj.getInjured_motorists().get()),
                new IntWritable(this.killed_pedestrians.get() + obj.getKilled_pedestrians().get()),
                new IntWritable(this.killed_cyclists.get() + obj.getKilled_cyclists().get()),
                new IntWritable(this.killed_motorists.get() + obj.getKilled_motorists().get()));
    }

    public int compareTo(CasualtiesCount obj) {
        int intcnt = 0;
        intcnt += injured_pedestrians.compareTo(obj.injured_pedestrians);
        if(intcnt != 0) return  intcnt;
        intcnt += injured_cyclists.compareTo(obj.injured_cyclists);
        if(intcnt != 0) return  intcnt;
        intcnt += injured_motorists.compareTo(obj.injured_motorists);
        if(intcnt != 0) return  intcnt;
        intcnt += killed_pedestrians.compareTo(obj.killed_pedestrians);
        if(intcnt != 0) return  intcnt;
        intcnt += killed_cyclists.compareTo(obj.killed_cyclists);
        if(intcnt != 0) return  intcnt;
        intcnt += killed_motorists.compareTo(obj.killed_motorists);
        return intcnt == 0 ? 0 : intcnt;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        injured_pedestrians.write(dataOutput);
        injured_cyclists.write(dataOutput);
        injured_motorists.write(dataOutput);
        killed_pedestrians.write(dataOutput);
        killed_cyclists.write(dataOutput);
        killed_motorists.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        injured_pedestrians.readFields(dataInput);
        injured_cyclists.readFields(dataInput);
        injured_motorists.readFields(dataInput);
        killed_pedestrians.readFields(dataInput);
        killed_cyclists.readFields(dataInput);
        killed_motorists.readFields(dataInput);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CasualtiesCount casualtiesCount = (CasualtiesCount) obj;

        return injured_pedestrians.equals(casualtiesCount.injured_pedestrians) && injured_cyclists.equals(casualtiesCount.injured_cyclists)
                && injured_motorists.equals(casualtiesCount.injured_motorists) && killed_pedestrians.equals(casualtiesCount.killed_pedestrians)
                && killed_cyclists.equals(casualtiesCount.killed_cyclists) && killed_motorists.equals(casualtiesCount.killed_motorists);
    }

    @Override
    public int hashCode() {
        return Objects.hash(injured_pedestrians, injured_cyclists, injured_motorists, killed_pedestrians, killed_cyclists, killed_motorists);
    }

    @Override
    public String toString(){
        return injured_pedestrians.toString() + "," + killed_pedestrians.toString() + "," + injured_cyclists.toString() + "," + killed_cyclists.toString() + "," + injured_motorists.toString() + "," + killed_motorists.toString();
    }
}
