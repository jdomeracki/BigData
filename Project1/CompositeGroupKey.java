import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeGroupKey implements WritableComparable<CompositeGroupKey> {
    String street;
    String zip;

    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, street);
        WritableUtils.writeString(out, zip);
    }
    public void readFields(DataInput in) throws IOException {
        this.street = WritableUtils.readString(in);
        this.zip = WritableUtils.readString(in);
    }
    public int compareTo(CompositeGroupKey pop) {
        if (pop == null)
            return 0;
        int intcnt = street.compareTo(pop.street);
        return intcnt == 0 ? zip.compareTo(pop.zip) : intcnt;
    }
    @Override
    public String toString() {
        return street + "," + zip + ",";
    }
}