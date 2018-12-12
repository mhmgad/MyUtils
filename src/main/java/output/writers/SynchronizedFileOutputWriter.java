package output.writers;

import java.io.IOException;
import java.util.Collection;

public class SynchronizedFileOutputWriter extends FileOutputWriter {

    public SynchronizedFileOutputWriter(String outputFilePath, OutputFormat mode, String header) {
        super(outputFilePath, mode, header);
    }

    public SynchronizedFileOutputWriter(String outputFilePath, OutputFormat mode) {
        super(outputFilePath, mode);
    }

    @Override
    public synchronized void write(Collection collection) {
        super.write(collection);
    }

    @Override
    public synchronized void  write(SerializableData record) {
        super.write(record);

    }



    @Override
    public String getName() {
        return "SyncFile-"+mode.toString();
    }



}
