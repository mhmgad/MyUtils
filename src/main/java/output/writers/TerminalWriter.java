package output.writers;

public class TerminalWriter extends AbstractOutputChannel<SerializableData> {


    public TerminalWriter(OutputFormat mode) {
        this.mode=mode;

    }

    @Override
    public void write(SerializableData record) {
        System.out.println(super.formatString(record));
    }

    @Override
    public boolean close() {
        return true;
    }

    @Override
    public String getName() {
        return "Terminal-"+getName();
    }
}
