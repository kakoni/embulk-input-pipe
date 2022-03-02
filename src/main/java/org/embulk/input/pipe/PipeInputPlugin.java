package org.embulk.input.pipe;

import java.io.*;
import java.util.List;

import org.embulk.config.*;
import org.embulk.util.config.*;
import org.embulk.util.file.InputStreamFileInput;
import org.slf4j.Logger;

    import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.slf4j.LoggerFactory;

public class PipeInputPlugin
        implements FileInputPlugin
{
    public interface PluginTask
            extends Task
    {
    }

    private static final Logger LOG = LoggerFactory.getLogger(org.embulk.input.pipe.PipeInputPlugin.class);

    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    public static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    public static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control)
    {
        PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        return resume(task.toTaskSource(), 1, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control)
    {
        control.run(taskSource, taskCount);
        ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex)
    {
        PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);
        InputStream stream = System.in;
        PluginFileInput input = new PluginFileInput(task, new EofInputStream(stream));
        return input;
    }

    private static class EofInputStream extends InputStream
    {

        protected InputStream wrappedStream;
        private boolean selfClosed;

        public EofInputStream(final InputStream in)
        {
            wrappedStream = in;
            selfClosed = false;
        }

        protected boolean isReadAllowed() throws IOException {
            if (selfClosed) {
                throw new IOException("Attempted read on closed stream.");
            }
            return (wrappedStream != null);
        }

        @Override
        public int read() throws IOException {
            int l = -1;

            if (isReadAllowed()) {
                try {
                    l = wrappedStream.read();
                    checkEOF(l);
                } catch (final IOException ex) {
                    checkAbort();
                    throw ex;
                }
            }

            return l;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            int l = -1;

            if (isReadAllowed()) {
                try {
                    l = wrappedStream.read(b,  off,  len);
                    checkEOF(l);
                } catch (final IOException ex) {
                    checkAbort();
                    throw ex;
                }
            }

            return l;
        }

        @Override
        public int read(final byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int available() throws IOException {
            int a = 0; // not -1

            if (isReadAllowed()) {
                try {
                    a = wrappedStream.available();
                    // no checkEOF() here, available() can't trigger EOF
                } catch (final IOException ex) {
                    checkAbort();
                    throw ex;
                }
            }

            return a;
        }

        @Override
        public void close() throws IOException {
            // tolerate multiple calls to close()
            selfClosed = true;
            checkClose();
        }

        protected void checkEOF(final int eof) throws IOException {
            if ((wrappedStream != null) && (eof < 0)) {
                try {
                    boolean scws = true; // should close wrapped stream?
                    if (scws) {
                        wrappedStream.close();
                    }
                } finally {
                    wrappedStream = null;
                }
            }
        }

        protected void checkClose() throws IOException {

            if (wrappedStream != null) {
                try {
                    boolean scws = true; // should close wrapped stream?
                    if (scws) {
                        wrappedStream.close();
                    }
                } finally {
                    wrappedStream = null;
                }
            }
        }

        protected void checkAbort() throws IOException {

            if (wrappedStream != null) {
                try {
                    boolean scws = true; // should close wrapped stream?
                    if (scws) {
                        wrappedStream.close();
                    }
                } finally {
                    wrappedStream = null;
                }
            }
        }

    }

    // TODO almost copied from S3FileInputPlugin. include an InputStreamFileInput utility to embulk-core.
    public class PluginFileInput
            extends InputStreamFileInput
            implements TransactionalFileInput
    {

        public PluginFileInput(PluginTask task, InputStream stream)
        {
            super(Exec.getBufferAllocator(), new SingleFileProvider(stream));
        }

        public void abort() {

        }

        public TaskReport commit() {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        @Override
        public void close() { }
    }

    private class SingleFileProvider
            implements InputStreamFileInput.Provider
    {
        private InputStream stream;
        private boolean opened = false;

        public SingleFileProvider(InputStream stream)
        {
            this.stream = stream;
        }

        @Override
        public InputStreamFileInput.InputStreamWithHints openNextWithHints()
        {
            if (opened) {
                return null;
            }
            opened = true;
            return new InputStreamFileInput.InputStreamWithHints(stream);
        }

        @Override
        public void close() throws IOException
        {
            if (!opened) {
                stream.close();
            }
        }
    }

}