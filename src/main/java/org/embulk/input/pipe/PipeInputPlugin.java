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