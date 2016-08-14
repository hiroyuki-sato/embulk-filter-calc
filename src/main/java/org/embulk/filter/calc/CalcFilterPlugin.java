package org.embulk.filter.calc;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfigException;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;

import java.util.List;

import static java.util.Locale.ENGLISH;

public class CalcFilterPlugin
        implements FilterPlugin
{

    // private Object IOException;

    public interface CalcConfig
            extends Task
    {
        @Config("formula")
        String getFormula();

        @Config("name")
        String getName();
    }

    public interface PluginTask
            extends Task
    {

        @Config("columns")
        public List<CalcConfig> getCalcConfig();

        @Config("output_columns")
        @ConfigDefault("[]")
        public List<ColumnConfig> getOutputColumns();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = buildOutputSchema(task, inputSchema);
        for (CalcConfig calcConfig : task.getCalcConfig()) {
            CalcConfigChecker calc = new CalcConfigChecker(calcConfig.getName(), calcConfig.getFormula(), outputSchema);
            calc.validateFormula();
        }

        control.run(task.dump(), outputSchema);
    }

    static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
    {
        Schema.Builder builder = Schema.builder();
        for (Column inputColumns : inputSchema.getColumns()) {
            builder.add(inputColumns.getName(), inputColumns.getType());
        }

        List<ColumnConfig> outputColumns = task.getOutputColumns();
        for (ColumnConfig outputColumn : outputColumns) {

            String name = outputColumn.getName();
            Type type = outputColumn.getType();
            Column inputColumn;
            try {
                inputColumn = inputSchema.lookupColumn(name);
            }
            catch (SchemaConfigException ex) {
                inputColumn = null;
            }
            if (inputColumn != null) {
                throw new SchemaConfigException(String.format(ENGLISH, "The column \"%s\" already exists.", name));
            }

            if (Types.DOUBLE.equals(type)) {
                builder.add(name, Types.DOUBLE);
            }
            else if (Types.LONG.equals(type)) {
                builder.add(name, Types.LONG);
            }
            else {
                throw new SchemaConfigException(String.format(ENGLISH, "The column \"%s\" must specify either long or double.", name));
            }
        }
        return builder.build();
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
            final Schema outputSchema, final PageOutput output)
    {
        final PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput()
        {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
            private CalcVisitorImpl visitor = new CalcVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

            @Override
            public void finish()
            {
                pageBuilder.finish();
            }

            @Override
            public void close()
            {
                pageBuilder.close();
            }

            @Override
            public void add(Page page)
            {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    outputSchema.visitColumns(visitor);
                    pageBuilder.addRecord();
                }
            }
        };
    }
}
