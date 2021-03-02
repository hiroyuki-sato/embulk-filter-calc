package org.embulk.filter.calc;

import com.google.common.collect.Lists;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.calc.CalcFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Schema;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;


public class TestCalcFilterPlugin
{

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private CalcFilterPlugin plugin;
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();


    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        return configMapper.map(config, PluginTask.class);
    }

    private void transaction(ConfigSource config, Schema inputSchema)
    {
        plugin.transaction(config, inputSchema, new FilterPlugin.Control() {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
            }
        });
    }


    @Before
    public void createResource()
    {
        plugin = new CalcFilterPlugin();
    }


    @Test
    public void buildOutputSchema_Columns()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - { name: long, formula: \"long + 1\"}");
        Schema inputSchema = Schema.builder()
                .add("timestamp", TIMESTAMP)
                .add("string", STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double", DOUBLE)
                .add("json", JSON)
                .add("remove_me", STRING)
                .build();

        Schema outputSchema = CalcFilterPlugin.buildOutputSchema(task, inputSchema);
        assertEquals(7, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(0);
            assertEquals("timestamp", column.getName());
        }
    }



}
