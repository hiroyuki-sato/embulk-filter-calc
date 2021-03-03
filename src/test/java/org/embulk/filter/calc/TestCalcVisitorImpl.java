package org.embulk.filter.calc;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.filter.calc.CalcFilterPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecInternal;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.util.Pages;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.value.ValueFactory;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;

import java.util.List;


public class TestCalcVisitorImpl
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Before
    public void createResource()
    {
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private PluginTask taskFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(ExecInternal.getModelManager());
        ConfigSource config = loader.fromYamlString(yamlString);
        ConfigMapper configMapper = CONFIG_MAPPER_FACTORY.createConfigMapper();
        return configMapper.map(config, PluginTask.class);
    }

    private List<Object[]> filter(PluginTask task, Schema inputSchema, Object ... objects)
    {
        TestPageBuilderReader.MockPageOutput output = new TestPageBuilderReader.MockPageOutput();
        Schema outputSchema = CalcFilterPlugin.buildOutputSchema(task, inputSchema);
        PageBuilder pageBuilder = Exec.getPageBuilder(runtime.getBufferAllocator(), outputSchema, output);
        PageReader pageReader = Exec.getPageReader(inputSchema);
        CalcVisitorImpl visitor = new CalcVisitorImpl(task, inputSchema, outputSchema, pageReader, pageBuilder);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), inputSchema, objects);
        for (Page page : pages) {
            pageReader.setPage(page);

            while (pageReader.nextRecord()) {
                outputSchema.visitColumns(visitor);
                pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
        pageBuilder.close();
        return Pages.toObjects(outputSchema, output.pages);
    }

    @Test
    public void visit_calc_NoFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns: []");
        Schema inputSchema = Schema.builder()
                .add("timestamp",TIMESTAMP)
                .add("string",STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double",DOUBLE)
                .add("json",JSON)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"),
                // row2
                Timestamp.ofEpochSecond(1436745600), "string", new Boolean(true), new Long(0), new Double(0.5), ValueFactory.newString("json"));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(6, record.length);
            assertEquals(Timestamp.ofEpochSecond(1436745600),record[0]);
            assertEquals("string",record[1]);
            assertEquals(new Boolean(true),record[2]);
            assertEquals(new Long(0),record[3]);
            assertEquals(new Double(0.5),record[4]);
            assertEquals(ValueFactory.newString("json"),record[5]);
        }
    }

    @Test
    public void visit_calc_NoFormulaWithNull()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns: []");
        Schema inputSchema = Schema.builder()
                .add("dummy",STRING)
                .add("timestamp",TIMESTAMP)
                .add("string",STRING)
                .add("boolean", BOOLEAN)
                .add("long", LONG)
                .add("double",DOUBLE)
                .add("json",JSON)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                "dummy",null,null,null,null,null,null,
                // row2
                "dummy",null,null,null,null,null,null);

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(7, record.length);
            assertEquals("dummy",record[0]);
            assertEquals(null,record[1]);
            assertEquals(null,record[2]);
            assertEquals(null,record[3]);
            assertEquals(null,record[4]);
            assertEquals(null,record[5]);
            assertEquals(null,record[6]);

        }
    }

    @Test
    public void visit_calc_NullFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: long,   formula: \" long + 10 \"}",
                "  - {name: double,   formula: \" double + 10 \"}");
        Schema inputSchema = Schema.builder()
                .add("long", LONG)
                .add("double", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                null,null,
                // row2
                null,null);

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(2, record.length);
            assertEquals(null,record[0]);
            assertEquals(null,record[1]);
        }
    }

    @Test
    public void visit_calc_SingleFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: long1,   formula: \" 100 \"}",
                "  - {name: long2,   formula: \" long2 \"}",
                "  - {name: double1,   formula: \" 11.1 \"}",
                "  - {name: double2,   formula: \" double2 \"}");
        Schema inputSchema = Schema.builder()
                .add("long1", LONG)
                .add("long2", LONG)
                .add("double1", DOUBLE)
                .add("double2", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Long(521),new Long(521),new Double(523.5),new Double(523.5),
                // row2
                new Long(521),new Long(521),new Double(523.5),new Double(523.5));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(4, record.length);
            assertEquals(new Long(100),   record[0]);
            assertEquals(new Long(521),   record[1]);
            assertEquals(new Double(11.1), record[2]);
            assertEquals(new Double(523.5),record[3]);
        }
    }


    @Test
    public void visit_calc_MathFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: sin_value,   formula: \" sin(sin_value) \"}",
                "  - {name: cos_value,   formula: \" cos(cos_value) \"}",
                "  - {name: tan_value,   formula: \" tan(tan_value) \"}");
        Schema inputSchema = Schema.builder()
                .add("sin_value", DOUBLE)
                .add("cos_value", DOUBLE)
                .add("tan_value", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Double(0.05),new Double(0.05),new Double(0.05),
                // row2
                new Double(0.05),new Double(0.05),new Double(0.05));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(3, record.length);
            assertEquals(Math.sin(0.05),record[0]);
            assertEquals(Math.cos(0.05),record[1]);
            assertEquals(Math.tan(0.05),record[2]);
        }
    }

    @Test
    public void visit_calc_SinglePowerFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: long1,   formula: \" 2 ^ 8\"}",
                "  - {name: long2,   formula: \" long2 ^ 8 \"}",
                "  - {name: double1,   formula: \" 2 ^ 8 \"}",
                "  - {name: double2,   formula: \" double2 ^ 8 \"}");
        Schema inputSchema = Schema.builder()
                .add("long1", LONG)
                .add("long2", LONG)
                .add("double1", DOUBLE)
                .add("double2", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Long(10),new Long(3),new Double(10.0),new Double(2.0),
                // row2
                new Long(10),new Long(2),new Double(10.0),new Double(2.0));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(4, record.length);
            assertEquals(new Long(256),   record[0]);
            assertEquals(new Long(6561),   record[1]);
            assertEquals(new Double(256), record[2]);
            assertEquals(new Double(256.0),record[3]);
        }
    }
    @Test
    public void visit_calc_BasicFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: add_long,   formula: \" add_long + 100\"}",
                "  - {name: sub_long,   formula: \" sub_long - 100\"}",
                "  - {name: mul_long,   formula: \" mul_long * 100\"}",
                "  - {name: div_long,   formula: \" div_long / 100\"}",
                "  - {name: mod_long,   formula: \" mod_long % 100\"}",
                "  - {name: add_double, formula: \" add_double + 100\"}",
                "  - {name: sub_double, formula: \" sub_double - 100\"}",
                "  - {name: mul_double, formula: \" mul_double * 100\"}",
                "  - {name: div_double, formula: \" div_double / 100\"}",
                "  - {name: mod_double, formula: \" mod_double % 100\"}");
        Schema inputSchema = Schema.builder()
                .add("add_long", LONG)
                .add("sub_long", LONG)
                .add("mul_long", LONG)
                .add("div_long", LONG)
                .add("mod_long", LONG)
                .add("add_double", DOUBLE)
                .add("sub_double", DOUBLE)
                .add("mul_double", DOUBLE)
                .add("div_double", DOUBLE)
                .add("mod_double", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),
                // row2
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(10, record.length);
            assertEquals(new Long(621),   record[0]);
            assertEquals(new Long(421),   record[1]);
            assertEquals(new Long(52100), record[2]);
            assertEquals(new Long(5),     record[3]);
            assertEquals(new Long(21),    record[4]);
            assertEquals(new Double(623.5),record[5]);
            assertEquals(new Double(423.5),record[6]);
            assertEquals(new Double(52350),record[7]);
            assertEquals(new Double(5.235),record[8]);
            assertEquals(new Double(23.5), record[9]);
        }
    }
    @Test
    public void visit_calc_PriorityChkFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: add_long,   formula: \" add_long + 100 * 3\"}",
                "  - {name: sub_long,   formula: \" sub_long - 100 * 3\"}",
                "  - {name: mul_long,   formula: \" mul_long * 100 * 3\"}",
                "  - {name: div_long,   formula: \" div_long / 100 * 3\"}",
                "  - {name: mod_long,   formula: \" mod_long % 100 * 3\"}",
                "  - {name: add_double, formula: \" add_double + 100 * 3\"}",
                "  - {name: sub_double, formula: \" sub_double - 100 * 3\"}",
                "  - {name: mul_double, formula: \" mul_double * 100 * 3\"}",
                "  - {name: div_double, formula: \" div_double / 100 * 3\"}",
                "  - {name: mod_double, formula: \" mod_double % 100 * 3\"}");
        Schema inputSchema = Schema.builder()
                .add("add_long", LONG)
                .add("sub_long", LONG)
                .add("mul_long", LONG)
                .add("div_long", LONG)
                .add("mod_long", LONG)
                .add("add_double", DOUBLE)
                .add("sub_double", DOUBLE)
                .add("mul_double", DOUBLE)
                .add("div_double", DOUBLE)
                .add("mod_double", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),
                // row2
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(10, record.length);
            assertEquals(new Long(821),   record[0]);
            assertEquals(new Long(221),   record[1]);
            assertEquals(new Long(156300), record[2]);
            assertEquals(new Long(15),     record[3]);
            assertEquals(new Long(63),    record[4]);
            assertEquals(new Double(823.5),record[5]);
            assertEquals(new Double(223.5),record[6]);
            assertEquals(new Double(157050),record[7]);
//            assertEquals(new Double(15.705),record[8]); // TODO result 15.705000000000002
            assertEquals(new Double(70.5), record[9]);
        }
    }
    @Test
    public void visit_calc_ParenChkFormula()
    {
        PluginTask task = taskFromYamlString(
                "type: calc",
                "columns:",
                "  - {name: add_long,   formula: \" ( add_long + 100 ) * 3\"}",
                "  - {name: sub_long,   formula: \" ( sub_long - 100 ) * 3\"}",
                "  - {name: mul_long,   formula: \" ( mul_long * 100 ) * 3\"}",
                "  - {name: div_long,   formula: \" ( div_long / 100 ) * 3\"}",
                "  - {name: mod_long,   formula: \" ( mod_long % 100 ) * 3\"}",
                "  - {name: add_double, formula: \" ( add_double + 100 ) * 3\"}",
                "  - {name: sub_double, formula: \" ( sub_double - 100 ) * 3\"}",
                "  - {name: mul_double, formula: \" ( mul_double * 100 ) * 3\"}",
                "  - {name: div_double, formula: \" ( div_double / 100 ) * 3\"}",
                "  - {name: mod_double, formula: \" ( mod_double % 100 ) * 3\"}");
        Schema inputSchema = Schema.builder()
                .add("add_long", LONG)
                .add("sub_long", LONG)
                .add("mul_long", LONG)
                .add("div_long", LONG)
                .add("mod_long", LONG)
                .add("add_double", DOUBLE)
                .add("sub_double", DOUBLE)
                .add("mul_double", DOUBLE)
                .add("div_double", DOUBLE)
                .add("mod_double", DOUBLE)
                .build();
        List<Object[]> records = filter(task, inputSchema,
                // row1
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),
                // row2
                new Long(521),new Long(521),new Long(521),new Long(521),new Long(521),
                new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5),new Double(523.5));

        assertEquals(2, records.size());

        Object[] record;
        {
            record = records.get(0);
            assertEquals(10, record.length);
            assertEquals(new Long(1863),   record[0]);
            assertEquals(new Long(1263),   record[1]);
            assertEquals(new Long(156300), record[2]);
            assertEquals(new Long(15),     record[3]);
            assertEquals(new Long(63),    record[4]);
            assertEquals(new Double(1870.5),record[5]);
            assertEquals(new Double(1270.5),record[6]);
            assertEquals(new Double(157050),record[7]);
//            assertEquals(new Double(15.705),record[8]); // TODO result 15.705000000000002
            assertEquals(new Double(70.5), record[9]);
        }
    }
}