package org.embulk.filter.calc;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.slf4j.Logger;

import java.util.HashMap;

public class CalcVisitorImpl
        implements ColumnVisitor
{

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(CalcFilterPlugin.class);
    private final CalcFilterPlugin.PluginTask task;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final HashMap<String, Calculator> calcMap = new HashMap<>();

    CalcVisitorImpl(CalcFilterPlugin.PluginTask task, Schema inputSchema, Schema outputSchema, PageReader pageReader, PageBuilder pageBuilder)
    {
        this.task = task;
        this.inputSchema = inputSchema;
        this.outputSchema = outputSchema;
        this.pageReader = pageReader;
        this.pageBuilder = pageBuilder;
        initializeCalcMap();
    }

    private void initializeCalcMap()
    {

        for (CalcFilterPlugin.CalcConfig calcConfig : task.getCalcConfig()) {
            String name = calcConfig.getName();
            String formula = calcConfig.getFormula();
            Calculator calc = new Calculator(formula, inputSchema, pageReader);
            calcMap.put(name, calc);
        }
    }

    @Override
    public void booleanColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setBoolean(outputColumn, pageReader.getBoolean(inputColumn));
        }
    }

    @Override
    public void longColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        Calculator calc = calcMap.get(outputColumn.getName());
        if (calc == null) {
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setLong(outputColumn, pageReader.getLong(inputColumn));
            }
        }
        else {
            Double val = calc.calc();
            if (val == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setLong(outputColumn, val.longValue());
            }
        }
    }

    @Override
    public void doubleColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        Calculator calc = calcMap.get(outputColumn.getName());
        if (calc == null) {
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setDouble(outputColumn, pageReader.getDouble(inputColumn));
            }
        }
        else {
            Double val = calc.calc();
            if (val == null) {
                pageBuilder.setNull(outputColumn);
            }
            else {
                pageBuilder.setDouble(outputColumn, val);
            }
        }
    }

    @Override
    public void stringColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setString(outputColumn, pageReader.getString(inputColumn));
        }
    }

    @Override
    public void jsonColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setJson(outputColumn, pageReader.getJson(inputColumn));
        }
    }

    @Override
    public void timestampColumn(Column outputColumn)
    {
        Column inputColumn = inputSchema.lookupColumn(outputColumn.getName());
        if (pageReader.isNull(inputColumn)) {
            pageBuilder.setNull(outputColumn);
        }
        else {
            pageBuilder.setTimestamp(outputColumn, pageReader.getTimestampInstant(inputColumn));
        }
    }
}
