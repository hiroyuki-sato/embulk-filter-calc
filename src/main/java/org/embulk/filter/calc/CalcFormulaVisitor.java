package org.embulk.filter.calc;

import org.embulk.spi.Column;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

public class CalcFormulaVisitor
        extends CalculatorBaseVisitor<Double>
{
    private PageReader pageReader;
    private Schema inputSchema;

    public CalcFormulaVisitor(Schema inputSchema, PageReader pageReader)
    {
        this.pageReader = pageReader;
        this.inputSchema = inputSchema;
    }

    @Override
    public Double visitMulDivMod(CalculatorParser.MulDivModContext ctx)
    {
        Double left = visit(ctx.expr(0));
        Double right = visit(ctx.expr(1));

        if (left == null || right == null) {
            return null;
        }
        else if (ctx.op.getType() == CalculatorParser.MUL) {
            return left * right;
        }

        else if (ctx.op.getType() == CalculatorParser.DIV) {
            return left / right;
        }
        else {
            return left % right;
        }
    }

    @Override
    public Double visitAddSub(CalculatorParser.AddSubContext ctx)
    {
        Double left = visit(ctx.expr(0));
        Double right = visit(ctx.expr(1));
        if (left == null || right == null) {
            return null;
        }
        else if (ctx.op.getType() == CalculatorParser.ADD) {
            return left + right;
        }
        else {
            return left - right;
        }
    }

    @Override
    public Double visitNumber(CalculatorParser.NumberContext ctx)
    {
        String id = ctx.NUM().getText();

        return Double.parseDouble(id);
    }

    @Override
    public Double visitIdentifier(CalculatorParser.IdentifierContext ctx)
    {
        String id = ctx.ID().getText();
        Double val;
        Column column = inputSchema.lookupColumn(id);

        if (pageReader.isNull(column)) {
            val = null;
        }
        else if (Types.DOUBLE.equals(column.getType())) {
            val = pageReader.getDouble(column);
        }
        else if (Types.LONG.equals(column.getType())) {
            Long v;
            v = pageReader.getLong(column);
            val = v.doubleValue();
        }
        else {
            // throw
            val = null;
        }
        return val;
    }

    @Override
    public Double visitParen(CalculatorParser.ParenContext ctx)
    {
        return visit(ctx.expr());
    }

    @Override
    public Double visitPower(CalculatorParser.PowerContext ctx)
    {
        Double left = visit(ctx.expr(0));
        Double right = visit(ctx.expr(1));
        if (left == null || right == null) {
            return null;
        }

        return Math.pow(left, right);
    }

    // Scientific Functions
    @Override
    public Double visitFuncCos(CalculatorParser.FuncCosContext ctx)
    {
        return Math.cos(visit(ctx.expr()));
    }

    @Override
    public Double visitFuncSin(CalculatorParser.FuncSinContext ctx)
    {
        return Math.sin(visit(ctx.expr()));
    }

    @Override
    public Double visitFuncTan(CalculatorParser.FuncTanContext ctx)
    {
        return Math.tan(visit(ctx.expr()));
    }
}
