package org.apache.asterix.optimizer.base;

import java.util.List;

import org.apache.asterix.common.functions.AggregateFunctionSignature;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

    public static FunctionIdentifier getBuiltinAggOrUDF(AbstractFunctionCallExpression fc, MetadataProvider mdPv)
            throws AlgebricksException {
        FunctionIdentifier fid = fc.getFunctionIdentifier();
        AggregateFunctionSignature fSig = new AggregateFunctionSignature(fid).toAgg();
        org.apache.asterix.metadata.entities.Function udf = mdPv.lookupUserDefinedFunction(fSig);
        if (udf != null) {
            return udf.getSignature().createFunctionIdentifier();
        } else
            return BuiltinFunctions.getAggregateFunction(fc.getFunctionIdentifier());
    }

    public static AggregateFunctionCallExpression getBuiltinAggExprOrUDF(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args, MetadataProvider mdPv) throws AlgebricksException {
        AggregateFunctionSignature fSig = new AggregateFunctionSignature(fi);
        org.apache.asterix.metadata.entities.Function udf = mdPv.lookupUserDefinedFunction(fSig);
        if (udf != null) {
            IFunctionInfo finfo = ExternalFunctionCompilerUtil.getExternalFunctionInfo(mdPv, udf);

            IFunctionInfo localFinfo = ExternalFunctionCompilerUtil.getExternalFunctionInfo(mdPv, mdPv.lookupUserDefinedFunction(fSig.toLocal()));
            IFunctionInfo globalFinfo = ExternalFunctionCompilerUtil.getExternalFunctionInfo(mdPv, mdPv.lookupUserDefinedFunction(fSig.toGlobal()));
            AggregateFunctionCallExpression expr = new AggregateFunctionCallExpression(finfo, true, args);
            expr.setStepOneAggregate(localFinfo);
            expr.setStepTwoAggregate(globalFinfo);
            return expr;
        } else
            return BuiltinFunctions.makeAggregateFunctionExpression(fi, args);
    }
}
