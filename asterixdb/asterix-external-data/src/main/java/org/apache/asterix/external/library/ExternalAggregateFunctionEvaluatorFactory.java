/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.library;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ExternalAggregateFunctionEvaluatorFactory implements IAggregateEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IExternalFunctionInfo finfo;
    private final IScalarEvaluatorFactory[] args;
    private final IAType[] argTypes;
    private final SourceLocation sourceLoc;

    public ExternalAggregateFunctionEvaluatorFactory(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, SourceLocation sourceLoc) {
        this.finfo = finfo;
        this.args = args;
        this.argTypes = argTypes;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public IAggregateEvaluator createAggregateEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        switch (finfo.getLanguage()) {
            case PYTHON:
                // TODO: MORE HACKINESS
                if (finfo.getFunctionIdentifier().getName().startsWith("agg-local-")) {
                    return new LocalExternalAggregatePythonFunctionEvaluator(finfo, args, argTypes, ctx, sourceLoc);
                } else if (finfo.getFunctionIdentifier().getName().startsWith("agg-global-")) {
                    return new GlobalExternalAggregatePythonFunctionEvaluator(finfo, args, argTypes, ctx, sourceLoc);
                }
                return new ScalarExternalAggregatePythonFunctionEvaluator(finfo, args, argTypes, ctx, sourceLoc);
            default:
                throw new RuntimeDataException(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND,
                        finfo.getLanguage());
        }
    }
}
