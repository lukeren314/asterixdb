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

import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

class ExternalAggregatePythonFunctionEvaluator extends ExternalAggregateFunctionEvaluator {
    private static final String INIT_IDENTIFIER = "init";
    private static final String STEP_IDENTIFIER = "step";
    private static final String FINISH_IDENTIFIER = "finish";

    private final ExternalPythonFunctionEvaluator externalPythonFunctionEvaluator;

    private final long initFnId;
    private final long stepFnId;
    private final long finishFnId;

    ExternalAggregatePythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        externalPythonFunctionEvaluator = new ExternalPythonFunctionEvaluator(finfo, args, ctx, sourceLoc);
        try {
            externalPythonFunctionEvaluator.initializeClass(finfo);
            this.initFnId = externalPythonFunctionEvaluator.initialize(addFunctionIdentifier(finfo, INIT_IDENTIFIER));
            this.stepFnId = externalPythonFunctionEvaluator.initialize(addFunctionIdentifier(finfo, STEP_IDENTIFIER));
            this.finishFnId = externalPythonFunctionEvaluator.initialize(addFunctionIdentifier(finfo, FINISH_IDENTIFIER));
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException("Failed to initialize Python class", e);
        }
    }

    @Override
    public void init() throws HyracksDataException {
        externalPythonFunctionEvaluator.callPython(initFnId, finfo.getNullCall());
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        externalPythonFunctionEvaluator.callPython(stepFnId, argTypes, argEvals, tuple, finfo.getNullCall());
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        externalPythonFunctionEvaluator.callPython(finishFnId, result, finfo.getNullCall());
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        finish(result);
    }

    private IExternalFunctionInfo addFunctionIdentifier(IExternalFunctionInfo finfo, String functionName) {
        List<String> newExternalIdentifier = extendExternalIdentifier(finfo.getExternalIdentifier(), functionName);
        return new ExternalFunctionInfo(finfo, newExternalIdentifier);
    }

    private List<String> extendExternalIdentifier(List<String> externalIdentifier, String functionName) {
        List<String> newExternalIdentifier = new ArrayList<>(externalIdentifier);
        int lastIndex = newExternalIdentifier.size() - 1;
        String newFunctionName = newExternalIdentifier.get(lastIndex) + "." + functionName;
        newExternalIdentifier.set(lastIndex, newFunctionName);
        return newExternalIdentifier;
    }
}
