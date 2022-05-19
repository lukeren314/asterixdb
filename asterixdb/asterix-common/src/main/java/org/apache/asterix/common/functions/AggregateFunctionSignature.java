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
package org.apache.asterix.common.functions;

import java.io.Serializable;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class AggregateFunctionSignature extends FunctionSignature implements Serializable {

    public static String AGGREGATE_PREFIX = "agg-";
    public static String LOCAL_PREFIX = "agg-local-";
    public static String GLOBAL_PREFIX = "agg-global-";

    private final String baseName;

    public AggregateFunctionSignature(FunctionIdentifier fi) {
        super(fi);
        if (fi.getName().startsWith(AGGREGATE_PREFIX)) {
            baseName = fi.getName().substring(AGGREGATE_PREFIX.length());
        } else {
            baseName = fi.getName();
        }
    }

    public AggregateFunctionSignature(FunctionSignature fs) {
        super(fs.getDataverseName(), fs.getName(), fs.getArity());
        baseName = fs.getName();
    }

    public AggregateFunctionSignature toAgg() {
        AggregateFunctionSignature fSig = new AggregateFunctionSignature(this);
        fSig.setName(AGGREGATE_PREFIX + baseName);
        return fSig;
    }

    public AggregateFunctionSignature toLocal() {
        AggregateFunctionSignature fSig = new AggregateFunctionSignature(this);
        fSig.setName(LOCAL_PREFIX + baseName);
        return fSig;
    }

    public AggregateFunctionSignature toGlobal() {
        AggregateFunctionSignature fSig = new AggregateFunctionSignature(this);
        fSig.setName(GLOBAL_PREFIX + baseName);
        return fSig;
    }

    public static boolean isAgg(FunctionIdentifier fid) {
        return fid.getName().startsWith(AGGREGATE_PREFIX);
    }

    public static boolean isLocal(FunctionIdentifier fid) {
        return fid.getName().startsWith(LOCAL_PREFIX);
    }

    public static boolean isGlobal(FunctionIdentifier fid) {
        return fid.getName().startsWith(GLOBAL_PREFIX);
    }
}
