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
package org.apache.asterix.external.input.record.reader.hdfs.parquet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.util.HDFSUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.projection.FunctionCallInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class ParquetReadSupport extends ReadSupport<IValueReference> {
    @Override
    public ReadContext init(InitContext context) {
        MessageType requestedSchema = getRequestedSchema(context);
        return new ReadContext(requestedSchema, Collections.emptyMap());
    }

    @Override
    public RecordMaterializer<IValueReference> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new ADMRecordMaterializer(readContext);
    }

    private static MessageType getRequestedSchema(InitContext context) {
        Configuration configuration = context.getConfiguration();
        MessageType fileSchema = context.getFileSchema();
        AsterixTypeToParquetTypeVisitor visitor = new AsterixTypeToParquetTypeVisitor();
        try {
            ARecordType expectedType = HDFSUtils.getExpectedType(configuration);
            Map<String, FunctionCallInformation> functionCallInformationMap =
                    HDFSUtils.getFunctionCallInformationMap(configuration);
            MessageType requestedType = visitor.clipType(expectedType, fileSchema, functionCallInformationMap);
            List<Warning> warnings = visitor.getWarnings();

            if (!warnings.isEmpty()) {
                //New warnings were created, set the warnings in hadoop configuration to be reported
                HDFSUtils.setWarnings(warnings, configuration);
                //Update the reported warnings so that we do not report the same warning again
                HDFSUtils.setFunctionCallInformationMap(functionCallInformationMap, configuration);
            }
            return requestedType;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class ADMRecordMaterializer extends RecordMaterializer<IValueReference> {
        private final RootConverter rootConverter;

        public ADMRecordMaterializer(ReadContext readContext) {
            rootConverter = new RootConverter(readContext.getRequestedSchema());
        }

        @Override
        public IValueReference getCurrentRecord() {
            return rootConverter.getRecord();
        }

        @Override
        public GroupConverter getRootConverter() {
            return rootConverter;
        }
    }
}
