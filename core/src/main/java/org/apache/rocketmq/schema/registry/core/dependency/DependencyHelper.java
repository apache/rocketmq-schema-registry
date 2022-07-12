/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.schema.registry.core.dependency;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.model.Dependency;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;

@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DependencyHelper {

    private String schemaName;

    private String schemaDir;
    private String idlDir;
    private String javaDir;

    private String idlFilePath;
    private String jarFilePath;
    private String pomFilePath;
    private String jdkPath;

    private String idl;

    Map<String, ByteBuffer> schemaJarClass;
    private Dependency dependency;

    public DependencyHelper(final String jdkPath, final String parentDir, final SchemaInfo schemaInfo) {
        this.schemaName = schemaInfo.getSchemaName();

        this.dependency = Dependency.builder()
            .groupId(schemaInfo.getNamespace())
            .artifactId(schemaInfo.getSchemaName())
            .version(schemaInfo.getSchemaType() + "." + schemaInfo.getLastRecordVersion())
            .build();

        this.schemaDir = String.format("%s/%s/%d", parentDir, schemaInfo.getSchemaName(), schemaInfo.getLastRecordVersion());
        this.idlDir = String.format("%s/avro", schemaDir);
        this.javaDir = String.format("%s/java", schemaDir);

        this.idlFilePath = String.format("%s/%s.avro", idlDir, schemaName);
        this.jarFilePath = String.format("%s/%s.jar", schemaDir, dependency.getArtifactId());
        this.pomFilePath = String.format("%s/%s.pom", schemaDir, dependency.getArtifactId());
        this.jdkPath = jdkPath;

        this.idl = schemaInfo.getLastRecordIdl();
    }

    public void initIdlFile() {
        CommonUtil.mkdir(idlDir);
        try (FileWriter fileWriter = new FileWriter(idlFilePath)) {
            fileWriter.write(idl);
            fileWriter.flush();
        } catch (IOException e) {
            throw new SchemaException("Init idl file failed", e);
        }

        log.info("write schema " + schemaName + "'s idl: " + idl + " to file " + idlFilePath + " success");
    }

    public Map<String, ByteBuffer> compileSchema() {
        CommonUtil.mkdir(javaDir);
        try (Stream<Path> pathStream = Files.walk(Paths.get(javaDir))) {
            CommonUtil.execCommand(jdkPath, "-jar", "tools/avro-tools-1.11.0.jar", "compile", "schema", idlFilePath, javaDir);
            List<File> javaFileList = pathStream
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .collect(Collectors.toList());
            log.info("Success flush java files: " + javaFileList);
            return CommonUtil.compileJavaFile(javaFileList);
        } catch (Throwable e){
            throw new SchemaException("Compile schema failed", e);
        }
    }
}
