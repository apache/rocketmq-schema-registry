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

package org.apache.rocketmq.schema.registry.common.utils;

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.apache.rocketmq.schema.registry.common.QualifiedName;
import org.apache.rocketmq.schema.registry.common.dto.SchemaDto;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.exception.SchemaCompatibilityException;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;

@Slf4j
public class CommonUtil {

    public static void validateName(QualifiedName qualifiedName) {
//        Preconditions.checkNotNull(qualifiedName.getTenant(), "Tenant is null");
//        Preconditions.checkNotNull(qualifiedName.getSubject(), "Subject is null");
//        Preconditions.checkNotNull(qualifiedName.getName(), "Schema name is null");
    }

    public static boolean isQualifiedNameEmpty(QualifiedName qualifiedName) {
        return Strings.isNullOrEmpty(qualifiedName.getTenant()) || Strings.isNullOrEmpty(qualifiedName.getSchema());
    }

    public static List<File> listFiles(File path) {
        if (path != null && path.isDirectory()) {
            File[] files = path.listFiles();
            if (files != null) {
                return Arrays.stream(files).collect(Collectors.toList());
            }
        }
        return new ArrayList<>();
    }

    public static Properties loadProperties(File file) {
        Properties properties = new Properties();
        if (file.isFile()) {
            try (InputStreamReader in = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                properties.load(in);
            } catch (IOException e) {
                throw new SchemaException(String.format("Load properties failed: %s", file), e);
            }
        }
        return properties;
    }

    public static File mkdir(String pathStr) {
        return mkdir(new File(pathStr));
    }

    public static File mkdir(File fileDir) {
        if (fileDir.exists() && !fileDir.isDirectory()) {
            throw new SchemaException("Not a directory: " + fileDir.getAbsolutePath());
        }

        if (!fileDir.exists() && !fileDir.mkdirs()) {
            throw new SchemaException("Couldn't create directory: " + fileDir.getAbsolutePath());
        }

        return fileDir;
    }

    /**
     * can't modify
     *
     * @param value
     * @return
     * @throws SchemaException
     */
    private String md5hash(String value) throws SchemaException {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("MD5");
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            messageDigest.update(bytes, 0, bytes.length);
            return new BigInteger(1, messageDigest.digest()).toString(16);
        } catch (Exception e) {
            throw new SchemaException("md5hash failed: " + value);
        }
    }

    public static Map<String, ByteBuffer> compileJavaFile(List<File> javaFileList) throws IOException {
        Map<String, ByteBuffer> results = new HashMap<String, ByteBuffer>();
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager stdManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits = stdManager.getJavaFileObjectsFromFiles(javaFileList);

        MemoryJavaFileManager manager = new MemoryJavaFileManager(stdManager);
        JavaCompiler.CompilationTask task = compiler.getTask(null, manager, null, null, null, compilationUnits);
        if (task.call()) {
            results.putAll(manager.getClassBytes());
        }
        return results;
    }

    public static void generateJarFile(String jarFilePath, Map<String, ByteBuffer> classResult) {
        try (JarOutputStream jarOutStream = new JarOutputStream(Files.newOutputStream(Paths.get(jarFilePath)))) {
            for (String name : classResult.keySet()) {
                JarEntry entry = new JarEntry(name.replace(".", "/") + ".class");
                jarOutStream.putNextEntry(entry);
                jarOutStream.write(classResult.get(name).array());
                jarOutStream.closeEntry();
            }
        } catch (IOException e) {
            throw new SchemaException("Generate jar file: " + jarFilePath + " failed", e);
        }
    }

    public static void generatePomFile(String dependencyTemplate,
        String group, String artifact, String version, String pomFilePath) {
        try (FileWriter fileWriter = new FileWriter(pomFilePath)) {
            dependencyTemplate = dependencyTemplate.replace("$groupId", group)
                .replace("$artifactId", artifact)
                .replace("$version", version);
            fileWriter.write(dependencyTemplate);
            fileWriter.flush();
        } catch (IOException e) {
            throw new SchemaException("Generate pom file: " + pomFilePath + " failed", e);
        }
    }

    public static void execCommand(String... command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command);
        Process process = pb.start();

        ProcessStream infoThread = new ProcessStream(process.getInputStream());
        ProcessStream errThread = new ProcessStream(process.getErrorStream());
        infoThread.start();
        errThread.start();

        process.waitFor();
        process.destroy();

        String info = infoThread.getOutput();
        String err = errThread.getOutput();
        log.info("exec command " + Arrays.toString(command) + " info: " + info);
        log.info("exec command " + Arrays.toString(command) + " error: " + err);
        if (err.contains("Exception")) {
            throw new SchemaException("exec command failed: " + err);
        }
    }

    private static class ProcessStream extends Thread {
        private final InputStream inputStream;
        private String output;

        public ProcessStream(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        public String getOutput() {
            return this.output;
        }

        @Override
        public void run() {
            try (
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(inputStreamReader);
            ) {
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
                output = sb.toString();
            } catch (Throwable e) {
                log.error("", e);
            }
        }
    }

    public static void validateIdl(SchemaDto schemaDto) throws SchemaCompatibilityException {
        switch (schemaDto.getMeta().getType()) {
            case AVRO:
                break;
            case JSON:
                throw new SchemaCompatibilityException("Unsupported schema type: " + schemaDto.getMeta().getType());
        }
    }

    public static void validateCompatibility(SchemaInfo update, SchemaInfo current,
        Compatibility expectCompatibility) {
        switch (update.getMeta().getType()) {
            case AVRO:
                SchemaValidator validator = new SchemaValidatorBuilder().canReadStrategy().validateLatest();
                try {
                    Schema toValidate = new Schema.Parser().parse(update.getLastRecordIdl());
                    List<Schema> existing = new ArrayList<>();
                    existing.add(new Schema.Parser().parse(current.getLastRecordIdl()));
                    validator.validate(toValidate, existing);
                } catch (SchemaValidationException e) {
                    throw new SchemaCompatibilityException("Schema validation failed", e);
                }
                break;
            default:
                throw new SchemaCompatibilityException("Unsupported schema type: " + update.getMeta().getType());
        }

    }
}
