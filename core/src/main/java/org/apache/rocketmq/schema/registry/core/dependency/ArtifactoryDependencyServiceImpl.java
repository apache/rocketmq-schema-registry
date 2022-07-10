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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.model.Dependency;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;

@Slf4j
public class ArtifactoryDependencyServiceImpl implements DependencyService {

    private final GlobalConfig config;
    private final String parent;
    private final String localRepo;
    private final String jdkPath;
    private String dependencyTemplate;


    public ArtifactoryDependencyServiceImpl(final GlobalConfig config) {
        this.config = config;
        this.parent = config.getDependencyCompilePath();
        this.localRepo = config.getDependencyLocalRepositoryPath();
        this.jdkPath = config.getDependencyJdkPath();

        if (config.isUploadEnabled()) {
            try {
                CommonUtil.mkdir(parent);
                CommonUtil.mkdir(localRepo);
                Path templatePath = Paths.get(config.getDependencyTemplate());
                this.dependencyTemplate = new String(Files.readAllBytes(templatePath));
            } catch (IOException e) {
                throw new SchemaException("Init dependency template file failed", e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Dependency compile(SchemaInfo schemaInfo) {
        DependencyHelper dependencyHelper = DynamicCompileProvider
            .compile(jdkPath, parent, schemaInfo, dependencyTemplate);

        // upload jar file to remote repository
        DynamicCompileProvider.upload(
            localRepo,
            dependencyHelper,
            config.getDependencyUsername(),
            config.getDependencyPassword(),
            config.getDependencyRepositoryUrl()
        );
        return dependencyHelper.getDependency();
    }


}
