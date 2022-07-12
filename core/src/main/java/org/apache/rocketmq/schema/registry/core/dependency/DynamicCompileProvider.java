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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfigImpl;
import org.apache.rocketmq.schema.registry.common.utils.CommonUtil;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.deployment.DeployRequest;
import org.eclipse.aether.deployment.DeployResult;
import org.eclipse.aether.deployment.DeploymentException;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.Authentication;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.eclipse.aether.util.artifact.SubArtifact;
import org.eclipse.aether.util.repository.AuthenticationBuilder;

@Data
@Slf4j
public class DynamicCompileProvider {
    private final static RepositorySystem system = newRepositorySystem();

    public static DependencyHelper compile(final String jdkPath, final String parentDir,
        final SchemaInfo schemaInfo, final String dependencyTemplate) {
        DependencyHelper dependencyHelper = new DependencyHelper(jdkPath, parentDir, schemaInfo);

        dependencyHelper.initIdlFile();

        Map<String, ByteBuffer> schemaJarClass = dependencyHelper.compileSchema();

        CommonUtil.generateJarFile(dependencyHelper.getJarFilePath(), schemaJarClass);

        CommonUtil.generatePomFile(
            dependencyTemplate,
            dependencyHelper.getDependency().getGroupId(),
            dependencyHelper.getDependency().getArtifactId(),
            dependencyHelper.getDependency().getVersion(),
            dependencyHelper.getPomFilePath()
        );

        return dependencyHelper;
    }

    private static RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
        locator.addService(TransporterFactory.class, FileTransporterFactory.class);
        locator.addService(TransporterFactory.class, HttpTransporterFactory.class);
        locator.setErrorHandler(new DefaultServiceLocator.ErrorHandler() {
            @Override
            public void serviceCreationFailed(Class<?> tpe, Class<?> impl, Throwable cause) {
                log.error("maven service locator create failed", cause);
            }
        });
        return locator.getService(RepositorySystem.class);
    }

    public static RepositorySystemSession newSession(RepositorySystem rs, String versionCache) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();
        LocalRepository localRepo = new LocalRepository(versionCache);
        session.setLocalRepositoryManager(rs.newLocalRepositoryManager(session, localRepo));
        return session;
    }

    // TODO: replace to artifactory-client-java
    public static void upload(
        final String localRepo,
        final DependencyHelper dependencyHelper,
        final String userName,
        final String passWord,
        final String repoUrl
    ) {
        String groupId = dependencyHelper.getDependency().getGroupId();
        String artifactId = dependencyHelper.getDependency().getArtifactId();
        String version = dependencyHelper.getDependency().getVersion();
        File jarFile = new File(dependencyHelper.getJarFilePath());
        File pomFile = new File(dependencyHelper.getPomFilePath());

        Artifact jarArtifact = new DefaultArtifact(groupId, artifactId, null, "jar", version, null, jarFile);
        Artifact pomArtifact = new SubArtifact(jarArtifact, null, "pom", pomFile);

        Authentication auth = new AuthenticationBuilder()
            .addUsername(userName)
            .addPassword(passWord)
            .build();

        RemoteRepository remoteRepo = new RemoteRepository
            .Builder("central", "default", repoUrl)
            .setAuthentication(auth)
            .build();

        try {
            String versionCache = CommonUtil.mkdir(String.format("%s/version_cache/%d_resources",
                localRepo, System.currentTimeMillis())).getCanonicalPath();
            RepositorySystemSession session = newSession(system, versionCache);
            DeployRequest deployRequest = new DeployRequest()
                .setRepository(remoteRepo)
                .addArtifact(jarArtifact)
                .addArtifact(pomArtifact);
            DeployResult result = system.deploy(session, deployRequest);
            log.info("transfer artifact " + groupId + ":" + artifactId + ":" + version + " to central success.");
        } catch (DeploymentException | IOException e) {
            throw new SchemaException("Deploy jar file: " + jarFile + " failed", e);
        }
    }
}
