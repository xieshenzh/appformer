/*
 * Copyright 2019 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.uberfire.java.nio.fs.k8s;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uberfire.java.nio.file.FileSystem;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.file.spi.FileSystemProvider;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class K8SFileSystemTest {

    @ClassRule
    public static KubernetesServer SERVER = new KubernetesServer(false, true);
    // The default namespace for MockKubernetes Server is 'test'
    protected static String TEST_NAMESPACE = "test";
    protected static ThreadLocal<KubernetesClient> CLIENT_FACTORY =
            new ThreadLocal<>().withInitial(() -> SERVER.getClient());

    protected static final FileSystemProvider fsProvider = new K8SFileSystemProvider() {

        @Override
        public KubernetesClient createKubernetesClient() {
            return CLIENT_FACTORY.get();
        }

    };

    @BeforeClass
    public static void setup() {
        // Load testing KieServerState ConfigMap data into mock server from file
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SFileSystemTest.class.getResourceAsStream("/test-k8sfs-dir-r-configmap.yml"))
                                                     .get());
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SFileSystemTest.class.getResourceAsStream("/test-k8sfs-dir-0-configmap.yml"))
                                                     .get());
        CLIENT_FACTORY.get()
                      .configMaps()
                      .inNamespace(TEST_NAMESPACE)
                      .createOrReplace(CLIENT_FACTORY.get().configMaps()
                                                     .load(K8SFileSystemTest.class.getResourceAsStream("/test-k8sfs-file-configmap.yml"))
                                                     .get());
    }

    @AfterClass
    public static void tearDown() {
        CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE).delete();
        CLIENT_FACTORY.get().close();
    }

    @Test
    public void simpleRootTests() throws URISyntaxException {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        final Path root = fileSystem.getPath("/");
        Map<String, String> ne = K8SFileSystemUtils.getFsObjNameElementLabel(root);

        assertThat(root).isEqualTo(fileSystem.getPath("/path").getRoot());
        assertThat(root.getRoot().equals(root)).isTrue();
        assertThat(root.toString().equals("/")).isTrue();
        assertThat(root.toRealPath().toString().equals("/")).isTrue();
        assertThat(root.getParent()).isNull();
        assertThat(root.getFileName()).isNull();
        assertThat(root.getNameCount()).isEqualTo(0);
        assertThat(root.iterator().hasNext()).isEqualTo(false);
        assertThat(K8SFileSystemUtils.getFileNameString(root).equals("/")).isTrue();
        assertThat(ne.size()).isEqualTo(0);
    }

    @Test
    public void testGetFsObjNameElement() {
        final FileSystem fileSystem = fsProvider.getFileSystem(URI.create("default:///"));
        final Path aFile = fileSystem.getPath("/testDir/../testDir/./testFile");
        Map<String, String> ne = K8SFileSystemUtils.getFsObjNameElementLabel(aFile);
        assertThat(ne.size()).isEqualTo(2);
        assertThat(ne.containsValue("testDir")).isTrue();
        assertThat(ne.containsValue("testFile")).isTrue();
    }

    @Test
    public void tesGetCMByName() {
        assertThat(CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                 .withName("dummy").get()).isNull();
        assertThat(CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                 .withName("k8s-fsobj-86403b0c-78b7-11e9-ad76-8c16458eff35").get()).isNotNull();
        assertThat(CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                 .withName("k8s-fsobj-e6bb5ba5-527f-11e9-8a93-8c16458eff35").get()).isNotNull();
    }

    @Test
    public void testGetCreationTime() {
        ConfigMap cfm = CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                      .withName("k8s-fsobj-86403b0c-78b7-11e9-ad76-8c16458eff35").get();
        assertThat(K8SFileSystemUtils.getCreationTime(cfm)).isEqualTo(0);
    }

    @Test
    public void testGetSize() {
        ConfigMap cfm = CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                      .withName("k8s-fsobj-86403b0c-78b7-11e9-ad76-8c16458eff35").get();
        assertThat(K8SFileSystemUtils.getSize(cfm)).isEqualTo(19);
    }

    @Test
    public void testIsFile() {
        ConfigMap cfm = CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                      .withName("k8s-fsobj-86403b0c-78b7-11e9-ad76-8c16458eff35").get();
        assertThat(K8SFileSystemUtils.isFile(cfm)).isTrue();
        assertThat(K8SFileSystemUtils.isDirectory(cfm)).isFalse();
    }

    @Test
    public void testIsDir() {
        ConfigMap cfm = CLIENT_FACTORY.get().configMaps().inNamespace(TEST_NAMESPACE)
                                      .withName("k8s-fsobj-e6bb5ba5-527f-11e9-8a93-8c16458eff35").get();
        assertThat(K8SFileSystemUtils.isFile(cfm)).isFalse();
        assertThat(K8SFileSystemUtils.isDirectory(cfm)).isTrue();
    }

}
