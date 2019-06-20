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

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uberfire.java.nio.file.Path;
import org.uberfire.java.nio.fs.cloud.CloudClientConstants;

public class K8SFileSystemUtils {

    private static final Logger logger = LoggerFactory.getLogger(K8SFileSystemUtils.class);
    static final String CFG_MAP_LABEL_FSOBJ_SIZE_KEY = "k8s.fs.nio.java.uberfire.org/fsobj-size";
    static final String CFG_MAP_LABEL_FSOBJ_TYPE_KEY = "k8s.fs.nio.java.uberfire.org/fsobj-type";
    static final String CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX = "k8s.fs.nio.java.uberfire.org/fsobj-name-";
    static final String CFG_MAP_FSOBJ_NAME_PREFIX = "k8s-fsobj-";
    static final String CFG_MAP_FSOBJ_CONTENT_KEY = "fsobj-content";

    private K8SFileSystemUtils() {}

    static Optional<ConfigMap> createOrReplaceParentDirFSCM(KubernetesClient client, Path self, long selfSize) {
        String selfName = getFileNameString(self);
        Path parent = Optional.ofNullable(self.getParent()).orElseThrow(IllegalArgumentException::new);
        Map<String, String> parentContent = Optional.ofNullable(getFsObjCM(client, parent))
                .map(ConfigMap::getData)
                .orElseGet(Collections::emptyMap);
        parentContent.put(selfName, String.valueOf(selfSize));
        final long parentSize = parentContent.values().stream().mapToLong(Long::parseLong).sum();

        return Optional.of(createOrReplaceFSCM(client, parent,
                                               parent.getRoot().equals(parent)
                                                       ? Optional.empty()
                                                       : createOrReplaceParentDirFSCM(client, parent, parentSize),
                                               parentContent,
                                               true));
    }

    static ConfigMap createOrReplaceFSCM(KubernetesClient client,
                                         Path path,
                                         Optional<ConfigMap> parentOpt,
                                         Map<String, String> content,
                                         boolean isDir) {
        String fileName = getFileNameString(path);
        long size = 0;
        Map<String, String> labels = getFsObjNameElementLabel(path);
        if (isDir) {
            if (labels.isEmpty()) {
                labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.ROOT.toString());
            } else {
                labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.DIR.toString());
            }
            size = content.values().stream().mapToLong(Long::parseLong).sum();
        } else {
            labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.FILE.toString());
            size = parentOpt.map(cm -> Long.parseLong(cm.getData().get(fileName)))
                            .orElseThrow(() -> new IllegalStateException("File [" +
                                                                         fileName +
                                                                         "] is not found at parent directory [" +
                                                                         path.toRealPath().getParent().toString() +
                                                                         "]"));
        }
        labels.put(CFG_MAP_LABEL_FSOBJ_SIZE_KEY, String.valueOf(size));
        String cmName = Optional.ofNullable(getFsObjCM(client, path))
                .map(cm -> cm.getMetadata().getName())
                .orElseGet(() -> CFG_MAP_FSOBJ_NAME_PREFIX + UUID.randomUUID().toString());
        return parentOpt.map(parent -> client.configMaps().createOrReplace(new ConfigMapBuilder()
                                             .withNewMetadata()
                                               .withName(cmName)
                                               .withLabels(labels)
                                               .withOwnerReferences(new OwnerReferenceBuilder()
                                                 .withApiVersion(parent.getApiVersion())
                                                 .withKind(parent.getKind())
                                                 .withName(parent.getMetadata().getName())
                                                 .withUid(parent.getMetadata().getUid())
                                                 .build())
                                             .endMetadata()
                                             .withData(content)
                                             .build()))
                        .orElseGet(() -> client.configMaps().createOrReplace(new ConfigMapBuilder()
                                               .withNewMetadata()
                                                 .withName(cmName)
                                                 .withLabels(labels)
                                               .endMetadata()
                                               .withData(content)
                                               .build()));
    }

    static ConfigMap getFsObjCM(KubernetesClient client, Path path) {
        Map<String, String> labels = getFsObjNameElementLabel(path);
        if (labels.isEmpty()) {
            labels.put(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, K8SFileSystemObjectType.ROOT.toString());
        }
        List<ConfigMap> configMaps = client.configMaps()
                                           .withLabels(labels)
                                           .list()
                                           .getItems();
        if (configMaps.size() > 1) {
            throw new IllegalStateException("Ambiguous K8S FileSystem object name: [" + path.toString() +
                                            "]; should not have be associated with more than one " +
                                            "K8S FileSystem ConfigMaps.");
        }
        if (configMaps.size() == 1) {
            return configMaps.get(0);
        }
        return null;
    }

    static byte[] getFsObjContentBytes(ConfigMap cm) {
        byte[] content = new byte[0];
        try {
            content = cm.getData().get(CFG_MAP_FSOBJ_CONTENT_KEY).getBytes(CloudClientConstants.ENCODING);
        } catch (UnsupportedEncodingException e) {
            logger.warn("Invalid encoding [{}], returns zero length byte array content.",
                        CloudClientConstants.ENCODING);
        }
        return content;
    }

    static  Map<String, String> getFsObjNameElementLabel(Path path) {
        Map<String, String> labels = new HashMap<>();
        path.toAbsolutePath().toRealPath().iterator().forEachRemaining(
            subPath -> labels.put(CFG_MAP_LABEL_FSOBJ_NAME_KEY_PREFIX + labels.size(), subPath.toString())
        );
        return labels;
    }
    
    static String getFileNameString(Path path) {
        return Optional.ofNullable(path.getFileName()).map(Path::toString).orElse("/");
    }

    static long getSize(ConfigMap fileCM) {
        return Long.parseLong(fileCM.getMetadata().getLabels().getOrDefault(CFG_MAP_LABEL_FSOBJ_SIZE_KEY, "0"));
    }

    static long getCreationTime(ConfigMap fileCM) {
        return Optional.ofNullable(ZonedDateTime.parse(fileCM.getMetadata().getCreationTimestamp(),
                                                       DateTimeFormatter.ISO_DATE_TIME)
                                                .toInstant()
                                                .getEpochSecond())
                       .orElseGet(() -> Instant.now().getEpochSecond());
    }

    static boolean isFile(ConfigMap fileCM) {
        return K8SFileSystemObjectType.FILE.toString()
                                           .equals(fileCM.getMetadata()
                                                         .getLabels()
                                                         .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, "unknown"));
    }

    static boolean isDirectory(ConfigMap fileCM) {
        return K8SFileSystemObjectType.DIR.toString()
                                          .equals(fileCM.getMetadata()
                                                        .getLabels()
                                                        .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, "unknown"))
               ||                           
               K8SFileSystemObjectType.ROOT.toString()
                .equals(fileCM.getMetadata()
                              .getLabels()
                              .getOrDefault(CFG_MAP_LABEL_FSOBJ_TYPE_KEY, "unknown"));
    }
}
