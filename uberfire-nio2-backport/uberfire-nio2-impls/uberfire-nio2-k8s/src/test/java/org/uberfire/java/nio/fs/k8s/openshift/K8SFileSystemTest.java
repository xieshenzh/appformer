package org.uberfire.java.nio.fs.k8s.openshift;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class K8SFileSystemTest {

    @Test
    @Category(org.uberfire.java.nio.fs.k8s.openshift.OpenshiftTest.class)
    public void test_test() {
        assertThat(1 == 1).isTrue();
    }
}
