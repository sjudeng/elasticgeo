/**
 * This file is hereby placed into the Public Domain. This means anyone is
 * free to do whatever they wish with this file.
 */
package mil.nga.giat.data.elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.HttpHost;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class UsernamePasswordElasticDatastoreFactoryTest {

    @Test
    public void testFactoryDefaults() throws IOException {
        UsernamePasswordElasticDataStoreFactory fac = new UsernamePasswordElasticDataStoreFactory();

        assertTrue(fac.getDisplayName().equals(UsernamePasswordElasticDataStoreFactory.DISPLAY_NAME));
        assertTrue(fac.getDescription().equals(UsernamePasswordElasticDataStoreFactory.DESCRIPTION));
        assertTrue(fac.getParametersInfo().equals(UsernamePasswordElasticDataStoreFactory.PARAMS));
        assertTrue(fac.getImplementationHints()==null);

    }

    @Test
    public void testFactoryWithMissingRequired() throws IOException {
    	UsernamePasswordElasticDataStoreFactory fac = new UsernamePasswordElasticDataStoreFactory();
        assertTrue(fac.isAvailable());

        Map<String,Serializable> map = new HashMap<>();
        map.put(UsernamePasswordElasticDataStoreFactory.HOSTNAME.key, "localhost");
        map.put(UsernamePasswordElasticDataStoreFactory.HOSTPORT.key, 9200);

        assertTrue(!fac.canProcess(map));

    }

    @Test
    public void testCreateRestClient() throws IOException {
        assertEquals(ImmutableList.of(new HttpHost("localhost", 9200, "http")), getHosts("localhost"));
        assertEquals(ImmutableList.of(new HttpHost("localhost.localdomain", 9200, "http")), getHosts("localhost.localdomain"));

        assertEquals(ImmutableList.of(new HttpHost("localhost", 9201, "http")), getHosts("localhost:9201"));
        assertEquals(ImmutableList.of(new HttpHost("localhost.localdomain", 9201, "http")), getHosts("localhost.localdomain:9201"));

        assertEquals(ImmutableList.of(new HttpHost("localhost", 9200, "http")), getHosts("http://localhost"));
        assertEquals(ImmutableList.of(new HttpHost("localhost", 9200, "http")), getHosts("http://localhost:9200"));
        assertEquals(ImmutableList.of(new HttpHost("localhost", 9201, "http")), getHosts("http://localhost:9201"));

        assertEquals(ImmutableList.of(new HttpHost("localhost", 9200, "https")), getHosts("https://localhost"));
        assertEquals(ImmutableList.of(new HttpHost("localhost", 9200, "https")), getHosts("https://localhost:9200"));
        assertEquals(ImmutableList.of(new HttpHost("localhost", 9201, "https")), getHosts("https://localhost:9201"));

        assertEquals(ImmutableList.of(
                new HttpHost("somehost.somedomain", 9200, "http"),
                new HttpHost("anotherhost.somedomain", 9200, "http")),
                getHosts("somehost.somedomain:9200,anotherhost.somedomain:9200"));
        assertEquals(ImmutableList.of(
                new HttpHost("somehost.somedomain", 9200, "https"),
                new HttpHost("anotherhost.somedomain", 9200, "https")),
                getHosts("https://somehost.somedomain:9200,https://anotherhost.somedomain:9200"));
        assertEquals(ImmutableList.of(
                new HttpHost("somehost.somedomain", 9200, "https"),
                new HttpHost("anotherhost.somedomain", 9200, "https")),
                getHosts("https://somehost.somedomain:9200, https://anotherhost.somedomain:9200"));
        assertEquals(ImmutableList.of(
                new HttpHost("somehost.somedomain", 9200, "https"),
                new HttpHost("anotherhost.somedomain", 9200, "http")),
                getHosts("https://somehost.somedomain:9200,anotherhost.somedomain:9200"));
    }

    private List<HttpHost> getHosts(String hosts) throws IOException {
        Map<String,Serializable> params = new HashMap<>();
        params.put(UsernamePasswordElasticDataStoreFactory.HOSTNAME.key, hosts);
        params.put(UsernamePasswordElasticDataStoreFactory.HOSTPORT.key, 9200);
        params.put(UsernamePasswordElasticDataStoreFactory.ADMIN_USER.key, "admin");
        params.put(UsernamePasswordElasticDataStoreFactory.ADMIN_PASSWD.key, "admin");
        params.put(UsernamePasswordElasticDataStoreFactory.PROXY_USER.key, "admin");
        params.put(UsernamePasswordElasticDataStoreFactory.PROXY_PASSWD.key, "admin");
        params.put(ElasticDataStoreFactory.INDEX_NAME.key, "index");
        params.put(ElasticDataStoreFactory.SCROLL_ENABLED.key, true);
        params.put(ElasticDataStoreFactory.SCROLL_SIZE.key, 20);
        UsernamePasswordElasticDataStoreFactory factory = new UsernamePasswordElasticDataStoreFactory();
        return factory.createRestClient(params).getNodes().stream().map(node -> node.getHost()).collect(Collectors.toList());
    }

}
