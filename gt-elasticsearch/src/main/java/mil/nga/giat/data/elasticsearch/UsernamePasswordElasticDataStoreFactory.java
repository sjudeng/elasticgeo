package mil.nga.giat.data.elasticsearch;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import mil.nga.giat.data.elasticsearch.ElasticDataStore.ArrayEncoding;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.geotools.data.DataStore;
import org.geotools.util.logging.Logging;

/**
 * Data store factory that creates {@linkplain ElasticDataStore} instances.
 *
 */

public class UsernamePasswordElasticDataStoreFactory extends ElasticDataStoreFactory
{

	/** The logger for this class */
	protected final static Logger LOGGER = Logging.getLogger(UsernamePasswordElasticDataStoreFactory.class);

	/** The admin user parameter */
	public static final Param ADMIN_USER = new Param("admin_user", String.class, "Admin User", false);

	/** The admin password parameter */
	public static final Param ADMIN_PASSWD = new Param("admin_passwd", String.class, "Admin Password (encrypted)", false);

	/** The proxy user parameter */
	public static final Param PROXY_USER = new Param("proxy_user", String.class, "Proxy User", false);

	/** The proxy password parameter */
	public static final Param PROXY_PASSWD = new Param("proxy_passwd", String.class, "Proxy Password (encrypted)", false);

	/** The display name string in the list of datastores */
	protected static final String DISPLAY_NAME = "Elasticsearch";

	/** The display name description of this datastore */
	protected static final String DESCRIPTION = "Elasticsearch Index with Username and Password Connection";

	/** All the parameters for this data store */
	protected static final Param[] PARAMS = {
		HOSTNAME,
		HOSTPORT,
		INDEX_NAME,
		DEFAULT_MAX_FEATURES,
		ADMIN_USER,
		ADMIN_PASSWD,  
		PROXY_USER,
		PROXY_PASSWD,		 
		SOURCE_FILTERING_ENABLED,
		SCROLL_ENABLED,
		SCROLL_SIZE,
		SCROLL_TIME_SECONDS,
		ARRAY_ENCODING,
		GRID_SIZE,
		GRID_THRESHOLD
	};

	/** Counter of HTTP threads we generate */
	protected static final AtomicInteger httpThreads = new AtomicInteger(1);

	@Override
	public String getDisplayName() {
		return DISPLAY_NAME;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION;
	}

	@Override
	public Param[] getParametersInfo() {
		return PARAMS;
	}

    protected RestClient createRestClient(Map<String, Serializable> params) throws IOException {
		final String searchHost = getValue(HOSTNAME, params);
		final Integer hostPort = getValue(HOSTPORT, params);
		final String indexName = (String) INDEX_NAME.lookUp(params);
		final String arrayEncoding = getValue(ARRAY_ENCODING, params);

		String[] nodes = searchHost.split(",");
		final AuthScope[] auths = new AuthScope[nodes.length];
		final HttpHost[] hosts = new HttpHost[nodes.length];

		for (int i = 0; i < nodes.length; i++) {
			String node = nodes[i];
			auths[i] = new AuthScope(node, hostPort);
			hosts[i] = new HttpHost(node, hostPort, "https");
		}

		final RestClientBuilder adminBuilder = RestClient.builder(hosts);
		final RestClientBuilder proxyBuilder = RestClient.builder(hosts);

		final String adminUser = getValue(ADMIN_USER, params);
		final String adminPasswd = getValue(ADMIN_PASSWD, params);
		adminBuilder.setRequestConfigCallback((b) -> {
			LOGGER.fine("Calling Admin setRequestConfigCallback");
			return b.setAuthenticationEnabled(true);
		});
		adminBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				LOGGER.fine("Calling Admin customizeHttpClient");
				httpClientBuilder.setThreadFactory(new ThreadFactory() {
					@Override
					public Thread newThread(Runnable run) {
						Thread t = new Thread(run);
						t.setDaemon(true);
						t.setName(String.format("esrest-asynchttp-admin-%d", httpThreads.getAndIncrement()));
						return t;
					}
				});
				httpClientBuilder.useSystemProperties();
				CredentialsProvider cp = new BasicCredentialsProvider();
				Credentials creds = new org.apache.http.auth.UsernamePasswordCredentials(adminUser, adminPasswd);
				for (AuthScope scope : auths)
					cp.setCredentials(scope, creds);

				httpClientBuilder.setDefaultCredentialsProvider(cp);
				return httpClientBuilder;
			}
		});

		final String proxyUser = getValue(PROXY_USER, params);
		final String proxyPasswd = getValue(PROXY_PASSWD, params);
		proxyBuilder.setRequestConfigCallback((b) -> {
			LOGGER.fine("Calling Proxy setRequestConfigCallback");
			return b.setAuthenticationEnabled(true);
		});
		proxyBuilder.setHttpClientConfigCallback(new HttpClientConfigCallback() {
			@Override
			public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
				LOGGER.fine("Calling Proxy customizeHttpClient");
				httpClientBuilder.setThreadFactory(new ThreadFactory() {
					@Override
					public Thread newThread(Runnable run) {
						Thread t = new Thread(run);
						t.setDaemon(true);
						t.setName(String.format("esrest-asynchttp-proxy-%d", httpThreads.getAndIncrement()));
						return t;
					}
				});
				httpClientBuilder.useSystemProperties();
				CredentialsProvider cp = new BasicCredentialsProvider();
				Credentials creds = new org.apache.http.auth.UsernamePasswordCredentials(proxyUser, proxyPasswd);
				for (AuthScope scope : auths)
					cp.setCredentials(scope, creds);

				httpClientBuilder.setDefaultCredentialsProvider(cp);
				return httpClientBuilder;
			}
		});

        return adminBuilder.build();
    }	
}
