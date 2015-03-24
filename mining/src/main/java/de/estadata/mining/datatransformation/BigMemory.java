package de.estadata.mining.datatransformation;

import net.sf.ehcache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

public class BigMemory {

	private static CacheManager manager = null;
	private static final Logger log = LoggerFactory.getLogger("application");

	public static CacheManager memoryDB() {
		try {
			URI config = BigMemory.class.getClassLoader()
					.getResource("./ehcache.xml").toURI();
			return memoryDB(new File(config));
		} catch (URISyntaxException e) {
			log.error("Could not load Terracota configuration file", e);
		}
		return null;
	}

	public static CacheManager memoryDB(File config) {
		if (manager != null) {
			return manager;
		} else {
			System.setProperty("net.sf.ehcache.pool.sizeof.AgentSizeOf.bypass",
					"true");
			System.setProperty("com.tc.productkey.path",
					"terracotta-license.key");
			try {
				manager = CacheManager.create(config.toURI().toURL());
			} catch (MalformedURLException e) {
				log.error("Could not load Terracota configuration file", e);
			}

			System.out.println("**** bm1 and bm2 share ****"
					+ manager.getConfiguration().getMaxBytesLocalHeap()
					+ "b heap and "
					+ manager.getConfiguration().getMaxBytesLocalOffHeap()
					+ "b off-heap");
			System.out.println("**** Successfully configured with ARC **** ");
			return manager;
		}
	}

}
