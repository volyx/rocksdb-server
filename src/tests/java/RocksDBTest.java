import com.ranksays.rocksdb.client.RocksDB;
import com.ranksays.rocksdb.server.Main;
import com.ranksays.rocksdb.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.locks.Condition;

import static org.junit.Assert.*;

public class RocksDBTest {

	public Thread t;


	@Before
	public void before() throws InterruptedException {


//		final Server server = new Server(8516);
		t = new Thread(() -> {
			try {
				Main.main(new String[0]);
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		t.start();

//		while (!Main.started) {
			Thread.sleep(2_000L);
//		}
		System.out.println("Server started");
	}

	@After
	public void after() throws InterruptedException {


		t.interrupt();

		Thread.sleep(1_000L);
		System.out.println("Server stopped");
	}

	@Test(timeout = 5_000L)
	public void testBasic() throws IOException {
		String db = "test";
		String username = "username";
		String password = "password";

		RocksDB rdb = new RocksDB(username, password);

		byte[] key = "k".getBytes();
		byte[] value = "v".getBytes();

		// get
		assertNull(rdb.get(db, key));

		// put
		rdb.put(db, key, value);
		assertArrayEquals(value, rdb.get(db, key));

		// remove
		rdb.remove(db, key);
		assertNull(rdb.get(db, key));

		rdb.dropDatabase(db);
	}

	@Test
	public void testBatch() throws IOException {
		String db = "test";
		String username = "username";
		String password = "password";

		RocksDB rdb = new RocksDB(username, password);

		byte[][] keys1 = { "k1".getBytes(), "k2".getBytes(), "k3".getBytes() };
		byte[][] keys2 = { keys1[0], keys1[2] };
		byte[][] values1 = { "k1".getBytes(), "k2".getBytes(), "k3".getBytes() };
		byte[][] values2 = { null, values1[1], null };

		// batch put
		rdb.putBatch(db, keys1, values1);

		// batch get
		byte[][] values = rdb.getBatch(db, keys1);
		for (int i = 0; i < values1.length; i++) {
			assertArrayEquals(values1[i], values[i]);
		}

		// batch remove
		rdb.removeBatch(db, keys2);

		// batch get
		values = rdb.getBatch(db, keys1);
		for (int i = 0; i < values2.length; i++) {
			assertArrayEquals(values2[i], values[i]);
		}

		rdb.dropDatabase(db);
	}
}