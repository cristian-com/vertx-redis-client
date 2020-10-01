package io.vertx.test.redis;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class RedisClusterClientTest {

  @Parameterized.Parameters
  public static Iterable<String> redisVersions() {
    return Arrays.asList("redis:5-alpine", "redis:6-alpine");
  }

  public final Network network = Network.newNetwork();

  @Rule
  public final GenericContainer<?> redis7000;
  @Rule
  public final GenericContainer<?> redis7001;
  @Rule
  public final GenericContainer<?> redis7002;
  @Rule
  public final GenericContainer<?> redis7003;
  @Rule
  public final GenericContainer<?> redis7004;
  @Rule
  public final GenericContainer<?> redis7005;

  public GenericContainer<?> redisCli;

  public RedisClusterClientTest(String redisVersion) {
   redis7000 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redis7001 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redis7002 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redis7003 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redis7004 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redis7005 = new GenericContainer<>(redisVersion)
      .withExposedPorts(6379)
      .withNetwork(network)
      .withCommand("redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 --appendonly yes");

    redisCli = new GenericContainer<>(redisVersion)
      .withNetwork(network).waitingFor(
        Wait.forLogMessage(".*OK.*", 2)
      );
  }

  @Rule
  public final RunTestOnContext rule = new RunTestOnContext();

  private Redis client;

  @Before
  public void before(TestContext should) {
    final Async before = should.async();
    rule.vertx().executeBlocking(handler -> {
      List<GenericContainer<?>> containerList = new ArrayList<>();
      containerList.add(redis7000);
      containerList.add(redis7001);
      containerList.add(redis7002);
      containerList.add(redis7003);
      containerList.add(redis7004);
      containerList.add(redis7005);
      StringBuilder stringBuilder = new StringBuilder();
      for (GenericContainer<?> container : containerList) {
        String ip = container.getContainerInfo().getNetworkSettings().getNetworks().values().stream().findFirst().get().getIpAddress();
        stringBuilder.append(ip).append(":").append("6379").append(" ");
      }

      String connectionString = stringBuilder.substring(0, stringBuilder.length() - 1);
      redisCli.setCommand(String.format("redis-cli --cluster create %s --cluster-replicas 1 --cluster-yes", connectionString));
      redisCli.start();
      handler.complete();
    }).onSuccess(handler -> {
      client = Redis.createClient(
        rule.vertx(), new RedisOptions()
          .setType(RedisClientType.CLUSTER)
          .addConnectionString("redis://" + redis7000.getContainerIpAddress() + ":" + redis7000.getFirstMappedPort())
      );
      before.complete();
    }).onFailure(should::fail);
  }

  @After
  public void after() {
    client.close();
    redisCli.close();
  }

  @Test(timeout = 10_000L)
  public void testConnect(TestContext should) {
    final Async test = should.async();

    client.connect(onConnect -> {
      if (onConnect.failed()) {
        should.fail(onConnect.cause());
      }
      should.assertTrue(onConnect.succeeded());
      test.complete();
    });
  }
}
