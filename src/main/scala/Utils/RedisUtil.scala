package Utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}


object RedisUtil {

     val jedisPool = new  JedisPool(new GenericObjectPoolConfig,"hadoop")
      def getJedis():Jedis={
        jedisPool.getResource
      }
}
