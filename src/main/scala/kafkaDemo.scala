import Utils.RedisUtil
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

/*
 *Kafka使用MySQL维护offset
 */
object kafkaDemo {
  def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()

    val kafkaParams = Map(
      "metadata.broker.list" -> load.getString("kafka.broker.list"),
      "group.id" -> load.getString("kafka.group.id"),
      "auto.offset.reset" -> "smallest"
    )
    val topic = load.getString("kafka.topics").split(",").toSet[String]
    //创建streaming
    val conf = new SparkConf()
    conf.setAppName("实时统计")
    conf.setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(2))
    //DB读取配置文件
    DBs.setup()
    //使用SQL查询数据哭获得偏移量
    val fromOffsets: Map[TopicAndPartition, Long] = DB.readOnly { implicit session =>
      sql"select * from Streaming_offset where groupid=?".bind(load.getString("kafka.group.id")).map(
        rs => {
          (TopicAndPartition(rs.string("topic"), rs.int("partitions")), rs.long("offset"))
        }).list().apply()
    }.toMap
    /**
      * 创建stream
      *
      */
    val stream = if (fromOffsets.size == 0) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topic)
    } else {
      var checkedOffset = Map[TopicAndPartition, Long]()
      //创建kafka的 客户端
      val kafkaCluster = new KafkaCluster(kafkaParams)
      //使用kafkaCluster.getEarliestLeaderOffsets获得Either
      val earliestLeaderOffsets: Either[Err, Map[TopicAndPartition, KafkaCluster.LeaderOffset]] = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)

      if (earliestLeaderOffsets.isRight) {
        //.right获得Map[TopicAndPartition, KafkaCluster.LeaderOffset]
        val value = earliestLeaderOffsets.right.get
        //开始对比
        checkedOffset = fromOffsets.map(owner => {
          val maybeOffset = value.get(owner._1).get.offset
          if (owner._2 >= maybeOffset) owner
          else {
            (owner._1, maybeOffset)
          }
        })

      }
      //      ssc: StreamingContext,
      //      kafkaParams: Map[String, String],
      //      fromOffsets: Map[TopicAndPartition, Long],
      //      messageHandler: MessageAndMetadata[K, V]=> R
      val messageHandler = (mm: MessageAndMetadata[String, String]) => mm.message()
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](streamingContext, kafkaParams, checkedOffset, messageHandler)
    }

    stream.foreachRDD(rdd => {
      // rdd.foreach(println)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val client = RedisUtil.getJedis()
      client.hincrBy("","",1)



      //记录offset并向数据库中插入
      offsetRanges.foreach(osr => {
        DB.autoCommit { implicit session =>
          sql"REPLACE INTO Streaming_offset(topic, groupid, partitions, offset) VALUES(?,?,?,?)"
            .bind(osr.topic, load.getString("kafka.group.id"), osr.partition, osr.untilOffset).update().apply()
        }
        // println(s"${osr.topic} ${osr.partition} ${osr.fromOffset} ${osr.untilOffset}")
      })
    })

    //启动程序
    //终止程序
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
