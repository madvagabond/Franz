import io.netty.channel._
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import scala.collection.concurrent.TrieMap
import com.twitter.concurrent.Broker


object mainMan extends App {
  val grp = new NioEventLoopGroup()
  val bs = new ServerBootstrap()
  val brokerHandler = TrieMap[String, Broker[Messaging]]()
  val rtbrk = new Broker[handleMe]
  bs.group(grp)
    .channel(classOf[NioServerSocketChannel])
    .childHandler(new InitializeFranz(brokerHandler))
  bs.bind(3012)
}
