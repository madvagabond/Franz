import io.netty.channel._
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.string._
import io.netty.handler.codec.bytes.ByteArrayDecoder
import com.twitter.concurrent.{Broker, Offer}
import scala.collection.concurrent.TrieMap
import scala.util.Random
trait Messaging
case class handleMe(ctx: ChannelHandlerContext, msg: FranzProtocol)
case class Subscriber(ctx: ChannelHandlerContext) extends Messaging
case class Publisher(ctx: ChannelHandlerContext, payload: String) extends Messaging
class Server(subscriptionTable: TrieMap[String, Broker[Messaging]]) extends SimpleChannelInboundHandler[FranzProtocol] {
  override def channelRead0(ctx: ChannelHandlerContext, msg: FranzProtocol) = {
    msg match {
      case FranzProtocol("Publish", topic, pl) if (subscriptionTable.contains(topic)) =>
        subscriptionTable.get(topic).get.send(Publisher(ctx, pl)).sync
      case FranzProtocol("New Topic", topic, payload) if (!subscriptionTable.contains(topic)) =>
        val br = new Broker[Messaging]
        subscriptionTable.put(topic, br)
        logic.topicHandler(br)
        ctx.writeAndFlush(s"Topic ${topic} added")
        ctx.close()
      case FranzProtocol("Subscribe", topic, pl) if (subscriptionTable.contains(topic)) =>
        subscriptionTable.get(topic).get.send(Subscriber(ctx)).sync
      case _ =>
        ctx.close()
    }
  }
}
object logic {
  def dispatchRandom(clients: List[ChannelHandlerContext]): ChannelHandlerContext = {
    val rnd = new Random()
    clients( rnd.nextInt(clients.size) )
  }
  def topicHandler(broker: Broker[Messaging]) = {
    def loop(t: Set[ChannelHandlerContext]): Unit = {
      broker.recv { msg =>
        msg match {
          case Publisher(ctx, pl) if (pl != "") =>
            ctx.writeAndFlush(s"${pl} published")
            ctx.close()
            publishMessage(t, pl)
            loop(t)
          case Subscriber(ctx) =>
            ctx.flush()
            loop(t + ctx)
        }
      }.sync()

    }
    loop(Set[ChannelHandlerContext]())
  }
  def publishMessage(t: Set[ChannelHandlerContext], msg: String) = {
    val rcvr = dispatchRandom(t.toList)
    rcvr.writeAndFlush(msg)
  }
}
class InitializeFranz(subscriptionTable: TrieMap[String, Broker[Messaging]]) extends ChannelInitializer[SocketChannel] {
  override def initChannel(ch: SocketChannel) = {
    val pipe = ch.pipeline()
    pipe.addLast(new StringDecoder())
    pipe.addLast(new StringEncoder())
    pipe.addLast(new FranzDecoder())
    pipe.addLast(new Server(subscriptionTable))
  }
}
