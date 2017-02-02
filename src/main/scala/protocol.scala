import io.netty.channel._
import io.netty.handler.codec.MessageToMessageDecoder
import spray.json._
import DefaultJsonProtocol._
import com.twitter.concurrent.Broker

class FranzDecoder extends MessageToMessageDecoder[String] {
  override def decode(ctx: ChannelHandlerContext, msg: String, out: java.util.List[Object]) = {
    implicit val dec = jsonFormat3(FranzProtocol)
    val data = msg.parseJson.convertTo[FranzProtocol]
    out.add(data)
  }
}

case class FranzProtocol(command: String, topic: String, payload: String)
