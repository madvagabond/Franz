import spray.json._
import DefaultJsonProtocol._
import java.nio.{CharBuffer, ByteBuffer}
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.Charset
import java.net.InetSocketAddress

object FranzClient {
  def serializeMsg(msg: FranzProtocol): String = {
    implicit val js = jsonFormat3(FranzProtocol)
    msg.toJson.toString
  }
  def serializeAndSend(addr: String, msg: FranzProtocol) = {
    val pl = serializeMsg(msg)
    sendMessage(addr, pl)
  }
  def sendMessage(address: String, msg: String) = {
    val chan = AsynchronousSocketChannel.open
    //val cs = Charset.forName("UTF-8")
    //val cb = CharBuffer.wrap(msg.getBytes())
    val buff = ByteBuffer.wrap(msg.getBytes())
    chan.connect(new InetSocketAddress(address, 3012)).get()
    chan.write(buff).get()
    buff.clear()
    chan
  }
  def readChan(ch: AsynchronousSocketChannel) = {
    var buffer = ByteBuffer.allocateDirect(1024)
    val d = ch.read(buffer).get()
    buffer.flip
    Charset.forName("UTF-8").decode(buffer).toString
  }

  def subscribe(address: String, topic: String, callback: String => Unit): Unit = {
    val payload = FranzProtocol("Subscribe", topic, "")
    val s = serializeAndSend(address, payload)
    def subLoop(sock: AsynchronousSocketChannel) {
      val res = readChan(sock)
      callback(res)
      subLoop(sock)
    }
    subLoop(s)
  }

  def publish(address: String, topic: String, payload: String) = {
    val msg = FranzProtocol("Publish", topic, payload)
    val s = serializeAndSend(address, msg)
    readChan(s)
  }
  def newTopic(address: String, topic: String) = {
    val msg = FranzProtocol("New Topic", topic, "")
    val s = serializeAndSend(address, msg)
    readChan(s)
  }
}
