# Franz
A simple topic based pub sub broker. 

Usage: 
  val addr = "127.0.0.1"
  FranzClient.newTopic(addr, "bullet")
  
  def f(num: Int)(x: String) = println(s" client ${num} ${x}")

  val t2 = new Thread{override def run = FranzClient.subscribe(addr, "bullet", f(1))}
   
  val t3 = new Thread{override def run = FranzClient.subscribe(addr, "bullet", f(2))}
  
  t2.start
  
  t3.start

  Thread.sleep(5)
  
  for (x <- 1 to 30) FranzClient.publish(addr, "bullet", "pew pew pew")
