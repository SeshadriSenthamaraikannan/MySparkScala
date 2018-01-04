package helloworld

object FunctionsAndExpression {
  
  def main(args: Array[String]){
    
    val radius = 24
    
    val getArea = (radius:Double) => {
      
      val PI = 3.14;
      
      PI * radius * radius
      
    }:Double
    
    println(getArea(radius))
    
  def MyArea(radius:Double):Double = {
      
      val PI = 3.14;
      
      PI * radius * radius
      
    }
    
    println(MyArea(radius));
  
  val getMyArea:(Double)=> Double = MyArea
  
  getMyArea(radius);
  
  def myMeth[k,v](K:k,V:v)={
   
    val keyval = K.getClass();
    val valueVal = V.getClass();
    println(s"$K and $V are of types $keyval and $valueVal");          
  }
  
  myMeth("Seshadri",7)
  
  val MyList = List("Sesha",1);
  
  val newList = List("Sent",2);
  
  val FinalList = MyList ++ newList
  
  val ZippedList = MyList zip newList
  
  ZippedList.foreach(println)
  
  val NewMap = ZippedList.toMap
  
  println(NewMap.values)
  
  def fraction(Numer:Int,Den:Int): Option[Double] = {
    
    if(Den == 0)  None
    else Option(Numer/Den)
    
  }
  
  val PI = fraction(22,0)
  
val NEWPI = PI match{
    case Some(pi) => pi
    
    case None => "Please give denominor more than 0"
    
  }
  
  println(NEWPI);
  
  val MapCheck = util.Try(NewMap("Seshadri"));
  
  println(MapCheck);
  
  val DataList = List("sesha","Sesha","Chennai","Pondy");
  
  println(DataList.sortBy(_(0)))
  
  }
   
  
}