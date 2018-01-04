/*package com.toyota.pqssurvey

abstract class Animal[A]{
  
  def Name : A
  
}

abstract class Type[A] extends Animal[A]{
  
  def add(a:A ,b:A) : A
  
}

object AbstractClass {
  
  def main(args: Array[String]) {
    
    implicit object StringObj extends Type[String]{
      
      def add(x:String,y:String) : String = x concat y;
      
      def name : String = "Lion";
      
    }
    
    
    implicit object IntObj extends Type[Int]{
      
      def add(x:Int,b:Int): Int = x+b;
      
      def name : String = "Lion";
      
    }
    
    def sum[A](xs:List[A])(implicit m:Type[A]): A ={
      
       m.add(xs.head,sum(xs.tail))
      
    }
    
    println(sum(List(1,2,3)));
    println(sum(List("a","b","c")));
    
  }
  
  
}*/