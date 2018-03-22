package study.lsw.sql

case class Person_missing (name : String, age : Option [Int], Job : Option [String])

case class Person (name : String, age : Long, Job : String)   

case class Friends(name: String, friends: String, no : Int)
case class Friends_Missing(name: String, friends: Option[String])

case class Characters(name: String, 
                      height: Integer, 
                      weight: Option[Integer], 
                      eyecolor: Option[String], 
                      haircolor: Option[String], 
                      jedi: String,
                      species: String)
                      
case class Characters_BadType(name: String,
                      height: Integer, 
                      weight: Integer, 
                      eyecolor: String, 
                      haircolor: String, 
                      jedi: String,
                      species: String)                      

