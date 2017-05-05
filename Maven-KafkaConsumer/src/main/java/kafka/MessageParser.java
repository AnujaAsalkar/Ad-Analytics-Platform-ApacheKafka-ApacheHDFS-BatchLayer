package kafka;

public class MessageParser {
	
	public static void parseMessage(String message){
		System.out.println("From parseMessage:"+message);
		
		String [] productCategory=null;
		String [] ageMax=null;
		String [] ageMin=null;
		String [] gender=null;
		String [] budget=null;
		String [] city=null;
			
		String [] fields=message.split(",");
	
		productCategory=fields[2].split(":");
		ageMax=fields[4].split(":");
		ageMin=fields[5].split(":");
		gender=fields[6].split(":");
		budget=fields[7].split(":");
		city=fields[8].split(":");
		
		System.out.println("Displaying content:\nProduct Category:"+productCategory[1].trim().substring(1,productCategory[1].length()-2)+ " " + 
		"AgeMax:" +ageMax[1].trim().substring(1,ageMax[1].length()-1) + " " +
		"AgeMin:" +ageMin[1].trim().substring(1,ageMin[1].length()-1) + " " + 
		"Gender:" +gender[1].trim().substring(1,gender[1].length()-1) + " " +
		"Budget:" +budget[1].trim().substring(1,budget[1].length()-1) + " " +
		"City:" +city[1].trim().substring(1, city[1].length()-2));
			
	}

}