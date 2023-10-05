echo "\n\n select * from table1;"
sh 2020201099.sh "select * from table1;"
echo
echo
echo "\n\n select a,c,e from table1,table2;"	
sh 2020201099.sh "select a,c,e from table1,table2;"							

echo "\n\n select sum(b) from table1;" 							
sh 2020201099.sh "select sum(b) from table1;" 							

echo "\n\n select average(f) from table2;" 								
sh 2020201099.sh "select average(f) from table2;" 								

echo "\n\n select distinct a,b from table1;"							
sh 2020201099.sh "select distinct a,b from table1;"							
echo "\n\n select distinct a,e,f from table1,table2;"  					
sh 2020201099.sh "select distinct a,e,f from table1,table2;"  					

echo "\n\n select a,b from table1 where a < 5 and b <= 15;" 				
sh 2020201099.sh "select a,b from table1 where a < 5 and b <= 15;" 				
echo "\n\n select a,e from table1,table2 where a > 7 or b < 14;" 			
sh 2020201099.sh "select a,e from table1,table2 where a > 7 or b < 14;" 			

echo "\n\n select a,min(b) from table1 group by a;" 					
sh 2020201099.sh "select a,min(b) from table1 group by a;" 					
echo "\n\n select c from table1 group by c;" 						
sh 2020201099.sh "select c from table1 group by c;" 						

echo "\n\n select a from table1 order by a DESC;" 					
sh 2020201099.sh "select a from table1 order by a DESC;" 					
echo "\n\n select a,i from table1,table3 order by a ASC;" 				
sh 2020201099.sh "select a,i from table1,table3 order by a ASC;" 				

echo "\n\n select a,min(c) from table1 group by a order by a;" 				
sh 2020201099.sh "select a,min(c) from table1 group by a order by a;" 				
echo "\n\n select distinct a from table1,table3 where a = h;" 				
sh 2020201099.sh "select distinct a from table1,table3 where a = h;" 				
echo "\n\n select a,sum(b) from table1 where a < 5 group by a order by a;" 		
sh 2020201099.sh "select a,sum(b) from table1 where a < 5 group by a order by a;" 		

echo error handling:
echo "\n\n select * from table4;" 					
sh 2020201099.sh "select * from table4;" 					
echo "\n\n select e from table1;"			
sh 2020201099.sh "select e from table1;"			
echo "\n\n select a,sum(b) from table1;"			
sh 2020201099.sh "select a,sum(b) from table1;"			
echo "\n\n select a,b from table1 were a < 5;" 			
sh 2020201099.sh "select a,b from table1 were a < 5;" 			
echo "\n\n select a,b from table1 group by a;" 
sh 2020201099.sh "select a,b from table1 group by a;" 
