bash 2018201063.sh "select max(A) from table1;" >> output/1.txt
bash 2018201063.sh "select min(B) from table2;" >> output/2.txt
bash 2018201063.sh "select avg(C) from table1;" >> output/3.txt
bash 2018201063.sh "select sum(D) from table2;" >> output/4.txt
bash 2018201063.sh "select A,D from table1,table2;" >> output/5.txt
bash 2018201063.sh "select distinct C from table1;" >> output/6.txt
bash 2018201063.sh "select B,C from table1 where A=-900;" >> output/7.txt
bash 2018201063.sh "select A,B from table1 where A=775 OR B=803;" >> output/8.txt
bash 2018201063.sh "select * from table1,table2;" >> output/9.txt
bash 2018201063.sh "select * from table1,table2 where table1.B=table2.B;" >> output/10.txt
bash 2018201063.sh "select A,D from table1,table2 where table1.B=table2.B;" >> output/11.txt
bash 2018201063.sh "select table1.C from table1,table2 where table1.A<table2.B;" >> output/12.txt
bash 2018201063.sh "select A from table4;" >> output/13.txt
bash 2018201063.sh "select Z from table1;" >> output/14.txt
bash 2018201063.sh "select B from table1,table2;" >> output/15.txt
bash 2018201063.sh "select distinct A,B from table1;" >> output/16.txt
bash 2018201063.sh "select table1.C from table1,table2 where table1.A<table2.D OR table1.A>table2.B;" >> output/17.txt
bash 2018201063.sh "select table1.C from table1,table2 where table1.A=table2.D;" >> output/18.txt
bash 2018201063.sh "select table1.A from table1,table2 where table1.A<table2.B AND table1.A>table2.D;" >> output/19.txt
bash 2018201063.sh "select sum(table1.A) from table1,table2;" >> output/20.txt