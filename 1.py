import MySQLdb

f = open("metadata.txt","r")
f_1=[]
for i in f:
    i=i.strip()
    f_1.append(i)

f_1=filter(lambda a:a!='',f_1)
#print f_1

i = 0
tables=[]
attributes=[]
while(i<len(f_1)):
    string = f_1[i];
    if(string=='<begin_table>'):
        i=i+1;
        string=f_1[i];
        tables.append(string);
        i=i+1;
        attr=[]
        string = f_1[i];
        while(i<len(f_1) and string!='<end_table>'):
            attr.append(string);
            i=i+1;
            string=f_1[i];
        attributes.append(attr);
        i=i+1;

#print tables,attributes

db  = MySQLdb.connect("localhost","root","manju123","ENGINE")
cursor = db.cursor()


for i in range(0,len(tables)):
    sti="DROP TABLE IF EXISTS "+tables[i];
    cursor.execute(sti);
    sql = "CREATE TABLE "+tables[i]+" ( ";
    for j in range(0,len(attributes[i])):
        sql =sql+attributes[i][j] + " INT"
        if(j!=len(attributes[i])-1):
            sql=sql+','
    sql+=")"
    #print sql;
    cursor.execute(sql);
    g = open(tables[i]+".csv","r");
    g = g.readlines();
    g_1=[];
    for x in g:
        x = x.strip();
        g_1.append(x);
    #print g_1
    g_2=[]
    for x in g_1:
        x=x.split(',')
        g_2.append(x);
    #print g_2
    for y in range(0,len(g_2)):
        sql="INSERT INTO "+tables[i]+" VALUES(";
        for z in range(0,len(g_2[y])):
            sql+=g_2[y][z];
            if(z!=len(g_2[y])-1):
                sql+=',';
        sql+=')';
        #print sql;
        cursor.execute(sql);
        db.commit();
    

while(1):
    print "mysql>",
    inp=raw_input();
    if(inp=="exit"):
        break;
    if(inp[len(inp)-1]!=';'):
        print "Invalid Syntax: Forgetting semicolon"
        continue;
    dup=inp;
    inp = inp.split(" ")
    if inp[1]=='*':
        table_name = inp[3];
        sql = "SELECT * FROM "+table_name;
	try:
        	cursor.execute(sql)
        	results = cursor.fetchall()
        	if(len(results)==0):
            		print "Empty set (0.00 sec)"
        	for i in results:
           		 for j in i:
                		print j,
            		 print "\n";
		db.commit();
	except:
	        print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	        db.rollback();

    elif(inp[1][0]=='m' and inp[1][1]=='a' and inp[1][2]=='x'):
        table_name=inp[3];
        column_name=""
        for p in range(0,len(inp[1])):
            if(inp[1][p]=='('):
                p=p+1;
                while(p<len(inp[1])-1):
                    column_name+=inp[1][p];
                    p=p+1;
        sql = "SELECT MAX("+column_name+")"+" from "+table_name;
	try:
	    cursor.execute(sql)
            results = cursor.fetchall()
            if(len(results)==0):
                print "Empty set (0.00 sec)"

            for i in results:
                for j in i:
                    print j,
                print "\n";
            db.commit();
        except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();

    elif(inp[1][0]=='m' and inp[1][1]=='i' and inp[1][2]=='n'):
        table_name=inp[3];
        column_name=""
        for p in range(0,len(inp[1])):
            if(inp[1][p]=='('):
                p=p+1;
                while(p<len(inp[1])-1):
                    column_name+=inp[1][p];
                    p=p+1;
        sql = "SELECT MIN("+column_name+")"+" from "+table_name;
	try:
	    cursor.execute(sql)
            results = cursor.fetchall()
            if(len(results)==0):
                print "Empty set (0.00 sec)"

            for i in results:
                for j in i:
                    print j,
                print "\n";
            db.commit();
	except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();
    
    elif(inp[1][0]=='s' and inp[1][1]=='u' and inp[1][2]=='m'):
        table_name=inp[3];
        column_name=""
        for p in range(0,len(inp[1])):
            if(inp[1][p]=='('):
                p=p+1;
                while(p<len(inp[1])-1):
                    column_name+=inp[1][p];
                    p=p+1;
        sql = "SELECT SUM("+column_name+")"+" from "+table_name;
        try:
	    cursor.execute(sql)
            results = cursor.fetchall()
            if(len(results)==0):
                print "Empty set (0.00 sec)"

            for i in results:
                for j in i:
                    print j,
                print "\n";
            db.commit();
	except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();

    elif(inp[1][0]=='a' and inp[1][1]=='v' and inp[1][2]=='g'):
        table_name=inp[3];
        column_name=""
        for p in range(0,len(inp[1])):
            if(inp[1][p]=='('):
                p=p+1;
                while(p<len(inp[1])-1):
                    column_name+=inp[1][p];
                    p=p+1;
        sql = "SELECT AVG("+column_name+")"+" from "+table_name;
	try:
	    cursor.execute(sql)
            results = cursor.fetchall()
            if(len(results)==0):
                print "Empty set (0.00 sec)"

            for i in results:
                for j in i:
                    print j,
                print "\n";
            db.commit();
	except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();

    elif(len(inp[1])>8 and inp[1][0]=='d' and inp[1][1]=='i' and inp[1][2]=='s' and inp[1][3]=='t' and inp[1][4]=='i' and inp[1][5]=='n' and inp[1][6]=='c' and inp[1][7]=='t'):
        table_name=inp[3];
        column_name=""
        for p in range(0,len(inp[1])):
            if(inp[1][p]=='('):
                p=p+1;
                while(p<len(inp[1])-1):
                    column_name+=inp[1][p];
                    p=p+1;
        sql = "SELECT DISTINCT("+column_name+")"+" from "+table_name;
	try:
	    cursor.execute(sql)
            results = cursor.fetchall()
            if(len(results)==0):
                print "Empty set (0.00 sec)"

            for i in results:
                for j in i:
                    print j,
                print "\n";
            db.commit();
        except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();


    elif(inp[0]=="TRUNCATE"):
        table_name = inp[2];
        sql = "TRUNCATE TABLE "+table_name;
	try:
		cursor.execute(sql);
		db.commit();
        	print "Query OK, 0 rows affected"
	except:
	        print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	        db.rollback();

    elif(inp[0]=="Insert"):
        table_name=inp[2];
        st=inp[3];
        i=0;
        em="";
        while(i<len(st)):
            if(st[i]=='('):
                while(i<len(st)-1):
                    em=em+st[i];
                    i=i+1;
            i=i+1;
        sql="INSERT INTO "+table_name+" VALUES"+em;
        try:
            cursor.execute(sql)
            db.commit();
            print "Query OK, 1 row affected (0.05 sec)"
        except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
            db.rollback();

    elif(inp[0]=="Delete"):
        table_name = inp[2];
        attribute_name = inp[4];
        value_name = inp[6];
        sql = "DELETE FROM "+table_name+ " WHERE " + attribute_name+ " = "+value_name;
        try:
            cursor.execute(sql)
            db.commit();
            print "Query OK, 1 row affected (0.05 sec)"
        except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
            db.rollback();
	
    elif(inp[0]=="DROP"):
        table_name = inp[2];
	sql = "SELECT COUNT(*) FROM "+table_name;
	cursor.execute(sql)
        results = cursor.fetchall()
	for i in results:
	    for j in i:
	        if(j==0):
			sql = "DROP TABLE "+table_name;
			try:
			    cursor.execute(sql);
			    db.commit();
			    print "Query OK, 0 rows affected (0.03 sec)"

			except:
			    db.rollback();
		else:
		    print "Table is not empty"

    elif(inp[0]=="CREATE"):
	#print inp[2]
        string = inp[2];
	i=0;
	table_name="";
	while(i<len(string) and string[i]!='('):
		table_name+=string[i];
		i=i+1;
	attributes_list=[];
	#string.split(" ")
	#print table_name,string
	dup=dup[13:]
	dup=dup.split(" ")
	for i in range(0,len(dup),2):
		if(i==0):
			s=dup[i];
			j=0;
			while(s[j]!='('):
				j=j+1;
			j=j+1;
			p = s[j:];
			attributes_list.append(p);
		else:
		        attributes_list.append(dup[i]);
	r = open("metadata.txt","a");
	stri=""
	stri+="\n<begin_table>\n"+table_name+"\n";
	for i in attributes_list:
	    stri+=i+"\n";
	stri+="<end_table>\n"
	r.write(stri);
	fi = open(table_name+".csv",'w')
    elif("AND" in inp):
	    st=dup[7:]
	    sql = "SELECT "+st;
	    try:
	        cursor.execute(sql);
		results=cursor.fetchall()
	        if(len(results)==0):
		    print "Empty set (0.00 sec)"
		for i in results:
		    for j in i:
		        print j,
		    print "\n";
		db.commit();
	    except:
	        db.rollback();
    elif("OR" in inp):
	    st=dup[7:]
	    sql = "SELECT "+st;
	    try:
	        cursor.execute(sql);
		results=cursor.fetchall()
	        if(len(results)==0):
		    print "Empty set (0.00 sec)"
		for i in results:
		    for j in i:
		        print j,
		    print "\n";
		db.commit();
	    except:
	        db.rollback();

    else:
        table_name = inp[len(inp)-1];
	column_names="";
	i = 1;
	while(i<len(inp)-2):
		column_names+=inp[i]+" ";
		i=i+1;
	#print table_name,column_names
	sql = "SELECT "+column_names+" FROM "+table_name;
	#print sql
        try:
	    cursor.execute(sql);
	    results = cursor.fetchall()
	    #print results
	    if(len(results)==0):
	        print "Empty set (0.00 sec)"
            for i in results:
	        for j in i:
		    print j,
	        print "\n";
	    db.commit();
        except:
	    print "ERROR 1146 (42S02): Table 'ENGINE."+table_name+" doesn't exist";
	    db.rollback();

    
     
