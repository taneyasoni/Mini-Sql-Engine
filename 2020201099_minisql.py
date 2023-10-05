import sys
import csv
import re
import itertools
import operator 
import copy
import collections
#====================================flag variables===============================
and_flag = or_flag = distinct_flag = group_by_flag = order_by_flag = False
agg_fn_list = ['max','min','sum','count', 'average']
#========================utility functions===================================
def read_one_column_from_final_table(Final_table, index):
    temp_list = []
    for row in Final_table:
        temp_list.append(row[index])
    return temp_list

def read_csv_table(t):
    whole_table = []
    path = 'files/'+ str(t) + '.csv'
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            temp_list = []
            for ele in row:
                ele = int(ele.replace("'","").replace('"',''))
                temp_list.append(ele)
            whole_table.append(temp_list)
    return whole_table

def get_tname_from_col(cname):
    for tname in metadata:
        if cname in metadata[tname]["column_name"]:
            return tname

def print_heading(columns_asked, Final_cols):
    heading = []
    for ca in columns_asked:
        if ca in Final_cols:
            heading.append(get_tname_from_col(ca) + '.' + ca)
        else:
            heading.append(ca)
    print(' '.join(heading))

def print_Final_Table(Final_table):
    j = 0
    i = 0
    for i in range(len(Final_table)):
        for j in range(len(Final_table[i])-1):
            print(Final_table[i][j], end=', ')
        print(Final_table[i][j+1], end='')
        print()
        
def get_index(Final_cols, single_column):
    if single_column not in Final_cols:
        error_exit('Column doesnt exist!')        
    return Final_cols.index(single_column)

def get_Cross_Product(table1,table2):
    Final_table=[]
    for element in itertools.product(table1,table2):
        Final_table.append(element[0]+element[1])
    return Final_table

def get_Table_Data(table):
    table = metadata[table]["table_data"]
    return table

def get_distinct(t):
    return list(t for t,_ in itertools.groupby(t))

def get_Max(Final_table, index):
    list1 = read_one_column_from_final_table(Final_table, index)
    return max(list1)
def get_Min(Final_table, index):
    list1 = read_one_column_from_final_table(Final_table, index)
    return min(list1)
def get_Sum(Final_table, index):
    list1 = read_one_column_from_final_table(Final_table, index)
    return sum(list1)
def get_Count(Final_table, index):
    list1 = read_one_column_from_final_table(Final_table, index)
    return len(list1)
def get_Avg(Final_table, index):
    list1 = read_one_column_from_final_table(Final_table, index)
    return sum(list1)/len(list1)
def error_exit(msg):
    print('Error: ' + msg)
    sys.exit()
#===============================from function=============================
def execute_From(table_names):
    
    heading=[]
    for t in table_names:
        heading = heading+metadata[t]["column_name"]
    if(len(table_names) == 1):
        Final_table = get_Table_Data(table_names[0])
        return Final_table,heading
    else:
        Final_table = []
        Final_table = get_Cross_Product(get_Table_Data(table_names[0]), get_Table_Data(table_names[1]))
        for i in range(2, len(table_names)):
            temp_table = get_Table_Data(table_names[i])
            Final_table = get_Cross_Product(Final_table, temp_table)
        return Final_table,heading
#===============================where function================================
def execute_Where(Final_table, Final_cols, query):

    ops = {"=": operator.eq, ">=": operator.ge,">": operator.gt, "<": operator.lt, "<=": operator.le}
    con = query.split("where")[1].strip().split()
    operators =[]  
    operands = [] 

    if 'and' in con:
        and_flag = True
        or_flag = False
    elif 'or' in con:
        and_flag = False
        or_flag = True
    else:
        and_flag = False
        or_flag = False
    #get operator and operand index
    if 'and' in con or 'or' in con:
        temp = [con[0], con[2], con[4], con[6]]
        for i in temp:
            if i in Final_cols:
                operands.append(i)
            else:
                operands.append(int(i))
        
        operators = [con[1], con[5]]

        operand_index=[] # [index of E , -1 , index of A, -1]
        for op in operands:
            if(type(op)==str):
                for i in range(0,len(Final_cols)):
                    if op==Final_cols[i]:
                        operand_index.append(i)
            else:
                operand_index.append(-1)

    else:
        temp = [con[0], con[2]]
        for i in temp:
            if i in Final_cols:
                operands.append(i)
            else:
                operands.append(int(i))
        operators = [con[1]]
        
        operand_index=[] # [index of E , -1 ]
        for op in operands:
            if(type(op)==str):
                for i in range(0,len(Final_cols)):
                    if op==Final_cols[i]:
                        operand_index.append(i)
            else:
                operand_index.append(-1)

    if and_flag == True:
        Temp_table=Final_table
        Final_table=[]
        for i in range(0,len(Temp_table)):    
                if operand_index[0] != -1 and operand_index[1] == -1 and operand_index[2] != -1 and operand_index[3] == -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) and ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] == -1 and operand_index[1] != -1 and operand_index[2] == -1 and operand_index[3]!=-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) and ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] !=-1 and operand_index[1] == -1 and operand_index[2] == -1 and operand_index[3] != -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) and ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])  
                elif operand_index[0] == -1 and operand_index[1]!=-1 and operand_index[2]!=-1 and operand_index[3]==-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) and ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                #-------------------------------------------------------------------
                elif operand_index[0] != -1 and operand_index[1] == -1 and operand_index[2] != -1 and operand_index[3] != -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) and ops[operators[1]](Temp_table[i][operand_index[2]],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] == -1 and operand_index[1] != -1 and operand_index[2]!=-1 and operand_index[3]!=-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) and ops[operators[1]](Temp_table[i][operand_index[2]],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] !=-1 and operand_index[1] !=-1 and operand_index[2]==-1 and operand_index[3]!=-1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],Temp_table[i][operand_index[1]]) and ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])  
                elif operand_index[0]!=-1 and operand_index[1]!=-1 and operand_index[2]!=-1 and operand_index[3]==-1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],Temp_table[i][operand_index[1]]) and ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                else:
                    error_exit('Invalid expression in where clause')
                    
    elif or_flag == True:
        Temp_table=Final_table
        Final_table=[]
        for i in range(0,len(Temp_table)):    
                if operand_index[0] != -1 and operand_index[1] == -1 and operand_index[2] != -1 and operand_index[3] == -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) or ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] == -1 and operand_index[1] != -1 and operand_index[2] == -1 and operand_index[3]!=-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) or ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] !=-1 and operand_index[1] == -1 and operand_index[2] == -1 and operand_index[3] != -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) or ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])  
                elif operand_index[0] == -1 and operand_index[1]!=-1 and operand_index[2]!=-1 and operand_index[3]==-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) or ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                #-------------------------------------------------------------------
                elif operand_index[0] != -1 and operand_index[1] == -1 and operand_index[2] != -1 and operand_index[3] != -1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]) or ops[operators[1]](Temp_table[i][operand_index[2]],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] == -1 and operand_index[1] != -1 and operand_index[2]!=-1 and operand_index[3]!=-1:
                    if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]) or ops[operators[1]](Temp_table[i][operand_index[2]],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])
                elif operand_index[0] !=-1 and operand_index[1] !=-1 and operand_index[2]==-1 and operand_index[3]!=-1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],Temp_table[i][operand_index[1]]) or ops[operators[1]](operands[2],Temp_table[i][operand_index[3]]):
                        Final_table.append(Temp_table[i])  
                elif operand_index[0]!=-1 and operand_index[1]!=-1 and operand_index[2]!=-1 and operand_index[3]==-1:
                    if ops[operators[0]](Temp_table[i][operand_index[0]],Temp_table[i][operand_index[1]]) or ops[operators[1]](Temp_table[i][operand_index[2]],operands[3]):
                        Final_table.append(Temp_table[i])
                else:
                    error_exit('Invalid expression in where clause')
    else:
        Temp_table=Final_table
        Final_table=[]
        for i in range(0,len(Temp_table)):
            if operand_index[0] == -1 and operand_index[1] != -1:
                if ops[operators[0]](operands[0],Temp_table[i][operand_index[1]]):
                    Final_table.append(Temp_table[i])
            elif operand_index[0]!=-1 and operand_index[1]==-1:
                if ops[operators[0]](Temp_table[i][operand_index[0]],operands[1]):
                    Final_table.append(Temp_table[i])
            elif operand_index[1] != -1 and operand_index[0] != -1:
                if ops[operators[0]](Temp_table[i][operand_index[0]],Temp_table[i][operand_index[1]]):
                    Final_table.append(Temp_table[i])
            else:
                error_exit('Invalid expression in where clause')

    return Final_table, Final_cols
#==================================group by ===================================================
def execute_Group(Final_table, Final_cols,columns_asked, query):
    
    group_by_col = query.split("group")[1].strip(";").split()[1]
        
    index = get_index(Final_cols,group_by_col)
    temp_dict = {}
    for row in Final_table:
        temp_dict[row[index]]=[]
        
    for row in Final_table:
        temp_dict[row[index]].append(row) 
    Final_table=[]    
    for key in temp_dict:
        Final_table=Final_table+temp_dict[key]
        
    return Final_table, Final_cols, temp_dict, group_by_col
#================================order by======================================
def execute_Order(Final_table, Final_cols,query):
    
    order_by_col = query.split("order")[1].strip(";").split()[1]
        
    index = get_index(Final_cols, order_by_col)       
    Final_table.sort(key = lambda x: x[index])
    if 'desc' in query:
        Final_table.reverse()
        
    return Final_table, Final_cols, order_by_col

#===========================read metadata=========================================
metadata = {}
try:
    # metadata = {}
    file = open("files/metadata.txt",'r')
    data = file.read()
    data = data.split("\n")
    for i in range(0, len(data)):
        if data[i] == "<begin_table>":
            i=i+1
            tname = data[i].lower()
            i=i+1
            metadata[tname]={}
            list1=[]
            while(data[i] != "<end_table>"):
                list1.append(data[i].lower())
                i=i+1
            metadata[tname]["column_name"] = list1
            metadata[tname]["table_data"]  = read_csv_table(tname)
except:
    error_exit('cant excess metadata!')
#===========================start======================================

if len(sys.argv) == 1:
    error_exit('correct way to run: python3 2020201099_minisql.py "query_name"')

elif len(sys.argv) == 2:
    actual_query = sys.argv[1]
    
    if  actual_query[-1] != ';':
        error_exit('End your query with semicolon')
        
    query = actual_query.lower()[:-1]
    split_query = query[:-1].split()

    if 'select' not in split_query and 'from' not in split_query:
        print("Format: Select <columns> from <table_names> [(optional)where <conditions(s)> group by <column> order by <column> <aesc|desc>]")
        error_exit('Invalid Query')

# actual_query = "select count(*) from table1,table2,table3,table3;"
# actual_query = "select min(A),E from table1,table2 where E < 6000 and A > 500 group by E order by E desc;"
# actual_query = "select max(A) from table1 group by A;"
# actual_query = "select A, max(b) from table1 group by A order by A desc;"
# actual_query = "select distinct A,B,X from table1,table2;"
# actual_query = "Select distinct A,B from table1,table2 where A = 922 AND B = 158;"
# actual_query = "Select distinct A,B from table1,table2 where A > 0 AND B < 6000;"
# actual_query = "Select distinct A,B from table1 where b = a and b > 700;"
# actual_query = "select count(*) from table1,table2,table3;"
# actual_query = "select A,max(B),min(C) from table1,table2 group by A;"
# actual_query = "select distinct A,sum(B),sum(C) from table1,table2 group by A;"
# actual_query = "select distinct sum(C),sum(B) from table1,table2 group by A;"
# actual_query = "select max(A),min(B),sum(C) from table1,table2;"
# actual_query = "select count(A) from table1;"
# actual_query = "select * from table1,table2,table3,table3;"
# actual_query = "select * from table1;"
# actual_query = "select count(*) from table1;"
# actual_query = "Select distinct A,B from table1,table2 where 500 > a order by a desc;"
# actual_query = "Select distinct A,B from table1 where 67 = 78;" # a > 500, a < 500, a = b
# actual_query = "select * from table1,table2 where b = a and d > 800;"
# actual_query = "select max(B),a from table1 group by A order by A;"
# actual_query = "select A,E from table1,table2 where E < 6000 and A > 500 group by E order by E;"
# actual_query = "select E,min(A) from table1,table2 where E < 6000 and A > 500 group by E order by E;"
# actual_query = 'select count(c),max(*),min(B) from table1;'
# actual_query = "select a,b,c from table1;"
# actual_query = 'select a,max(b) from table1;'
query = actual_query.lower()[:-1]
split_query = query[:-1].split()
# print(split_query)
table_names = []
columns = []

new_query = query.split("select")[1].strip()
columns = new_query.split("from")[0].split(',')

new_query = query.split("from")[1].strip()
# print(new_query)
table_names = new_query.split()[0].split(',')
# print(table_names)
for i in range(len(columns)):
    if 'distinct' in columns[i]:
        columns[i] = columns[i].replace('distinct','')
        distinct_flag = True
    columns[i] = columns[i].strip()
        
for i in range(len(table_names)):
    # print(table_names[i])
    table_names[i] = table_names[i].strip()
    if table_names[i] not in metadata:
        error_exit("Table does not exist")
#=====================================from================================================
Final_table, Final_cols = execute_From(table_names)
#=====================================where==========================================================
if 'where' in query:
    Final_table, Final_cols = execute_Where(Final_table, Final_cols, query)
#===================================group by================================================
if 'group' in query:
    group_by_flag = True
    Final_table, Final_cols, group_by_dict, group_by_col = execute_Group(Final_table, Final_cols, columns, query)  
#==================================order by ===============================================
if 'order' in query:
    order_by_flag = True
    Final_table, Final_cols, order_by_col = execute_Order(Final_table, Final_cols, query)

    if order_by_flag == True and group_by_flag == True:
        if order_by_col != group_by_col:
            error_exit('group by and order by should be applied on the same column')
        if order_by_col == group_by_col: #sort dictionary on the basis of keys in ascending order
            group_by_dict = dict(sorted(group_by_dict.items()))
            if 'desc' in query:
                group_by_dict = dict(sorted(group_by_dict.items(), reverse = True))
        else:
            error_exit('group by column and order by column should  be same')
#===========select without group by with all columns as aggregate function================
def multiple_agg_fn_without_grpby(Final_table, Final_cols, columns):
    columns_asked = []
    agg_list = []
    
    for c in columns:
        a = c.split( "(" )[0]
        b = c.split('(')[1].replace(')','')
        if b == '*':
            if a == 'count':
                b = Final_cols[0]
            else:
                error_exit(' "*" can only come with count ')
        agg_list.append([a,b])
        columns_asked.append(b)
    
    if(not all(x in Final_cols for x in columns_asked)):
        error_exit('column(s) not present in table')
        
    Final_list =[]
    for j in range(0,len(columns)):
        agg_f = agg_list[j][0]
        agg_c = agg_list[j][1]
        agg_c_index = Final_cols.index(agg_c)
        
        if agg_f == 'max':
            Final_list.append(get_Max(Final_table, agg_c_index))
        elif agg_f == 'min':
            Final_list.append(get_Min(Final_table, agg_c_index))
        elif agg_f == 'sum':
            Final_list.append(get_Sum(Final_table, agg_c_index))
        elif agg_f == 'count':
            Final_list.append(get_Count(Final_table, agg_c_index))
        elif agg_f == 'average':
            Final_list.append(get_Avg(Final_table, agg_c_index))
    
    return Final_list, columns_asked
#=================================normal select without group by========================================
if len(columns) == 1 and group_by_flag == False:
    
    if any(word in columns[0] for word in agg_fn_list): #aggregate function case
        agg_f = columns[0].split( "(" )[0]
        agg_c = columns[0].split('(')[1].replace(')','')
        if agg_c == '*':
            if agg_f == 'count':
                agg_c = Final_cols[0]
            else:
                error_exit(' "*" can only come with count ')
        i = get_index(Final_cols,agg_c)
        if agg_f == 'max':
            m = get_Max(Final_table, i)
        elif agg_f == 'min':
            m = get_Min(Final_table, i)
        elif agg_f == 'sum':
            m = get_Sum(Final_table, i)
        elif agg_f == 'count':
            m = len(Final_table)
        elif agg_f == 'average':
            m = get_Count(Final_table, i)

        print_heading(columns,Final_cols)
        print(m)
        sys.exit()

    if columns[0] == "*":
        columns = Final_cols
        print_heading(columns,Final_cols)
        if distinct_flag == True:
            Final_table = get_distinct(Final_table)
        print_Final_Table(Final_table)
        
    else:
        single_column = columns[0]
        if single_column not in Final_cols:
            error_exit('columns doesnt exixt in table')
        else:
            print_heading(columns,Final_cols)
            i = get_index(Final_cols,single_column)
            if distinct_flag == True:
                Final_table = get_distinct(Final_table)
            for row in Final_table:
                print(row[i])
    
elif len(columns) > 1 and group_by_flag == False:

    count = 0
    for c in columns:
        if c not in Final_cols:
            count+=1
    
    if count != 0 and count < len(columns):
        error_exit('Invalid column name(s) or wrong format')
        
    if count == len(columns):
        Final_list, columns_asked = multiple_agg_fn_without_grpby(Final_table, Final_cols, columns)
        print_heading(columns_asked,Final_cols)
        for i in range(len(Final_list)-1):
            print(Final_list[i], end = ", ")
        print(Final_list[-1])
        
    else:
        index_list=[]
        for c in columns:
            for fc in Final_cols:
                if c == fc:
                    index_list.append(Final_cols.index(fc))
     
        print_heading(columns,Final_cols)
        Temp_table = []
        for row in Final_table:
            temp_list = []
            for ind in index_list:
                temp_list.append(row[ind])
            Temp_table.append(temp_list)
        
        Final_table = copy.deepcopy(Temp_table)
        
        if distinct_flag == True:
            Final_table = get_distinct(Final_table)
        print_Final_Table(Final_table)
#===========================select with group by============================
elif len(columns) == 1 and group_by_flag == True:
    
    Temp_list = []
    for key in group_by_dict:
        Temp_list.append(key)
    
    if any(word in columns[0] for word in agg_fn_list): #aggregate function case
        agg_f = columns[0].split( "(" )[0]
        agg_c = columns[0].split('(')[1].replace(')','')
        if agg_c not in Final_cols or agg_c != group_by_col:
            error_exit('either column not present or not same as in group by')
        list1=[]
        if agg_f == 'max':
            list1.append(max(Temp_list))
        elif agg_f == 'min':
            list1.append(min(Temp_list))
        elif agg_f == 'sum':
            list1.append(sum(Temp_list))
        elif agg_f == 'count':
            list1.append(len(Temp_list))
        elif agg_f == 'average':
            list1.append(sum(Temp_list)/len(Temp_list))
        Temp_list = list1
    elif columns[0] not in Final_cols:
        error_exit('Invalid Column')

    if distinct_flag == True:
        Temp_list = list(dict.fromkeys(Temp_list))
        
    print_heading(columns,Final_cols)
    for item in Temp_list:
        print(item)
        
elif len(columns) > 1 and group_by_flag == True:
    
    columns_asked = []
    agg_list = []
    
    chk = 0
    for c in columns:
        if c in Final_cols:
            chk += 1
    if chk > 1:
        error_exit('More than one column with group by')
        
    for c in columns:
        if c in Final_cols:
            agg_list.append(['plain', c])
            columns_asked.append(c)
        else:
            a = c.split( "(" )[0]
            b = c.split('(')[1].replace(')','')
            agg_list.append([a,b])
            columns_asked.append(b)
    
    new_table=[]
    new_table_row=[]
    dict_keys = list(group_by_dict.keys())
    for i in range(0,len(dict_keys)):
        new_table_row=[]
        mini_table=[]
        # new_table_row.append(dict_keys[i])
        for j in range(0,len(columns)):
            agg_f = agg_list[j][0]
            agg_c = agg_list[j][1]
            if agg_c == '*':
                if agg_f == 'count':
                    agg_c = Final_cols[0]
                else:
                    error_exit(' "*" can only come with count ')
            agg_c_index = Final_cols.index(agg_c)
            mini_table = group_by_dict[dict_keys[i]]
            sub_list = read_one_column_from_final_table(mini_table, agg_c_index)
            if agg_f == 'plain':
                new_table_row.append(dict_keys[i])
            elif agg_f == 'max':
                new_table_row.append(get_Max(mini_table, agg_c_index))
            elif agg_f == 'min':
                new_table_row.append(get_Min(mini_table, agg_c_index))
            elif agg_f == 'sum':
                new_table_row.append(get_Sum(mini_table, agg_c_index))
            elif agg_f == 'count':
                new_table_row.append(get_Count(mini_table, agg_c_index))
            elif agg_f == 'average':
                new_table_row.append(get_Avg(mini_table, agg_c_index))
        new_table.append(new_table_row)

    Final_table = copy.deepcopy(new_table) 
   
    if distinct_flag == True:
        Final_table = get_distinct(Final_table)
        
    print_heading(columns,Final_cols)
    print_Final_Table(Final_table)

else:
    error_exit("can't have zero column names")  

#------------for intermediate print---------------
# import pprint
# d = pprint.PrettyPrinter(indent=2)
# d.pprint(group_by_dict)
# t = pprint.PrettyPrinter(indent=2)
# t.pprint(Final_table)
