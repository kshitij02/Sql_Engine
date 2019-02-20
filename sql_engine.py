#!/usr/bin/env python
# coding: utf-8

# In[13]:


from collections import OrderedDict 
import csv
import pprint
import sqlparse
import itertools
import sys


# In[14]:


table_information={}
table_names=[]
table={}
querry_list=[]
distinct_flag=0
aggregate_flag=0
def metadata():
    metadata_file=open("files/metadata.txt","r")
    flag_new_table=0
    current_table=""
    metadata_lines=metadata_file.readlines()
#     metadata_lines.remove('/n')
    for i in metadata_lines:
#         print "1",i,"2"
        i=i.strip()
        if i=='<begin_table>':
            flag_new_table=1
        elif i=='<end_table>':
            flag_new_table=0
            current_table=""
        elif flag_new_table==1:
            table_information[i]=OrderedDict()
            table_names.append(i.lower())
            current_table=i.lower()
            flag_new_table=2
        else:
            table_information[current_table][current_table+"."+i.lower()]=[]
    content={}
    for i in table_names:
        content=csv.DictReader(open("files/"+i+".csv"),fieldnames=table_information[i].keys())
        for row in content:
            for k in row.keys():
                table_information[i][k].append(int(row[k]))
#     pprint.pprint (table_information)


# In[15]:


metadata()


# In[16]:


def parser(sql_querry,querry_list):
    parsed = sqlparse.parse(sql_querry)[0]
    for i in sqlparse.sql.IdentifierList(parsed).get_identifiers():
        querry_list.append(str(i))
    if querry_list[0].lower()!='select':
        print "Error"
        exit(1)
    else :
        querry_list=querry_list[1:]
#         print querry_list
    return querry_list


# In[17]:


def check_table_name(table_name):
    for j in range(len(table_name)):
        table_name[j]=table_name[j].strip().lower()
        if table_name[j] not in table_names:
            print "Error : No such Table Exits!"
            exit(1)


# In[18]:


def check_columns(columns,table_name):
    for col in columns:
        if col.lower() not in table_information[table_name].keys():
            print "Error! Columns not present in given table"
            exit(1)


# In[19]:


def distinct_checker(querry_list,distinct_flag):
        if querry_list[0].lower()=='distinct':
            distinct_flag=1
            querry_list=querry_list[1:]
        return distinct_flag,querry_list


# In[20]:


def get_required_table_data(querry_list):
    table_name=querry_list[2].split(',')
#     print table_name
    check_table_name(table_name)
    output_table=[]
    for i in table_name:
        output_table.append(table_information[i])
    return output_table,table_name
    


# In[21]:


def obtaining_result(output_table,tablename_currentq,querry_list):
    res =[]
    columns=querry_list[0].split(',')
    columns=map(str.strip, columns)
    if columns[0]=='*' and len(columns)==1:
        columns=output_table[0].keys()
    for i in range(len(columns)):
        if not columns[i].startswith(tablename_currentq+"."):
            columns[i]=tablename_currentq+"."+columns[i]
    
    res.append(columns)
    check_columns(columns,tablename_currentq)
    len_columms=len(output_table[0][columns[0]])
    for i in range(len_columms):
        li=[]
        for j in columns:
            li.append(output_table[0][j][i])
        res.append(li)

    return res        


# In[22]:


def obtaining_result(output_table,tablename_currentq,querry_list):
    res =[]
    columns=output_table[0].keys()
#     print columns
    res.append(columns)
#     check_columns(columns,tablename_currentq)
    len_columms=len(output_table[0][columns[0]])
    for i in range(len_columms):
        li=[]
        for j in columns:
            li.append(output_table[0][j][i])
        res.append(li)
    return res
        
        


# In[23]:


def obtaining_result_multiple(output_table,tablename_currentq,columns):
    res =[]
    if columns[0]=='*' and len(columns)==1:
        columns=table_information[tablename_currentq].keys()
    res.append(columns)
#     check_columns(columns,tablename_currentq)
    len_columms=len(table_information[tablename_currentq][columns[0]])
    for i in range(len_columms):
        li=[]
        for j in columns:
            li.append(table_information[tablename_currentq][j][i])
        res.append(li)
    return res
        


# In[24]:


def check_aggregate(querry_list,aggregate_flag):
    # print querry_list[0]
    if querry_list[0].startswith('max('):
        aggregate_flag='max'
    elif querry_list[0].startswith('sum('):
        aggregate_flag='sum'
    elif querry_list[0].startswith('min('):
        aggregate_flag='min'
    elif querry_list[0].startswith('avg('):
        aggregate_flag='avg'
    # print querry_list[0]
    if aggregate_flag!=0:
        querry_list[0]=querry_list[0][4:]
        # print querry_list[0]
        querry_list[0]=querry_list[0][:-1]
    # print querry_list
    return querry_list,aggregate_flag
        


# In[25]:


def printing_final_result(res,aggregate_flag,distinct_flag):
    if distinct_flag==1:
        temp=[]
        for i in res:
            if i not in temp:
                temp.append(i)
        res=temp
    if aggregate_flag!=0:
        if len(res)>1:
            li=[]
            for i in range(1,len(res)):
                li.append(res[i][0])
            res=res[0:1]
            if aggregate_flag=='sum':
                res.append([sum(li)])
            elif aggregate_flag=='avg':
                res.append([sum(li)/float(len(li))])
            elif aggregate_flag=='min':
                res.append([min(li)])
            elif aggregate_flag=='max':
                res.append([max(li)])
    for i in res:
        for j in range(len(i)):
            i[j]=str(i[j])
    for i in res:
        print ",".join(i)


# In[26]:


def print_helper_table(res,columns):
    index=[]
    out=[]
#     print res[0]
    for i in range(len(res[0])):
        if res[0][i] in columns:
            index.append(i)
    for row in res:
        li=[]
        for i in range(len(row)):
            if i in index:
                li.append(row[i])
        out.append(li)
    return out


# In[27]:


def print_join_table(virtual_data,columns):
    res=[]
    len_columns=len(virtual_data[columns[0]])
    res.append(columns)
    for i in range(len_columns):
        li=[]
        for col in columns:
            li.append(virtual_data[col][i])
        res.append(li)
    return res


# In[28]:


def where_parser(querry,tablename_currentq,res):
    condition_present=""
    li=[]
    if 'and' in querry:
        li=querry.split('and')
        li=map(str.strip, li)
        condition_present='and'
    elif 'or' in querry:
        li=querry.split('or')
        li=map(str.strip, li)
        condition_present='or'
    else :
        li=[querry[:].strip('')]
        li=map(str.strip, li)
#     print li,condition_present
    op_left=[]
    op_right=[]
    operator=[]
    minus=[]
    for i in li:
        l=[]
        if "<=" in i:
            operator.append("<=")
            l=i.split('<=')
            l=map(str.strip,l)
        elif ">=" in i:
            operator.append(">=")
            l=i.split('>=')
            l=map(str.strip,l)
        elif ">" in i:
            operator.append(">")
            l=i.split('>')
            l=map(str.strip,l)
        elif "<" in i:
            operator.append("<")
            l=i.split('<')
            l=map(str.strip,l)
        elif "=" in i:
            operator.append("==")
            l=i.split('=')
            l=map(str.strip,l)
        op_left.append(l[0])
        op_right.append(l[1])
    for i in range(len(op_right)):
        if '-' in op_right[i]:
            minus.append('-')
            op_right[i]=op_right[i][1:]
        else: 
            minus.append('')

    for col in range(len(op_left)):
        if not op_left[col].isdigit():
            occurance=0
            t=""
            sw=0
            for table in tablename_currentq:
                if op_left[col].startswith(table+"."):
                    occurance=1
                    sw=1
                    break

                if table+"."+op_left[col] in table_information[table].keys():
                    occurance=occurance+1
                    t=table
            if occurance>1:
                print "Error ! Ambiguous "+op_left[col]+" columns name"
                exit(1)
            elif occurance==0:
                print "Error ! Columns "+op_left[col]+" doesn't occur in given tables"
                exit(1)
            elif occurance==1 and sw!=1:
                op_left[col]=t+"."+op_left[col]
    for col in range(len(op_right)):
        if not op_right[col].isdigit():
            occurance=0
            t=""
            sw=0
            for table in tablename_currentq:
                if op_right[col].startswith(table+"."):
                    occurance=1
                    sw=1
                    break

                if table+"."+op_right[col] in table_information[table].keys():
                    occurance=occurance+1
                    t=table
            if occurance>1:
                print "Error ! Ambiguous "+op_right[col]+" columns name"
                exit(1)
            elif occurance==0:
                print "Error ! Columns "+op_right[col]+" doesn't occur in given tables"
                exit(1)
            elif occurance==1 and sw!=1:
                op_right[col]=t+"."+op_right[col]
#     print op_left
#     print op_right
#     print operator
    li_index={0:[],1:[]}
    for i in range(len(op_left)):
        if not op_right[i].isdigit() and not op_left[i].isdigit():
            left=res[0].index(op_left[i])
            right=res[0].index(op_right[i])
            z=1
            for j in res[1:]:
                if eval(str(j[left])+operator[i]+str(j[right])):
                    li_index[i].append(z)
                z=z+1
        elif not op_right[i].isdigit() and op_left[i].isdigit():
            right=res[0].index(op_right[i])
            z=1
            for j in res[1:]:
                if eval(str(op_left[i])+operator[i]+str(j[right])):
                    li_index[i].append(z)
                z=z+1
        elif op_right[i].isdigit() and not op_left[i].isdigit():
            left=res[0].index(op_left[i])
            z=1
            for j in res[1:]:
                if eval(str(j[left])+operator[i]+str(minus[i]+op_right[i])):
                    li_index[i].append(z)
                z=z+1
        elif op_left[i].isdigit() and op_right[i].isdigit():
            z=1
            for j in res[1:]:
                if eval(str(op_left[i])+operator[i]+str(op_right[i])):
                    li_index[i].append(z)
                z=z+1
#     print li_index
    final_index=[]
    if condition_present == 'and':
        final_index=list(set(li_index[0]) & set(li_index[1]))
    elif condition_present == 'or':
        final_index =list(set(li_index[0])| set(li_index[1]))
    elif condition_present=="":
        final_index=li_index[0]
#     print final_index
    out=[]
    k=1
    out.append(res[0])
    for j in res[1:]:
        if k in final_index:
            out.append(j)
        k=k+1
#     print out
    return out
        


# In[29]:


querry_list=[]
distinct_flag=0
aggregate_flag=0
sql_q=sys.argv[1].lower()
if sql_q[-1]!=';':
    # print sql_q
    print "Error ! Invaild sql querry , A querry must end with ';'"
    exit(1)

# print table_information
querry_list=parser(sql_q,querry_list)
# print querry_list
distinct_flag,querry_list=distinct_checker(querry_list,distinct_flag)
# print querry_list
output_table,tablename_currentq=get_required_table_data(querry_list)
# print querry_list


#Single Tables

if len(tablename_currentq)==1:
#     tablename_currentq=tablename_currentq[0]
    querry_list,aggregate_flag=check_aggregate(querry_list,aggregate_flag)
    # print querry_list
    columns=querry_list[0].split(',')    
    if columns[0]=="*" and len(columns)==1:
        star_flag=1
        columns=[]
        for table in tablename_currentq:
            for key in table_information[table].keys():
                columns.append(key)
    else:
        for col in range(len(columns)):
            occurance=0
            t=""
            sw=0
            for table in tablename_currentq:
                if columns[col].startswith(table+"."):
                    occurance=1
                    sw=1
                    break

                if table+"."+columns[col] in table_information[table].keys():
                    occurance=occurance+1
                    t=table
            if occurance>1:
                print "Error ! Ambiguous "+columns[col]+" columns name"
                exit(1)
            elif occurance==0:
                print "Error ! Columns "+columns[col]+" doesn't occur in given tables"
                exit(1)
            elif occurance==1 and sw!=1:
                columns[col]=t+"."+columns[col]
#     print columns
    res=obtaining_result(output_table,tablename_currentq,querry_list)
#     print res
    if 'where' in querry_list[3]:
        querry_list[3]=querry_list[3][5:]
        res=where_parser(querry_list[3][:-1],tablename_currentq,res)
    res=print_helper_table(res,columns)
    printing_final_result(res,aggregate_flag,distinct_flag)

# Mulitiple Tables    
elif len(tablename_currentq)>1:
    output_t=[]
    output_t_col_name=[]
    star_flag=0
    querry_list,aggregate_flag=check_aggregate(querry_list,aggregate_flag)  
    columns=querry_list[0].split(',')

    if columns[0]=="*" and len(columns)==1:
        star_flag=1
        columns=[]
        for table in tablename_currentq:
            for key in table_information[table].keys():
                columns.append(key)
    else:
        for col in range(len(columns)):
            occurance=0
            t=""
            sw=0
            for table in tablename_currentq:
                if columns[col].startswith(table+"."):
                    occurance=1
                    sw=1
                    break

                if table+"."+columns[col] in table_information[table].keys():
                    occurance=occurance+1
                    t=table
            if occurance>1:
                print "Error ! Ambiguous "+columns[col]+" columns name"
                exit(1)
            elif occurance==0:
                print "Error ! Columns "+columns[col]+" doesn't occur in given tables"
                exit(1)
            elif occurance==1 and sw!=1:
                columns[col]=t+"."+columns[col]
    new_dict={}
    for table in tablename_currentq:
        current_col=["*"]
        res=obtaining_result_multiple(output_table,table,current_col)
        output_t_col_name=output_t_col_name+res[0:1]
        output_t.append(res[1:])
        for col in table_information[table].keys():
            new_dict[col]=table_information[table][col]
    virtual_data={}
    for table in tablename_currentq:
        for key in table_information[table].keys():
            virtual_data[key] = []
    for i in itertools.product(*output_t):
        for j,z in zip(i,tablename_currentq):
            for k,q in zip(j,table_information[z].keys()):
                key = q
                virtual_data[key].append(k)
    temp_col=[]
    for table in tablename_currentq:
        for t in table_information[table].keys():
            temp_col.append(t)
            
    res=print_join_table(virtual_data,temp_col)
#     print virtual_data
#     print res
    if 'where' in querry_list[3]:
        querry_list[3]=querry_list[3][5:]
        # print res
        res=where_parser(querry_list[3][:-1],tablename_currentq,res)
#         print res
#     res=print_join_table(virtual_data,columns)
    res=print_helper_table(res,columns)
    printing_final_result(res,aggregate_flag,distinct_flag)        
        


# In[ ]:





# In[ ]:





# In[ ]:




