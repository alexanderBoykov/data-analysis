import pandas as pd
import os
import psycopg2

con = psycopg2.connect(
    database="postgres",
    user="postgres",
    password="zachem",
    host="127.0.0.1",
    port="5432"
)

# connection to own base
cur = con.cursor()
cur.execute('''
    CREATE TABLE CONTACTS(
    CLIENTID SERIAL PRIMARY KEY ,
    CLIENTNAME TEXT NOT NULL ) ;


    CREATE TABLE ADRESSES(
    ADRESS TEXT NOT NULL,
    CustomerID SERIAL  PRIMARY KEY,
    FOREIGN KEY (CustomerID) REFERENCES CONTACTS (CLIENTID)); 

''')
#PARSING FILES
files=os.listdir('./@OwnerData')

lst=[]
for k in files:
    try:

        direct = './@OwnerData/' + k
        if k.split('.')[-1]=='xlsx':
            testData=pd.read_excel(direct)
        elif k.split('.')[-1]=='xls':
            testData=pd.read_excel(direct)
        elif k.split('.')[-1]=='csv':
            testData = pd.read_csv(direct)

        columsName = testData.columns.values.tolist()
        indexes = []
        clientinfo = []
        for i in columsName:

            if ('number' and 'street') in str(i).lower():
                indexes.append(i)
            elif ('name' and 'street') in str(i).lower() :
                indexes.append(i)

            elif ('locality' in i and 'name' in i) or 'suburb' in str(i).lower():
                indexes.append(i)

            elif 'state' in str(i).lower():
                indexes.append(i)

            elif ('postcode' in str(i).lower()) :

                indexes.append(str(i))
                break
        for i in columsName:
            if ('name'in str(i).lower() and 'given'in str(i).lower()) or ('first' in str(i).lower() and 'name' in str(i).lower()):
                clientinfo.append(i)
            elif 'surname' in str(i).lower():
                clientinfo.append(i)


#ADDING INFO TO DATABASE
        adresTable = testData[indexes].values.tolist()
        clienTable= testData[clientinfo].values.tolist()
        for j in clienTable:
            inf=[]
            for i in j:
                if str(i)!= 'nan':
                    inf.append(str(i))
            owners = ' '.join(inf)
            if len (inf)==0:
                owners='No info'
            sqlst="INSERT INTO CONTACTS (CLIENTNAME) VALUES (%s);"
            owners=(owners,)
            cur.execute(sqlst,owners)

        for j in adresTable:
            j = [str(i) for i in j]
            STadress = ', '.join(j)
            sqlst = "INSERT INTO ADRESSES (ADRESS) VALUES (%s);"
            STadress = (STadress,)
            cur.execute(sqlst,STadress)

    except:
        continue



print()