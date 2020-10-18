# Azure SQL Database
It is a cloud-based SQL Database that stores structured data.
<br/>For more information visit:
<br/>https://docs.microsoft.com/en-us/azure/azure-sql/database/sql-database-paas-overview
<br/>In this example we will be commiting structed sensor data in a CSV to a SQL database

### Installing packages


```python
import pyodbc
import pandas as pd
```

### Create dataframe object
The dataframe will contaoin the data from the CSV file that is to be commited to the sql database


```python
#Read CSV file to pandas dataframe
df = pd.read_csv("sittingone_MetaOn_Gyr.csv") 

#Display the top five rows
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>epoch (ms)</th>
      <th>time (06:00)</th>
      <th>elapsed (s)</th>
      <th>x-axis (deg/s)</th>
      <th>y-axis (deg/s)</th>
      <th>z-axis (deg/s)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1600875978480</td>
      <td>2020-09-23T18:46:18.480</td>
      <td>0.00</td>
      <td>-13.476</td>
      <td>0.488</td>
      <td>0.915</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1600875978500</td>
      <td>2020-09-23T18:46:18.500</td>
      <td>0.02</td>
      <td>-14.756</td>
      <td>0.427</td>
      <td>0.427</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1600875978520</td>
      <td>2020-09-23T18:46:18.520</td>
      <td>0.04</td>
      <td>-18.171</td>
      <td>2.012</td>
      <td>-0.122</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1600875978540</td>
      <td>2020-09-23T18:46:18.540</td>
      <td>0.06</td>
      <td>-12.195</td>
      <td>1.524</td>
      <td>-0.183</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1600875978560</td>
      <td>2020-09-23T18:46:18.560</td>
      <td>0.08</td>
      <td>-2.683</td>
      <td>-0.610</td>
      <td>0.122</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Renaming the columns for easier reference(optional)
df.rename(columns = {'x-axis (deg/s)': 'x', 'y-axis (deg/s)': 'y', 'z-axis (deg/s)': 'z'}, inplace = True) 

df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>epoch (ms)</th>
      <th>time (06:00)</th>
      <th>elapsed (s)</th>
      <th>x</th>
      <th>y</th>
      <th>z</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1600875978480</td>
      <td>2020-09-23T18:46:18.480</td>
      <td>0.00</td>
      <td>-13.476</td>
      <td>0.488</td>
      <td>0.915</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1600875978500</td>
      <td>2020-09-23T18:46:18.500</td>
      <td>0.02</td>
      <td>-14.756</td>
      <td>0.427</td>
      <td>0.427</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1600875978520</td>
      <td>2020-09-23T18:46:18.520</td>
      <td>0.04</td>
      <td>-18.171</td>
      <td>2.012</td>
      <td>-0.122</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1600875978540</td>
      <td>2020-09-23T18:46:18.540</td>
      <td>0.06</td>
      <td>-12.195</td>
      <td>1.524</td>
      <td>-0.183</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1600875978560</td>
      <td>2020-09-23T18:46:18.560</td>
      <td>0.08</td>
      <td>-2.683</td>
      <td>-0.610</td>
      <td>0.122</td>
    </tr>
  </tbody>
</table>
</div>



### Connect to Database
You can obtain the connection details from the 


```python
#Create connection string

server = 'server-name.database.windows.net'
database = 'DBName'
username = 'user'
password = 'yourpassword'
DATABASE_URI = 'DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
```


```python
#Connect to SQL Server
conn = pyodbc.connect(DATABASE_URI)
cursor = conn.cursor()
```

### Create a table 
Create a table that will contain the structured data 


```python
# Create a Table if one with same name doesn't already exist
cursor.execute('''
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_NAME = N'your_table_name')
BEGIN
     CREATE TABLE your_table_name (Epoch bigint, X_axis float, Y_axis float, Z_axis float)
END '''
)
print('Table successfully created!')
```

###  Commit database
The data will be uoloaded to Azure SQL database row by row. This may take a considerable amount of time depending on the number of rows 


```python
# Insert DataFrame to Table
for row in acc_df.itertuples():
    cursor.execute('''
                    INSERT INTO DBName.dbo.body_acc_all (Epoch, X_axis, Y_axis, Z_axis)
                    VALUES (?,?,?,?,?)
                    ''',
                    row.Epoch, 
                    row.X_axis,
                    row.Y_axis,
                    row.Z_axis,
                    )


#Commit Database
conn.commit()
print("Finished!")

#Close connection
cursor.close()
```
