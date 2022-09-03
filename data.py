import pandas as pd
import seaborn as sns
import numpy as numpy
import glob,os,sys

try:
    customers = pd.read_csv("data/customers.csv")
except OSError:
    print("Could not open/read customers file:")
    sys.exit()

try:
    products = pd.read_csv("data/products.csv")
except OSError:
    print("Could not open/read products file:")
    sys.exit()

files_list=glob.glob('data/transactions/*')
#print(files_list)

def data_extract(transactions_1):
    df_final=pd.DataFrame()
    customer_id_l=[]
    product_id_l=[]
    price_l=[]
    date_of_purchase_l=[]
    for i,j,k in zip(transactions_1['customer_id'],transactions_1['basket'],transactions_1['date_of_purchase']):
#         print(i,j,k)
        for product in j:
#             print(product)
            product_id=product['product_id']
            price=product['price']
#             print(product_id)
#             print(price)
            customer_id_l.append(i)
            product_id_l.append(product_id)
            price_l.append(price)
            date_of_purchase_l.append(k)
    df_final['customer_id']=customer_id_l
    df_final['product_id']=product_id_l
    df_final['price']=price_l
    df_final['date_of_purchase']=date_of_purchase_l
    return df_final

def transactions():    
    final=pd.DataFrame()
    final['customer_id']=[]
    final['product_id']=[]
    final['price']=[]
    final['date_of_purchase']=[]

    for file in files_list:
        transactions_1 = pd.read_json(f"{file}/transactions.json", lines=True, orient='records')
    #     print(transactions_1)
        temp=data_extract(transactions_1)
    #     print(temp)
    #     print(final)
        final=final.append(temp)
#         print(final)
    return final

try: 
    final = transactions()
except OSError:
    print("Could not open/read transactions file:")
    sys.exit()


txns = final
df_final = pd.merge(customers, txns, how = 'left' , on='customer_id' )
master_table = pd.merge(df_final, products, how = 'left', on='product_id')
purchase_count_table = pd.DataFrame(master_table.groupby(['customer_id']).agg({'product_id':'count'})).reset_index()

purchase_count_table = purchase_count_table.rename(columns={'product_id': 'purchase_count'})

master_table = pd.merge(master_table, purchase_count_table, how = 'left', on='customer_id')
#print(master_table)

try:
    master_table.to_csv("output/output.csv",index=False)
    master_table.to_json("output/output.json",orient='records')
except OSError:
    print("Could not write output file:")
    sys.exit()



    
