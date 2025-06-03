import pandas as pd
import time
from datetime import datetime

invoices = pd.read_csv('invoicedump Fur 27052025.csv')
balance = pd.read_csv('Closingbalance Fur 27052025.csv')


current_date = datetime(2025, 3, 31).date() #update
#current_date = datetime.now().date()





def mapint(x):
    try:
        return int(x)
    except:
        return 0

def map_inv_rank_to_age (rank):
    agemap = {
            1: 'ZeroTo30',
            2: 'ThirtyOneToSixty',
            3: 'SixtyOneToNinety',
            4: 'NinetyOneToOneTwenty',
            5: 'OneTwentyOneToOneEighty',
            6: 'MoreThanOneEighty'
                }
    if rank <=6:
        return agemap[rank]

    else:
        return 'MoreThanOneEighty'

balance.columns = ['furid','balance']
balance = balance[~balance.furid.isnull()]
#balance.balance = balance.balance.map(lambda x : float(x.replace(',',''))) mukul
balance.balance = balance.balance.map(lambda x: float(str(x).replace(',', ''))) #joseph update

invoices.columns = ['furid','invoice_id','invoice_date','invoice_value']
invoices = invoices[~invoices.furid.isnull()]
#invoices.invoice_value = invoices.invoice_value.map(lambda x : mapint(x.replace(',','')))
invoices.invoice_value = invoices.invoice_value.map(lambda x: float(str(x).replace(',', ''))) #joseph update

invoices['invoice_date'] = pd.to_datetime(invoices['invoice_date'], format='%d-%b-%y')
balance = balance[balance.furid != 'Total']

users_with_outstanding = balance[balance.balance>0]
invoices = invoices[invoices['invoice_value']>=0]
def map_age_to_bucket(age):

    if age > 180:
        return 'MoreThanOneEighty'
    elif age >= 121:
        return 'OneTwentyOneToOneEighty'
    elif age >= 91:
        return 'NinetyOneToOneTwenty'
    elif age >= 61:
        return 'SixtyOneToNinety'
    elif age >= 31:
        return 'ThirtyOneToSixty'
    elif age >= 0:
        return 'ZeroTo30'
    else:
        return 'Invalid Age'

#invoices['age'] = (datetime.now().date() - invoices['invoice_date'].dt.date)
invoices['age'] = (current_date - invoices['invoice_date'].dt.date)
invoices['age'] = invoices['age'].map(lambda x:x.days)
invoices['age_bucket'] = invoices['age'].map(lambda x:map_age_to_bucket(x))
df_ageing_output = pd.DataFrame(columns=['furid','age_bucket','amount'])




s = time.time()
invoice_missing_user_list = []
invoices_user_list = list(invoices.furid)
df_output_list = []

users = users_with_outstanding

for user in list(users.furid):

    if user not in invoices_user_list:
        invoice_missing_user_list.append(user)
        os_amount = users_with_outstanding[users_with_outstanding.furid==user]['balance'].values[0]
        allocation_row = [user,os_amount,'MoreThanOneEighty']
        df_output_list.append(allocation_row)

    else:

        os_amount = users_with_outstanding[users_with_outstanding.furid==user]['balance'].values[0]
        df_invoices = invoices[invoices.furid==user].sort_values(by=['age'])

        invoice_values = list(df_invoices['invoice_value'])
        invoice_values.reverse()

        invoice_age_bucket = list(df_invoices['age_bucket'])
        invoice_age_bucket.reverse()

        while len(invoice_values)!=0:
            invoice_value  = invoice_values.pop()
            age_bucket = invoice_age_bucket.pop()

            if os_amount < invoice_value:
                allocation_row = [user,os_amount,age_bucket]
                df_output_list.append(allocation_row)
                os_amount = 0
                break

            if invoice_value <= os_amount:
                allocation_row = [user,invoice_value,age_bucket]
                df_output_list.append(allocation_row)
                os_amount = os_amount - invoice_value

            elif invoice_value > os_amount:
                allocation_row = [user,os_amount,age_bucket]
                df_output_list.append(allocation_row)

            if (os_amount > 0) and (len(invoice_values)==0):
                allocation_row = [user,os_amount,'MoreThanOneEighty']
                df_output_list.append(allocation_row)
                os_amount = 0

df_ageing_output = pd.DataFrame(data =df_output_list, columns=['furid','amount','age_bucket'])
df_missing_invoices_users = pd.DataFrame(data =invoice_missing_user_list, columns=['furid'])

e = time.time()
print(e-s)



df_ageing_output = df_ageing_output.groupby(['furid','age_bucket']).agg({'amount':'sum'}).reset_index()
df_output = df_ageing_output.pivot(index='furid', columns='age_bucket', values=['amount']).reset_index()
df_output = df_output.reset_index(drop=True)
df_output.columns = list(df_output.columns.droplevel())
df_output.rename(columns={'':'FurID'}, inplace=True)
df_output = df_output[['FurID','ZeroTo30','ThirtyOneToSixty','SixtyOneToNinety','NinetyOneToOneTwenty','OneTwentyOneToOneEighty','MoreThanOneEighty']]
df_output.fillna(0, inplace=True)

df_output.to_csv('invoicing_ageing_report_'+str(datetime.now().date())+'_by_invoice_age.csv', index=False)
