
import pandas as pd
import numpy as np
import datetime as dt


def get_growth_budget(df_transactions):
    '''
    runs a customer growth budget on the transactions data to account for new customers, churned and regained customers

    Parameters
    -----------
    df_transations: pandas dataframe of transactions

    Returns
    -------
    churn_dataframe: pandas dataframe with columns new, churned and regained for each month
    '''


    df_transactions = df_transactions.copy()
    df_transactions["Date"] =  df_transactions["Date"].apply(lambda x: x.strftime('%Y-%m')) 
    df_transactions = pd.pivot_table(df_transactions, index = 'Customer', columns = "Date", values = 'Total', aggfunc='sum')

    #replace the empty ones with Zero => means they didn't visit
    df_transactions =   df_transactions.fillna(0)


    # initialise the variables
    months = len(df_transactions.columns) 
    month_counter = 1
    row = 0
    churned = []
    new = []
    returned = []
    master_churned = []

    # The loop calculates the churned(hasn't come into salon for two months), new and returned (was churned but now back)
    for month in np.arange(1,months):
        for row in np.arange(0,len(df_transactions.index)):
            customer_name = df_transactions.index[row]
            
        
            if (df_transactions.iloc[row,month_counter] == 0):
                
                if month_counter != 1:
                
                    if df_transactions.iloc[row,month_counter-1] == 0 and df_transactions.iloc[row,month_counter-2] != 0:
                        churned.append(customer_name)
                        master_churned.append(customer_name)
                        
            else:
                
                if customer_name in master_churned:
                    returned.append(customer_name)
                    master_churned.remove(customer_name)
                        
                elif sum(df_transactions.iloc[row,0:month_counter]) == 0:
                    new.append(customer_name)
                    
                      
            
        returned_num = len(returned)
        new_num = len(new)
        churned_num = len(churned)
        
        #create the dataframes that will hold the three variables for each month
        if month_counter == 1:
            data = {"Regained": [returned_num], "New": [new_num], "Churned": [churned_num]}
            churn_dataframe = pd.DataFrame(data, index = [month_counter+1])
            
        else:
            data = {"Regained": [returned_num], "New": [new_num], "Churned": [churned_num]}
            data_df = pd.DataFrame(data, index = [month_counter+1])
            churn_dataframe = pd.concat([churn_dataframe,data_df])      
    
        new = []
        churned = []
        returned = []
        month_counter +=1 

    churn_dataframe["months"] = df_transactions.columns[1:]

    return churn_dataframe[2:]

  