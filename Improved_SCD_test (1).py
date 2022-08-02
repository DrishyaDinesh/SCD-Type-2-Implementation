#!/usr/bin/env python
# coding: utf-8

# In[10]:


import os
from snowflake.snowpark import *
from snowflake.snowpark.functions import *

from datetime import date, datetime


# In[39]:


def main(session, from_table, trg_table, to_table, load_type, hist_col, key, colnames, trsfrm_rule, trsfrm_res_col):
    
    if load_type.lower()=='spt':
        print("Creating DataFrame from Source Table: ")
        df_source= session.table(from_table)
        df_source.show()
        
        df_source_new = df_source.withColumn("tgt_"+str(key),col(key))
        
        for c in colnames:
            df_source_new = df_source_new.withColumn("tgt_"+str(c),col(c))
        
        column_names = ["tgt_"+str(key)]
        for i in colnames:
            column_names.append("tgt_"+str(i))
        
        df_source_new = df_source_new.select(column_names)
        
        trsfrm_rule1 = eval(trsfrm_rule)
        df_merge_final = df_source_new.withColumn(trsfrm_res_col,trsfrm_rule1).sort(col("tgt_"+str(key)))
        df_merge_final.show()
    
        print("Saving the final target table in the Snowflake Database")
        df_merge_final.write.mode("overwrite").saveAsTable(to_table)
        print("Target table SAVED in the Snowflake Database")

        print("")
        print("Reading data from the new target table")
        session.sql("Select * from "+to_table).show()
        
        return "SIMPLE PASS THROUGH IMPLEMENTED SUCCESSFULLY WITH TRANSFORMATIONS USING SNOWFLAKE STORED PROCEDURE!!"
    
    elif load_type.lower()=='scdtype1':
        
        print("Creating DataFrame from Source Table: ")
        df_source= session.table(from_table)
        df_source.show()
    
        print("Creating DataFrame from Target Table: ")
        df_target = session.table(trg_table)
        df_target.show()


        #FULL Merge, join on key column and high date column to only join to the latest records
        df_merge = df_target.join(df_source, (df_source.col(key) == df_target.col("tgt_"+str(key))),'full')
        df_merge.show()

        #Insert a new column to indicate the action
        df_merge = df_merge.withColumn("Action",when(col("tgt_"+str(hist_col)) != col(hist_col),lit("UPDATE")).when(is_null
                    (col("tgt_"+str(key))) & ~(is_null(col(key))), lit("INSERT")).otherwise(lit("NOACTION")))
        df_merge.show()

        #Generate the new data frames based on action code
        column_names = ["tgt_"+str(key)]
        for i in colnames:
            column_names.append("tgt_"+str(i))
    
        print("For records that require no action")
        df_merge_p1 = df_merge.filter(df_merge.col("action") == "NOACTION").select(column_names)
        df_merge_p1.show()

        print("For records that need to be inserted only")
        p2_cols = [key]
        for i in colnames:
            p2_cols.append(str(i))
    
        df_merge_p2= df_merge.filter(df_merge.col("action") == "INSERT").select(p2_cols)
        df_merge_p2.show()
        
        column_names_up = ["tgt_"+str(key)]
        for i in colnames:
            if i not in [hist_col]:
                column_names_up.append("tgt_"+str(i))
        column_names_up.append(hist_col)
    
        print("For records that need to be updated")
        df_merge_p4_2 = df_merge.filter(df_merge.col("action") == "UPDATE").select(column_names_up)
        df_merge_p4_2.show()
        
        # Union all DataFrames (records) together
        print("Final Target Table with updated information: ")
        trsfrm_rule1 = eval(trsfrm_rule)
        df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p4_2)
        df_merge_final = df_merge_final.withColumn(trsfrm_res_col,trsfrm_rule1).sort(col("tgt_"+str(key)))
        df_merge_final.show()
    
        print("Saving the final target table in the Snowflake Database")
        df_merge_final.write.mode("overwrite").saveAsTable(to_table)
        print("Target table SAVED in the Snowflake Database")

        print("")
        print("Reading data from the new target table")
        session.sql("Select * from "+to_table).show()
        
        return "SCD TYPE 1 IMPLEMENTED SUCCESSFULLY WITH TRANSFORMATIONS USING SNOWFLAKE STORED PROCEDURE!!"
    
    elif load_type.lower()=='scdtype2': 
        print("Creating DataFrame from Source Table: ")
        df_source= session.table(from_table)
        df_source.show()
    
        print("Creating DataFrame from Target Table: ")
        df_target = session.table(trg_table)
        df_target.show()
    
        date_string = '9999-12-31'
        high_date = datetime.strptime(date_string, '%Y-%m-%d').date()
    
        current_date = date.today()

        #Prepare for merge - Add start and end date columns in source table
        df_source_new = df_source.withColumn("src_start_date", lit(current_date)).withColumn("src_end_date", lit(high_date))
        df_source_new.show()

        #FULL Merge, join on key column and high date column to only join to the latest records
        df_merge = df_target.join(df_source_new, ((df_source_new.col(key) == df_target.col("tgt_"+str(key)))
        & (df_source_new.col("src_end_date") == df_target.col("end_date"))),"full")
        df_merge.show()

        #Insert a new column to indicate the action
        df_merge = df_merge.withColumn("Action",when(col("tgt_"+str(hist_col)) != col(hist_col),lit("UPSERT"))
        .when(is_null(col("tgt_"+str(key))) & ~(is_null(col(key))), lit("INSERT"))
        .otherwise(lit("NOACTION")))
        df_merge.show()

        #Generate the new data frames based on action code
        column_names = ["tgt_"+str(key)]
        for i in colnames:
            column_names.append("tgt_"+str(i))
        column_names= column_names+ ["is_active", "start_date", "end_date"]
    
        print("For records that require no action")
        df_merge_p1 = df_merge.filter(df_merge.col("action") == "NOACTION").select(column_names)
        df_merge_p1.show()

        print("For records that need to be inserted only")
        p2_cols = [key]
        for i in colnames:
            p2_cols.append(str(i))
        p2_cols = p2_cols + ["is_active", "start_date", "end_date"]  
    
        df_merge_p2= df_merge.filter(df_merge.col("action") == "INSERT").withColumn("is_active",lit(1)).withColumn("start_date",
                    df_merge.col("src_start_date")).withColumn("end_date",df_merge.col("src_end_date")).select(p2_cols)
        df_merge_p2.show()
    
        print("For records that need to be updated and inserted")
        print("Insertion table")
        df_merge_p4_1 = df_merge.filter(df_merge.col("action")  == "UPSERT").withColumn("is_active",lit(1)).withColumn("start_date",
                    df_merge.col("src_start_date")).withColumn("end_date",df_merge.col("src_end_date")).select(p2_cols)
        df_merge_p4_1.show()
    
        df_merge=df_merge.withColumn("i", lit(-1))
    
        print("Updation Table")
        df_merge_p4_2 = df_merge.filter(df_merge.col("action") == "UPSERT").withColumn("end_date",dateadd("day",df_merge.col("i"),
                    df_merge.col("src_start_date"))).withColumn("is_active", lit(0)).select(column_names)
        df_merge_p4_2.show()

    
        # Union all DataFrames (records) together
        print("Final Target Table with updated information: ")
        trsfrm_rule1 = eval(trsfrm_rule)
        df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p4_1).unionAll(df_merge_p4_2)
        df_merge_final = df_merge_final.withColumn(trsfrm_res_col,trsfrm_rule1).sort(col("tgt_"+str(key)), col("start_date"))
        df_merge_final.show()
    
        print("Saving the final target table in the Snowflake Database")
        df_merge_final.write.mode("overwrite").saveAsTable(to_table)
        print("Target table SAVED in the Snowflake Database")

        print("")
        print("Reading data from the new target table")
        session.sql("Select * from "+to_table).show()
    
        return "SCD TYPE 2 IMPLEMENTED SUCCESSFULLY WITH TRANSFORMATIONS USING SNOWFLAKE STORED PROCEDURE!!"


# In[45]:


main(session,'customer','customer_lkp','cust_final_tgt','SCDTYPE2','email','cust_id',['name','age','gender','email'],'concat(concat(col("tgt_name"),lit("#")),col("tgt_cust_id"))','CONCAT_RESULT')


# In[ ]:




