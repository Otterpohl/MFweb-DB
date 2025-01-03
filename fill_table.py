#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
 Script to fill the MF DB tables
'''

# Connect to the DB 
import sqlalchemy as db
import h5py

# Establish the connection with the DB: 
engine     = db.create_engine('postgresql://root@localhost:5432/mf', echo = True)
metadata   = db.MetaData()
connection = engine.connect()

# print the tables names present in the DB: 
engine.table_names()

for task_no in range(1,101):
    
    folder_usr = '../data/generative/tasks/task_' + str(task_no) + '/'
    
    ###########################
    ###### Table: task  #######
    ###########################
    
    # Specify the table you want to insert the data into: 
    table  = db.Table('task', metadata, autoload=True, autoload_with=engine)
    
    #Inserting many records at ones
    query  = db.insert(table) 

    with open(folder_usr + 'Task.mat', 'r') as file:
        for line in file:
            Tab = dict()
            if not line.strip().startswith('#'):
                parts = line.split()
                if len(parts) == 48:
                    Tab['TaskNo']              = parts[0]
                    Tab['TrialNo']             = parts[1]
                    Tab['BlockNo']             = parts[2]
                    Tab['Horizon']             = parts[3]
                    Tab['ItemNo']              = parts[4]
                    Tab['InitialSampleNb']     = parts[5]
                    Tab['UnusedTree']          = parts[6]      
                    Tab['DisplayOrder1']       = parts[7]
                    Tab['DisplayOrder2']       = parts[8]
                    Tab['DisplayOrder3']       = parts[9]     
                    Tab['TreePositions1']      = parts[10]
                    Tab['TreePositions2']      = parts[11]
                    Tab['TreePositions3']      = parts[12]
                    Tab['TreePositions4']      = parts[13]       
                    Tab['InitialSample1Tree']  = parts[14]
                    Tab['InitialSample2Tree']  = parts[15]
                    Tab['InitialSample3Tree']  = parts[16]
                    Tab['InitialSample4Tree']  = parts[17]
                    Tab['InitialSample5Tree']  = parts[18]
                    Tab['InitialSample1Size']  = parts[19]
                    Tab['InitialSample2Size']  = parts[20]
                    Tab['InitialSample3Size']  = parts[21]
                    Tab['InitialSample4Size']  = parts[22]
                    Tab['InitialSample5Size']  = parts[23]
                    Tab['Tree1FutureSize1']    = parts[24]
                    Tab['Tree1FutureSize2']    = parts[25]
                    Tab['Tree1FutureSize3']    = parts[26]
                    Tab['Tree1FutureSize4']    = parts[27]
                    Tab['Tree1FutureSize5']    = parts[28]
                    Tab['Tree1FutureSize6']    = parts[29]
                    Tab['Tree2FutureSize1']    = parts[30]
                    Tab['Tree2FutureSize2']    = parts[31]
                    Tab['Tree2FutureSize3']    = parts[32]
                    Tab['Tree2FutureSize4']    = parts[33]
                    Tab['Tree2FutureSize5']    = parts[34]
                    Tab['Tree2FutureSize6']    = parts[35]
                    Tab['Tree3FutureSize1']    = parts[36]
                    Tab['Tree3FutureSize2']    = parts[37]
                    Tab['Tree3FutureSize3']    = parts[38]
                    Tab['Tree3FutureSize4']    = parts[39]
                    Tab['Tree3FutureSize5']    = parts[40]
                    Tab['Tree3FutureSize6']    = parts[41]
                    Tab['Tree4FutureSize1']    = parts[42]
                    Tab['Tree4FutureSize2']    = parts[43]
                    Tab['Tree4FutureSize3']    = parts[44]
                    Tab['Tree4FutureSize4']    = parts[45]
                    Tab['Tree4FutureSize5']    = parts[46]
                    Tab['Tree4FutureSize6']    = parts[47]
            ResultProxy = connection.execute(query,[Tab])
   
    ###############################
    ###### Table: training  #######
    ###############################
    
for training_no in range(1,101):
    
    folder_usr = '../data/generative/trainings/training_' + str(training_no) + '/'
    
    # Specify the table you want to insert the data into: 
    table  = db.Table('training', metadata, autoload=True, autoload_with=engine)
    
    #Inserting many records at ones
    query  = db.insert(table) 

    with open(folder_usr + 'Training.mat', 'r') as file:
        for line in file:
            Tab = dict()
            if not line.strip().startswith('#'):
                parts = line.split()
                if len(parts) == 9:
                    Tab['TrainingNo']          = parts[0]
                    Tab['TrialNo']             = parts[1]
                    Tab['InitialSample1Size']  = parts[2]
                    Tab['InitialSample2Size']  = parts[3]
                    Tab['InitialSample3Size']  = parts[4]
                    Tab['Choice1Size']         = parts[5]
                    Tab['Choice2Size']         = parts[6]
                    Tab['Choice1Correct']      = parts[7]
                    Tab['Choice2Correct']      = parts[8]
            values_list = [Tab]
            ResultProxy = connection.execute(query,values_list)
         
         
        


