#!/usr/bin/python

from __future__ import print_function
import MySQLdb
import pandas as pd
import time
import numpy as np
import configparser

# parse config file and set user/password
parser = configparser.RawConfigParser()   
configFilePath = 'replica.my.cnf'
parser.read(configFilePath)
passdict = {}
for section_name in parser.sections():
    # print ('Section:', section_name)
    # print ('  Options:', parser.options(section_name))
    for name, value in parser.items(section_name):
        passdict[name] = value
    #     print ('  %s = %s' % (name, value))
    # print

hostname = '127.0.0.1'
username = passdict['user']
password = passdict['password']
database = 'enwiki_p'
port = 13306

start_time = time.time()

# file ="wpCreatorsAllBiographies.csv"
file ="100000pageids.csv"


def doQuery( conn, id_list, dfallmetas ) :
	cur = conn.cursor()	
	# correctly returns the values for one id/time
# 	sql_insert_query = "SELECT p.rev_id FROM revision r, revision p, comment c, change_tag ch WHERE r.rev_page={0} AND r.rev_deleted=0 AND p.rev_deleted=0 AND r.rev_timestamp < {1} AND r.rev_parent_id=p.rev_id AND r.rev_comment_id = c.comment_id AND c.comment_text NOT LIKE '%Undid revision%' AND ch.ct_rev_id=p.rev_id AND ch.ct_tag_id NOT IN (590, 1, 8, 16, 582, 21, 20, 28, 577, 6, 31, 26, 14, 43, 39, 32, 60, 29, 52, 46, 561, 45, 59, 56, 216, 172, 87, 193, 217, 86, 539) ORDER BY r.rev_timestamp DESC LIMIT 1".format(page_id, date)
	first = 24176
	second = 311615
	#returns ALL times for multiple ids
	
	if isinstance(id_list, list):
		sql_insert_query = "SELECT r.rev_page, p.rev_id p_rev_id, r.rev_id c_rev_id, r.rev_timestamp c_timestamp,p.rev_timestamp timestamp, pg.page_id target, op.page_id original,IFNULL(pg.page_id,op.page_id) conditional FROM  	page op LEFT JOIN 	redirect rd ON (rd_from=op.page_id) LEFT JOIN  	page pg  	ON (pg.page_namespace=rd_namespace AND pg.page_title=rd_title) LEFT JOIN 	revision r  	ON IFNULL(pg.page_id,op.page_id) = r.rev_page LEFT JOIN 	revision p 	ON r.rev_parent_id=p.rev_id LEFT JOIN 	comment c  	ON r.rev_comment_id = c.comment_id LEFT JOIN 	change_tag ch  	ON ch.ct_rev_id=p.rev_id WHERE op.page_id IN ({}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {},{}, {}, {}, {}, {}, {}, {}, {}, {}, {}) AND r.rev_deleted=0 AND (p.rev_deleted=0 OR p.rev_deleted IS NULL) AND c.comment_text NOT LIKE '%Undid revision%' AND (ch.ct_params IS NULL OR ch.ct_tag_id NOT IN (590, 1, 8, 16, 582, 21, 20, 28, 577, 6, 31, 26, 14, 43, 39, 32, 60, 29, 52, 46, 561, 45, 59, 56, 216, 172, 87, 193, 217, 86, 539))  ORDER BY r.rev_timestamp DESC;".format(id_list[0],id_list[1],id_list[2],id_list[3],id_list[4],id_list[5],id_list[6],id_list[7],id_list[8],id_list[9],id_list[10],id_list[11],id_list[12],id_list[13],id_list[14],id_list[15],id_list[16],id_list[17],id_list[18],id_list[19],id_list[20],id_list[21],id_list[22],id_list[23],id_list[24],id_list[25],id_list[26],id_list[27],id_list[28],id_list[29],id_list[30],id_list[31],id_list[32],id_list[33],id_list[34],id_list[35],id_list[36],id_list[37],id_list[38],id_list[39],id_list[40],id_list[41],id_list[42],id_list[43],id_list[44],id_list[45],id_list[46],id_list[47],id_list[48],id_list[49],id_list[50],id_list[51],id_list[52],id_list[53],id_list[54],id_list[55],id_list[56],id_list[57],id_list[58],id_list[59],id_list[60],id_list[61],id_list[62],id_list[63],id_list[64],id_list[65],id_list[66],id_list[67],id_list[68],id_list[69],id_list[70],id_list[71],id_list[72],id_list[73],id_list[74],id_list[75],id_list[76],id_list[77],id_list[78],id_list[79],id_list[80],id_list[81],id_list[82],id_list[83],id_list[84],id_list[85],id_list[86],id_list[87],id_list[88],id_list[89],id_list[90],id_list[91],id_list[92],id_list[93],id_list[94],id_list[95],id_list[96],id_list[97],id_list[98],id_list[99])
# 		sql_insert_query = "SELECT r.rev_page, p.rev_id, p.rev_timestamp, TIMEDIFF(r.rev_timestamp,p.rev_timestamp) FROM revision r, revision p, comment c WHERE r.rev_page IN ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})   AND r.rev_deleted=0 AND p.rev_deleted=0 AND r.rev_parent_id=p.rev_id AND r.rev_comment_id = c.comment_id AND c.comment_text NOT LIKE '%Undid revision%' ORDER BY r.rev_timestamp DESC;".format(id_list[0],id_list[1],id_list[2],id_list[3],id_list[4],id_list[5],id_list[6],id_list[7],id_list[8],id_list[9])
		print('set list query')
	else:
		sql_insert_query = "SELECT r.rev_page, p.rev_id p_rev_id, r.rev_id c_rev_id, TIMEDIFF(r.rev_timestamp,p.rev_timestamp), pg.page_id target, op.page_id original,IFNULL(pg.page_id,op.page_id) conditional FROM  	page op LEFT JOIN 	redirect rd ON (rd_from=op.page_id) LEFT JOIN  	page pg  	ON (pg.page_namespace=rd_namespace AND pg.page_title=rd_title) LEFT JOIN 	revision r  	ON IFNULL(pg.page_id,op.page_id) = r.rev_page LEFT JOIN 	revision p 	ON r.rev_parent_id=p.rev_id LEFT JOIN 	comment c  	ON r.rev_comment_id = c.comment_id LEFT JOIN 	change_tag ch  	ON ch.ct_rev_id=p.rev_id WHERE op.page_id IN ({}) AND r.rev_deleted=0 AND (p.rev_deleted=0 OR p.rev_deleted IS NULL) AND c.comment_text NOT LIKE '%Undid revision%' AND (ch.ct_params IS NULL OR ch.ct_tag_id NOT IN (590, 1, 8, 16, 582, 21, 20, 28, 577, 6, 31, 26, 14, 43, 39, 32, 60, 29, 52, 46, 561, 45, 59, 56, 216, 172, 87, 193, 217, 86, 539))  ORDER BY r.rev_timestamp DESC;".format(id_list)
# 		sql_insert_query = "SELECT r.rev_page, p.rev_id, p.rev_timestamp, TIMEDIFF(r.rev_timestamp,p.rev_timestamp) FROM revision r, revision p, comment c WHERE r.rev_page IN ({}) AND r.rev_deleted=0 AND p.rev_deleted=0 AND r.rev_parent_id=p.rev_id AND r.rev_comment_id = c.comment_id AND c.comment_text NOT LIKE '%Undid revision%' ORDER BY r.rev_timestamp DESC;".format(id_list)
		print('set single query')

	cur.execute( sql_insert_query )
	
	rows = []
	for row in cur.fetchall():
		data = {}
		data['page_id'] = row[0]
		data['rev_id'] = row[1]
		#for each page_id, i will need to capture the largest c_rev_id (most recent edit)
		data['c_rev_id'] = row[2]
		data['c_timestamp'] = row[3]
		data['timestamp'] = row[4]
		rows.append(data)
	dfmetas = pd.DataFrame(rows)
	dfallmetas = pd.concat([dfallmetas, dfmetas], ignore_index=True, sort=False)
# 	print(dfallmetas.size)
# 	print(dfmetas.size)
# 	print(dfmetas)
# 	print(dfallmetas)
	print('stored metas')
	
	return dfallmetas
# 	print( result_list )


print( "Using mysqlclient (MySQLdb):" )
#construct connection
myConnection = MySQLdb.connect( host=hostname, user=username, passwd=password, db=database, port=port )

#read file and make slices
df = pd.read_csv(file)
print(df.info())
df = df.fillna(0)
print(df["Page ID"])

# QID	Creator	Creation Date	Page ID
# df = df[df.line_race != 0]

# exclude repeated headers
df = df.loc[df["QID"] != "QID"]
df = df.loc[df["Creator"] != "Creator"]
df = df.loc[df["Creation Date"] != "Creation Date"]
df = df.loc[df["Page ID"] != "Page ID"]

# set datatypes
df["Page ID"] = df['Page ID'].astype('float')
df["Page ID"] = df['Page ID'].astype('int')
print(df.count)
n = 100  #chunk row size
counter = 0
list_df = [df[i:i+n] for i in range(0,df.shape[0],n)]
slices = len(list_df)

print(list_df[0].count)
print(slices)

dfallmetas = pd.DataFrame(columns=['page_id','rev_id','c_rev_id','c_timestamp','timestamp'])
dfnewest = pd.DataFrame(columns=['page_id','rev_id','c_rev_id','c_timestamp','timestamp'])

id_exec_list = []
# date = 20200101000000
# date_list = [20220804000000, 20210804000000]
for slice in range(0,2): # for testing
# for slice in range(0,slices):
	id_list = list_df[slice]['Page ID'].fillna(0).astype(int).tolist()	
	id_exec_list = id_exec_list + id_list
	
	if len(id_list) < n:
		print ("TOOOOO SMAAAALLLLLLL")
		for page_id in id_list:
			dfallmetas = doQuery( myConnection, page_id, dfallmetas )
			pass
	else:
# 		print (len(id_list))	
		#execute query
		dfallmetas = doQuery( myConnection, id_list, dfallmetas )
		pass
	
	counter += 1
	print(counter)
	if counter > 0: #for testing
	# if counter % n == 0:

		#set datatypes
		dfallmetas["rev_id"] = dfallmetas['rev_id'].fillna(0).astype('int')
		dfallmetas["c_rev_id"] = dfallmetas['c_rev_id'].fillna(0).astype('int')
		dfallmetas['c_timestamp'] = dfallmetas['c_timestamp'].fillna(0).astype('int64').astype(str)
		dfallmetas['timestamp'] = dfallmetas['timestamp'].fillna(0).astype('int64').astype(str)
		print(dfallmetas.info())

		#locate and concat the most most recent timestamp
		idx = dfallmetas.groupby(['page_id'])['timestamp'].transform(max) == dfallmetas['timestamp']
		dftemp = dfallmetas[idx]


		# tempy = dfallmetas.groupby(['page_id'])['timestamp'].agg('max')
		tempy = dfallmetas.groupby(['page_id'])['timestamp'].transform(max)

		print('tempy')
		print(tempy.count)
		print(tempy)

		# remove your pwds from the file!!! then commit
		# why is it not clearing the df after saving?
		# why is the newest the same for both saves?
		# why can't i group and max the newest ones? 
		# then i need to reassign the column names, moving most recent parent to the child column

		dfnewest['page_id'] = dftemp.iloc[:, 0]
		dfnewest['c_rev_id'] = dftemp.iloc[:, 1]
		dfnewest['c_timestamp'] = dftemp.iloc[:, 4]
		# dfnewest = dfnewest.unique()

		# # well, that did SOMETHING. unsure what...!!!! 
		# dfnewest = dfnewest.groupby('page_id')['c_timestamp'].apply(lambda x: list(np.unique(x)))

		print('dfnewest')
		print(dfnewest)

		# dfallmetas = pd.concat([dfallmetas, dfnewest], ignore_index=True, sort=False)

		#save df and reset to empty
		savepath = f"output/enwiki_bio_rev_ids{str(counter)}.csv"
		savepathnew = f"output/enwiki_bio_rev_idsnew{str(counter)}.csv"
		print(savepath)
		dfallmetas.to_csv(savepath, index=False)
		dfnewest.to_csv(savepathnew, index=False)
		dfallmetas.iloc[0:0]	
		dfnewest.iloc[0:0]	
		dftemp.iloc[0:0]	

print(dfallmetas)

redirects = []
for element in id_exec_list:
	if element not in dfallmetas.page_id.unique().tolist():
		redirects.append(element)

print("these didn't come back:")
print(redirects)

moved =[]
df_list = dfallmetas.page_id.unique().tolist()
for element in df_list:
	if element not in id_exec_list:
		moved.append(element)

print("these are my new page IDs:")
print(moved)


#when done, close
myConnection.close()

print("query completed")
print("--- %s seconds ---" % (time.time() - start_time))

