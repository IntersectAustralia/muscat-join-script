'''
written for Python2
    
PYTHON SCRIPT TO CHECK IF LINKING DATA EXISTS IN sources_to_people TABLE

written by Paddy Tobias, Intersect Australia (June, 2018)
'''

import MySQLdb
import re
import os
import logging
import datetime

# GLOBALS
logging.basicConfig(filename='join_check.log',level=logging.DEBUG)

host = "localhost"
user = "join_check"
password = "python"
database = "muscat"

# for all records in SOURCES table, get source_id and Partner object id and append to join_table. Partner ids are coming from the marc data in the source record (Column 14 in the SOURCES table).
def get_sources_join(marc_code):
    join_table = []
    # query sources table
    cursor.execute("SELECT * FROM sources;")
    sources_result = cursor.fetchall()
    for i in range(len(sources_result)):
        record_index = sources_result[i][0] # source_id
        marc_all = str(sources_result[i][14]).split('\n=') # marc data appears in 14th column
        # search marc data to find data for the particular marc code being used
        for j in range(len(marc_all)):
            if re.search(('%s  ..\\$' % marc_code),marc_all[j]):
                partner_id = marc_all[j].replace('\n', ' ').replace('$', ' ').split()[-1]  # save associated partner id for this source record
                partner_id = re.sub("^0+", "",partner_id)  # strip 0 in numbers 1-9
                partner_id = re.sub("x", "",partner_id)  # strip x if exists
                partner_id = re.sub("w", "",partner_id)
                join_table.append((int(partner_id), record_index, marc_all[j]))  # append to list
    return join_table # return list of join data that *should* be in the database. sources_join_check_and_insert() checks against this list


# get sources_to_people DB table
def sources_join(model_name):
    cursor.execute("""SELECT * FROM sources_to_%s""" % model_name)
    return cursor.fetchall()


# do a check on whether linking data exists in sources_to_people, if not, insert it.
def sources_join_check_and_insert(marc_code, model_name):
    join_table = get_sources_join(marc_code)
    sources_join_query = sources_join(model_name)
    prepare_insert = "INSERT INTO sources_to_%s VALUES (%s, %s)"
    for i in range(len(join_table)):
        if join_table[i][0:2] not in sources_join_query:  # do the join check. if not in, insert
            logging.warning("Join record %s not in sources_to_%s table" % (join_table[i][2], model_name))
            insert = prepare_insert % (model_name,join_table[i][0], join_table[i][1])
            cursor.execute(insert)  # insert join record
            conn.commit()
            logging.info("Join record added to sources_to_%s table" % model_name)
        else:
            logging.info("Join record %s already in sources_to_%s table" % (join_table[i][2], model_name))


# DB connection
conn = MySQLdb.connect(host, user, password, database)
cursor = conn.cursor()

# start checking
logging.info("#######################   CHECKING STARTED AT %s   #######################" % datetime.datetime.now())

# dictionary holding marc codes for sources joining partners. PLACES is not checked
source_partner_marc_code = {'people': 100, 'institutions': 852, 'canonic_techniques': 695, 'liturgical_feasts': 657,
                            'places': 'NULL', 'sources': 773, 'catalogues': 690, 'standard_terms': 650,
                            'standard_titles': 240, 'works': 930}

for model in source_partner_marc_code:
    if source_partner_marc_code[model] != 'NULL':
        marc_code = source_partner_marc_code[model]
        model_name = model
        sources_join_check_and_insert(marc_code, model_name)

conn.close()
logging.info("#######################   CHECKING COMPLETED AT %s   #######################" % datetime.datetime.now())

# reindex application once complete
os.system("RAILS_ENV=production bundle exec rake sunspot:reindex")



for i in range(len(sources_result)):
    record_index = sources_result[i][0] # source_id
    marc_all = str(sources_result[i][14]).split('=[1-9]{3}') # marc data appears in 14th column
        # search marc data to find data for the particular marc code being used
    for j in range(len(marc_all)):
        if re.search(('%s  ..\\$' % marc_code),marc_all[j]):
            partner_id = marc_all[j].replace('\n', ' ').replace('$', ' ').split()[-1]  # save associated partner id for this source record
            partner_id = re.sub("^0+", "",partner_id)  # strip 0 in numbers 1-9
            partner_id = re.sub("x", "",partner_id)  # strip x if exists
            partner_id = re.sub("w", "",partner_id)
            join_table.append((int(partner_id), record_index, marc_all[j]))
