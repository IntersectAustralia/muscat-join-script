"""
written for Python2

Python script to import Canonic techniques records into the Muscat database. It does three things:
1. Imports Canonic techniques records into the canonic_techniques table
2. Imports the join records into the sources_to_canonic_techniques table
3. Adds canonic techniques marc data using the marc code "695" to the source record in the sources table.

It takes two CSV datasets:
1. The ingest for the authority Canonic Techniques table, canonic_techniques. This file has to be CSV and must be in the same table structure as the database table.
2. The ingest for the source_to_canonic_techniques table. This file has to be CSV and must contain the joining data from canonic_technique_id to source_id

Created by Paddy Tobias, Intersect Australia (July, 2018)
"""

## insert DB globals
host = <<insert local host>>
user = <<insert DB user>>
password = <<insert password>>
database = <<insert database name>>


import csv
import MySQLdb


conn = MySQLdb.connect(host, user, password, database)
cursor = conn.cursor()


def build_ct_marc(canon_type="", relation_operator="", relation_numerator ="", relation_denominator="", interval="", interval_direction="", temporal_offset="", offset_units="", mensurations="", ct_id=""):
    canon_type="$a"+canon_type if canon_type!="" else ""
    relation_numerator="$b"+relation_numerator if relation_numerator!="" else ""
    relation_denominator="$c"+relation_denominator if relation_denominator!="" else ""
    relation_operator="$d"+relation_operator if relation_operator!="" else ""
    interval="$e"+interval if interval!="" else ""
    interval_direction="$f"+interval_direction if interval_direction!="" else ""
    temporal_offset="$g"+temporal_offset if temporal_offset!="" else ""
    offset_units="$h"+offset_units if offset_units!="" else ""
    mensurations="$o"+mensurations if mensurations!="" else ""
    ct_id="$0"+str(ct_id) if ct_id!="" else ""
    ct_marc = canon_type+relation_numerator+relation_denominator+relation_operator+interval+interval_direction+temporal_offset+offset_units+mensurations+ct_id
    ct_marc="\n=695 ##"+ct_marc if ct_marc!="" else ""
    return ct_marc

def get_marc(source_id):
    cursor.execute("""SELECT marc_source FROM sources WHERE id=%s;""" % source_id)
    return cursor.fetchall()

def get_ct(source_id, ct_id):
    cursor.execute("""SELECT `canon_type`, `relation_operator`, `relation_numerator`, `relation_denominator`, `interval`, `interval_direction`, `temporal_offset`, `offset_units`, `mensurations` FROM canonic_techniques, sources_to_canonic_techniques WHERE id=canonic_technique_id AND source_id=%s AND canonic_technique_id=%s;""" % (source_id, ct_id))
    return cursor.fetchall()

def insert_marc(source_id, ct_id):
    insert_marc_statement="""UPDATE sources SET marc_source = "%s" WHERE id=%s;"""
    source_ct = get_ct(source_id, ct_id)
    current_marc = get_marc(source_id)
    ct_marc = build_ct_marc(source_ct[0][0],source_ct[0][1], source_ct[0][2], source_ct[0][3], source_ct[0][4], source_ct[0][5], source_ct[0][6], source_ct[0][7], source_ct[0][8], ct_id)
    new_marc = current_marc[0][0]+ct_marc
    insert = insert_marc_statement % (new_marc, source_id)
    cursor.execute(insert)
    conn.commit()



## baulk ingesting canonic techniques table. Importing table must be in same structure as the Muscat version.
with open('canonic_techniques.csv', 'rb') as f:
    reader = csv.reader(f)
    # reader = next(reader)
    insert_row = """INSERT INTO canonic_techniques (`canon_type`, `relation_operator`, `relation_numerator`, `relation_denominator`, `interval`, `interval_direction`, `temporal_offset`, `offset_units`, `mensurations`, `wf_audit`, `wf_stage`, `wf_notes`, `wf_owner`, `wf_version`, `created_at`, `updated_at`, `lock_version`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, '%s', '%s', %s);"""
    counter = 0
    for row in reader:
        if counter!=0:
            insert = insert_row % (row[0],row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16])
            cursor.execute(insert)
            conn.commit()
        counter = counter + 1

## baulk ingesting sources_to_canonic_techniques table. Importing table must be in same structure as the Muscat version
with open('sources_to_canonic_techniques.csv', 'rb') as f:
    reader = csv.reader(f)
    insert_row="""INSERT INTO sources_to_canonic_techniques VALUES (%s, %s);"""
    counter = 0
    for row in reader:
        if counter!=0:
            insert = insert_row % (row[0], row[1])
            cursor.execute(insert)
            conn.commit()
            # insert marc_source data into Source record for new Canonic Techniques record. The new marc_source data uses the 695 marc code
            insert_marc(source_id=row[1], ct_id=row[0])
            print "update completed for source id: %s and CT id: %s" % (row[1], row[0])
        counter = counter+1

conn.close()
