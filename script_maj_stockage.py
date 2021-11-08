import pandas as pd
import numpy as np
import datetime
import psycopg2
import sqlalchemy
import os
from sqlalchemy import create_engine
import math
import psycopg2
import psycopg2.extras as extras
from config import config, path
from connect import connect

path = path()

# Paramètre de connexion à la base de donnée
params = config()
# connect to the PostgreSQL server
print('Connecting to the PostgreSQL database...')
connection = psycopg2.connect(**params)

cur = connection.cursor()

basepath_ibm_volume = path + 'ibm/volume'

fichiers_ibm_volume=[]
with os.scandir(basepath_ibm_volume) as entries: 
    for entry in entries:
        if entry.is_file():
            fichiers_ibm_volume.append(str(entry.name))

query_read_fichiers = "SELECT * FROM stk_fichiers"
cur.execute(query_read_fichiers)
raw_fichiers = cur.fetchall()
df_fichiers = pd.DataFrame(raw_fichiers, columns =["nom_fichier","origine"])

fichiers_ibm_a_recuperer = []
for i in fichiers_ibm_volume:
    if i not in df_fichiers.nom_fichier.to_list():
        fichiers_ibm_a_recuperer.append(i)

def en_MB(x):
    if type(x)==str:
        if "GB" in x :
            x = x.replace("GB","")
            x= float(x)*1024
        elif "G" in x:
            x = x.replace("G","")
            x= float(x)*1024
        elif "MB" in x:
            x = x.replace("MB","")
            x= float(x)
        elif "TB" in x:
            x = x.replace("TB","")
            x = float(x)*1024*1024
    elif math.isnan(float(x)):
        pass
    return x

def en_giga(x):
    return x/1024

def typo(serveur):
    if "APP" in serveur:
        typo = "APP"
    elif "WEB" in serveur:
        typo = "WEB"
    elif "DAT" in serveur:
        typo = "DAT"
    elif "SQL" in serveur:
        typo = "DAT"
    elif "ORA" in serveur:
        typo = "DAT"
    elif "VIR" in serveur:
        typo = "VIR"
    elif "vir" in serveur:
        typo = "VIR"
    elif "FIC" in serveur:
        typo = "FIC"
    elif "MES" in serveur:
        typo = "MES"
    elif "DNS" in serveur:
        typo = "DNS"
    else:
        typo = "AUTRE"
 
    return typo    

if len(fichiers_ibm_a_recuperer)==0:
    print('pas de maj ibm')
else:
    basepath_ibm_volume = path + 'ibm/volume/'
    df_ibm_volume = pd.DataFrame()
    for name in fichiers_ibm_a_recuperer:
        with open(os.path.join(basepath_ibm_volume, name)) as f:
            df_temp = pd.read_fwf(f)
            df_temp['date']= datetime.datetime.strptime(str(int(name[:8])),'%Y%m%d')
            df_ibm_volume = pd.concat([df_ibm_volume, df_temp])

    #suppression des lignes où la used_capcity est null
    df_ibm_volume = df_ibm_volume[df_ibm_volume.used_capacity.notnull()]
    for colonne in ["capacity", "used_capacity","real_capacity", "free_capacity", "uncompressed_used_capacity", "used_capacity_before_reduction"]:
        df_ibm_volume[colonne] = df_ibm_volume[colonne].map(lambda x : en_MB(x) )
        
    df_ibm_volume = df_ibm_volume.rename(columns={"vdisk_id":"ID","vdisk_name":"name","mdisk_grp_name": "pool"})
    df_ibm_volume['offre'] = df_ibm_volume.pool
    df_ibm_volume.offre= df_ibm_volume.offre.map(lambda x: x[5:])
    df_ibm_volume["origine"] = np.array("ibm")

    df_ibm_volume_reduit = df_ibm_volume[["ID","name","pool","offre","capacity","used_capacity",'date','origine']].copy()
    df_ibm_volume_reduit.reset_index(drop=True,inplace =True)

    df_ibm_volume_GB = df_ibm_volume_reduit.copy()
    for colonne in ["capacity", "used_capacity"]:
        df_ibm_volume_GB[colonne] = df_ibm_volume_reduit[colonne].map(lambda x : en_giga(x)).copy()

    # trouver le fichier server le plus recent
    basepath_ibm_server =path +'ibm/server/'
    fichiers_ibm_server=[]
    with os.scandir(basepath_ibm_server) as entries: 
        for entry in entries:
            if entry.is_file():
                fichiers_ibm_server.append(str(entry.name))
                
    date_max = datetime.datetime(1999,1,1)
    fichier_recent =''
    for i in fichiers_ibm_server:
        d = datetime.datetime.strptime(str(int(i[:8])),'%Y%m%d')
        if d > date_max:
            date_max = d
            fichier_recent = i

    basepath = path +'ibm/server/'
    df_ibm_server = pd.DataFrame()
    for name in [fichier_recent]:
        with open(os.path.join(basepath, name)) as f:
            df_temp = pd.read_csv(f, delim_whitespace =True, names =['ID', 'name_server',"SCSI_id" ,"vdisk_id" ,"vdisk_name" ,"vdisk_UID","IO_group_id","IO_group_name", "mapping_type","host_cluster_id","host_cluster_name"])
            df_temp['date']= datetime.datetime.strptime(str(int(name[:8])),'%Y%m%d')
            df_ibm_server = pd.concat([df_ibm_server, df_temp])

    df_ibm_server.reset_index(drop=True,inplace =True)
    # supprime les lignes où le name n'est pas bon.
    df_ibm_server = df_ibm_server.drop(df_ibm_server.name_server[df_ibm_server.name_server == "name"].index)
    df_ibm_server = df_ibm_server.drop(df_ibm_server.name_server[df_ibm_server.vdisk_name.isnull()].index)

    # création d'une colonne typo
    df_ibm_server['typologie'] = df_ibm_server.name_server
    df_ibm_server.typologie = df_ibm_server.typologie.map(lambda x : typo(x))

    #Création d'un df pour connaitre la typologie du volume et le nom de client(si possible) à partir d'un nom de server associé
    liste_volume = []
    liste_type = []
    liste_client =[]
    for volume in df_ibm_server.vdisk_name.unique():
        if volume in liste_volume:
            pass
        else:
            liste_volume.append(volume)
            liste_client.append(df_ibm_server[df_ibm_server.vdisk_name == volume].name_server.unique()[0][:3])
            liste_type.append(df_ibm_server[df_ibm_server.vdisk_name == volume].typologie.unique()[0])
    df_t = pd.DataFrame({"name_v":liste_volume,"client":liste_client,"type":liste_type})

    # Etiquetage en typologie et nom des clients des volumes ibm à partir du dataframe constitué précédemment df_t.
    # Si le serveur n'est reconnu, on tente de trouver la typo via son nom avec la fonction typo()
    df_ibm_volume_plus = df_ibm_volume_GB.copy()
    liste_typo =[]
    liste_client=[]

    for v in df_ibm_volume_plus.name:
        if v in df_t.name_v.unique().tolist(): # récupération de la typologie via le df df_t
            liste_typo.append(df_t[df_t.name_v == v].type.values[0])
            liste_client.append(df_t[df_t.name_v == v].client.values[0])
        else:
            if v[:3]=="INF":
                liste_typo.append(typo(v))   # appel de la fonction typo() pour trouver la typologie
                liste_client.append('INF')
            elif v[:3]=="SIG":
                liste_typo.append(typo(v))   
                liste_client.append('SIG')
            else :
                liste_typo.append(typo(v)) 
                liste_client.append('Client_non_enregistré')
    df_ibm_volume_plus['typologie'] = liste_typo
    df_ibm_volume_plus['client'] = liste_client

# 3par

basepath_3par_volume = path +'3par/volume'
# List all files in a directory using scandir()
liste_3par_volume=[]
with os.scandir(basepath_3par_volume) as entries: # renvoie un itérateur
    for entry in entries:
        if entry.is_file():
            if 'space' in entry.name:
                liste_3par_volume.append(str(entry.name))
            

fichiers_3par_a_recuperer = []
for i in liste_3par_volume:
    if i not in df_fichiers.nom_fichier.to_list():
        fichiers_3par_a_recuperer.append(i)

print(len(fichiers_3par_a_recuperer))
if len(fichiers_3par_a_recuperer)==0:
    print('pas de maj 3par')
    pass
else:
    # création du DF principal à partir de la liste_3par_volume
    df_3par_volume = pd.DataFrame()
    for name in fichiers_3par_a_recuperer:
        with open(os.path.join(basepath_3par_volume, name)) as f:
            # gestion des formats de fichiers avant / après le 08 janvier 2020
            if datetime.datetime.strptime(str(int(name[:8])),'%Y%m%d')<= datetime.datetime(2020,1,8):
                df_temp = pd.read_csv(f,delim_whitespace =True,  names= ["ID","name","Prov","Type","adm_Rsvd","adm_Used","snp_Rsvd","snp_Used","%_snp_Used","snp_Wrn","snp_Lim","usr_Rsvd" ,"usr_Used","%usr_Used", "usr_Wrn" ,"usr_Lim","Tot_Rsvd","VSize","Compaction","Dedup"], header = 4)
                df_temp['date']= datetime.datetime.strptime(str(int(name[:8])),'%Y%m%d')
                df_temp['pool']= name[9:19]
                df_temp = df_temp.rename(columns={"Tot_Rsvd": "Tot_used"})
            else:
                df_temp = pd.read_csv(f,delim_whitespace =True,  names= ["ID","name","Prov","Type","adm_Rsvd","adm_Used","snp_Rsvd","snp_Used","%_snp_Used","snp_Wrn","snp_Lim","usr_Rsvd" ,"usr_Used","%usr_Used", "usr_Wrn" ,"usr_Lim","Tot_Rsvd","Tot_used","HostWR","VSize","Compact","Compress"], header = 4)
                df_temp['date']= datetime.datetime.strptime(str(int(name[:8])),'%Y%m%d')
                df_temp['pool']= name[9:19]
            df_3par_volume = pd.concat([df_3par_volume, df_temp])
            
    # supprime les lignes où le name n'est pas bon.        
    df_3par_volume = df_3par_volume.drop(df_3par_volume.name[df_3par_volume.name.isnull()].index)
    df_3par_volume = df_3par_volume.drop(df_3par_volume.name[df_3par_volume.name=='total'].index)
    df_3par_volume = df_3par_volume[df_3par_volume.name !="No"]
    # renommage des colonnes
    df_3par_volume = df_3par_volume.rename(columns={"Tot_used": "used_capacity", "VSize":"capacity"})

    # limitation du nb de colonnes
    df_3par_volume = df_3par_volume[["ID","name","capacity","used_capacity","date","pool"]]
    df_3par_volume['offre'] = np.array('T2')
    #suppression des lignes où la used_capcity est null
    df_3par_volume = df_3par_volume[df_3par_volume.used_capacity.notnull()]

    # réinitialise les index
    df_3par_volume.reset_index(drop=True,inplace =True)

    # traitement sur la colonne date
    df_3par_volume.date = df_3par_volume.date.map(lambda x :  datetime.datetime.strptime(str(x)[:10],'%Y-%m-%d'))

    # ajout d'une colonne origine
    df_3par_volume['origine'] = np.array("3par")
    # ajout de colonne Client
    df_3par_volume['client'] = df_3par_volume.name
    df_3par_volume.client = df_3par_volume.client.map(lambda x : x[:3])

    # conversion en GB
    df_3par_volume_GB = df_3par_volume.copy()
    for colonne in ['capacity','used_capacity']:
        df_3par_volume_GB[colonne] = df_3par_volume_GB[colonne].map(lambda x :en_giga(x))

    # trouver le fichier server le plus recent
    basepath_3par_server = path+'3par/server/lun'
    fichiers_3par_server=[]
    with os.scandir(basepath_3par_server) as entries: # renvoie un itérateur
        for entry in entries:
            if entry.is_file():
                fichiers_3par_server.append(str(entry.name))
                
    date_max = datetime.datetime(1999,1,1) # date de test pour être sur que le programme l'écrase en rechreche le fichier le plus récent
    fichier_server_3par_recent =''
    for i in fichiers_3par_server:
        d = datetime.datetime.strptime(str(int(i[:8])),'%Y%m%d')
        if d > date_max:
            date_max = d
            ladate = str(date_max)[:10].replace('-','')
            fichier_server_3par_recent = [ladate + "_mutapsan05_showvlun-lvw.txt", ladate +"_mutapsan06_showvlun-lvw.txt"]

    # création du df Server volume avec un fichier server 3par (le plus récent ) 
    basepath_3par_server = path + '3par/server/lun/'
    df_3par_server = pd.DataFrame()
    for name in fichier_server_3par_recent:
        with open(os.path.join(basepath_3par_server, name)) as f:
            df_temp = pd.read_csv(f, delim_whitespace =True, names =['ID_volume', 'name_volume',"vv_wwn" ,"server" ,"Host_WWN/ISCSI" ,"port","Type","Status", "id"])
            df_temp['date']= name[:8]
            df_3par_server = pd.concat([df_3par_server, df_temp])
        
    # réinitialise les index
    df_3par_server.reset_index(drop=True,inplace =True)
    # supprime les lignes où le name n'est pas bon.
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server.name_volume == "VVName"].index)
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server.name_volume == "VLUNs"].index)
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server.ID_volume == "VLUN"].index)
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server.name_volume == "total"].index)
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server['Host_WWN/ISCSI'] == "----------------"].index)
    df_3par_server = df_3par_server.drop(df_3par_server.name_volume[df_3par_server.name_volume.isnull()].index)

    df_3par_server['typologie'] = df_3par_server.server
    df_3par_server.typologie = df_3par_server.typologie.map(lambda x : typo(x))

    # étiquetage de la typologie des volumes 3par
    df_3par_volume_plus = df_3par_volume_GB.copy()
    liste_typo =[]
    for v in df_3par_volume_plus.name:
        if v in df_3par_server.name_volume.unique().tolist():
            liste_typo.append(df_3par_server[df_3par_server.name_volume == v].typologie.values[0])
        else:
            liste_typo.append(typo(v))
            
    df_3par_volume_plus['typologie'] = liste_typo

# lecture en base des derniers id_enreg pour la réindexation du df_volume ci-dessus
query_read = "SELECT max(id_enreg) FROM stk_enregistrements"
cur.execute(query_read)
raw = cur.fetchall()
connection.commit()
x, = raw[0]
last_index_id_enreg= x

# union des df ibm et 3PAR
if (len(fichiers_ibm_a_recuperer)==0) & (len(fichiers_3par_a_recuperer))==0 :
    print("Pas de MAJ a faire")
    pass
else :
    print("MAJ en cours")
    if (len(fichiers_ibm_a_recuperer)!=0) & (len(fichiers_3par_a_recuperer))!=0 :
        df_volume = pd.concat([df_ibm_volume_plus,df_3par_volume_plus])
    elif (len(fichiers_ibm_a_recuperer)==0) & (len(fichiers_3par_a_recuperer))!=0 :
        df_volume = df_3par_volume_plus.copy()
    elif (len(fichiers_ibm_a_recuperer)!=0) & (len(fichiers_3par_a_recuperer))==0 :
        df_volume = df_ibm_volume_plus.copy()

    df_volume.date = df_volume.date.map(lambda x :  datetime.datetime.strptime(str(x)[:10],'%Y-%m-%d'))
    df_volume = df_volume.rename(columns ={'ID':'vdisk_id','name':'name_volume','offre':'name_offre', 'pool':'name_pool','typologie':'name_typologie'})
    new_index = np.arange(start=(last_index_id_enreg+1), stop=(last_index_id_enreg+1)+len(df_volume), step=1)
    df_volume.index = new_index
    df_volume = df_volume.reset_index()
    df_volume = df_volume.rename(columns={"index": "id_enreg"})

    # création des tables annexes (offres, pools, fichiers, typologies, origines)
    query_read_offres = "SELECT * FROM stk_offres"
    cur.execute(query_read_offres)
    raw_offres = cur.fetchall()
    offres = pd.DataFrame(raw_offres, columns =["id_offre","name_offre"])

    query_read_pools = "SELECT * FROM stk_pools"
    cur.execute(query_read_pools)
    raw_pools = cur.fetchall()
    pools = pd.DataFrame(raw_pools, columns =["id_pool","name_pool","id_offre"])

    query_read_typologie = "SELECT * FROM stk_typologies"
    cur.execute(query_read_typologie)
    raw_typologies = cur.fetchall()
    typologies = pd.DataFrame(raw_typologies, columns =["id_typologie","name_typologie"])

    query_read_origines = "SELECT * FROM stk_origines"
    cur.execute(query_read_origines)
    raw_origines = cur.fetchall()
    origines = pd.DataFrame(raw_origines, columns =["id_origine","origine"])

    df_volume = df_volume.merge(pools, on='name_pool')
    df_volume = df_volume.merge(typologies, on='name_typologie')
    df_volume = df_volume.merge(origines, on='origine')

    def insert_table(conn, df, table):
        """  Using psycopg2.extras.insert_table() to insert the dataframe  """
        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("insert_table() done")
        cursor.close

    # création d'un df contenant le nom des fichiers récupérés
    df_fichiers_stockage_ibm = pd.DataFrame({'nom_fichier':fichiers_ibm_a_recuperer,'origine':np.array('ibm')})
    df_fichiers_stockage_3par = pd.DataFrame({'nom_fichier':fichiers_3par_a_recuperer,'origine':np.array('3par')})
    df_fichiers_stockage = pd.concat([df_fichiers_stockage_ibm,df_fichiers_stockage_3par])
    df_fichiers_stockage = df_fichiers_stockage.merge(origines, on="origine")

    # écriture en base des nouvelles données et des fichiers récupérés
    insert_table(connection, df_fichiers_stockage[["nom_fichier","id_origine"]], 'stk_fichiers')
    insert_table(connection, df_volume[['id_enreg','name_volume','capacity','used_capacity', 'date', 'client','id_pool', 'id_typologie', 'id_origine']], 'stk_enregistrements')