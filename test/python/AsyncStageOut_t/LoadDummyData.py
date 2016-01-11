"""
Insert dummy data into the AsyncTransfer CouchDB instance
"""
from AsyncStageOut import getHashLfn
import random
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile
import datetime
import time
import sys
from random import shuffle

config = loadConfigurationFile('config/asyncstageout/config.py')
server = CouchServer(config.AsyncTransfer.couch_instance)
db = server.connectDatabase(config.AsyncTransfer.files_database)

#users = ['jbalcas', 'ramiro', 'iosif', 'harvey']

#sites = ['T2_US_CaltechFDT',
#         'T2_US_UmichFDT']
#         'T3_CH_CernFDT1', 'T3_CH_CernFDT2']

#sites = ['T2_']

#dest_sitesL = sites

size = 200 #TODO: readize = 2000 #TODO: read from script input
i = 100
 
# lfn_base has store/temp in it twice to make sure that
# the temp->permananet lfn change is correct.

#lfn_base_site = {'T2_US_FIU': '/store/temp/user/%s/zero/',
#                 'T2_US_SC01': ['/store%s/temp/user/%s/my_cool_dataset%s_%s/file-%s-%s.root', 3],
#                 'T2_US_SC02': ['/store%s/temp/user/%s/my_cool_dataset%s_%s/file-%s-%    s.root', 3],
#                 'T2_US_SC03': ['/store%s/temp/user/%s/my_cool_dataset%s_%s/file-%s-%        s.root', 3]}
#
#dest_lfn_site = {}

lfn_base = '/store/temp/user/%(user)s/my_cool_dataset%(source)s_%(dest)s/file-%(id)s-%(i)s.root'
dest_lfn = '/store/user/%(user)s/my_cool_dataset%(source)s_%(dest)s/file-%(id)s-%(i)s.root'

now = str(datetime.datetime.now())
job_end_time =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
last_update = int(time.time());

print "Script starts at %s" %now

q = []
#for sourceSite in sites:
#    for destSite in sites:
#        for i in range(1,21):
#            temp = []
#            temp.append(sourceSite)
#            temp.append(destSite)
#            temp.append(i)
#            temp.append(users[random.randint(0,int(len(users)-1))])
#            q.append(temp)
        
#joint_transfers = q
#
#sizesOfSource = {"T2_US_UmichFDT" : 20*1024*1024*1024,
#                 "T2_US_CaltechFDT": 20*1024*1024*1024,
#                 "T3_CH_CernFDT2" : int(0.1*1024*1024*1024),
#                 "T3_CH_CernFDT1" : int(0.1*1024*1024*1024)}

#TODEVNULL = True # This means destination lfn will be /store/user/USERNAME/null.
# And it has to have a symlink to dev null
#if TODEVNULL:
#    dest_lfn = "/store/user/%s/null/"

#shuffle(joint_transfers)

source_site = sys.argv[1]
dest_site = sys.argv[2]
source_lfn = lfn_base
dest_lfn = dest_lfn
source_size = sys.argv[5]

user = 'jbalcas'
dict_keys = {'user': user, 'source': source_site, 'dest': dest_site, 'id': 0, 'i': 0}
cont = True
i = 0
while cont:
    while i <= size:
        state='new'
        dict_keys['i'] = i
        _id = getHashLfn(source_lfn % dict_keys)
        file_doc = {'_id': '%s' % _id,
                'lfn': lfn_base % dict_keys,
                'destination_lfn' :dest_lfn % dict_keys,
                'source_lfn' : lfn_base % dict_keys,
                'end_time' : '',
                'dn': 'UserDN',
                #'_attachments': '',
                'checksums': { "adler32": "ad:b2378cab"},
                "failure_reason": [ ],
                'group': '',
                'publish':1,
                'timestamp': now,
                'source':  source_site,
                'role': '',
                'type': 'output',
                'dbSource_url': 'WMS',
                'dbs_url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                'inputdataset': 'NULL',
                'publication_state': 'not_published',
                'publication_retry_count': [],
                'group': '',
                'jobid': '%s' %(random.randint(1,1000)),
                'destination': dest_site,
                'start_time' : now,
                'state' : state,
                'last_update': last_update,
                'publish_dbs_url': 'https://cmsweb.cern.ch/dbs/prod/phys03/DBSWriter',
                'workflow': 'CmsRunAnalysis-%s' %(random.randint(1,500)),
                'retry_count': [],
                'user': user,
                'size': source_size,
                'job_end_time': job_end_time,
                'job_retry_count': 0,
                'rest_host': 'balcas-crab2.cern.ch',
                'rest_uri' : '/crabserver/dev'
        }
        print "uploading docs in %s and db %s" %(config.AsyncTransfer.couch_instance, config.AsyncTransfer.files_database)
        try:
            db.queue(file_doc)
        except Exception, ex:
            print "Error when queuing docs"
            print ex
        print "doc queued %s" %file_doc
        # TODO: Bulk commit of documents
        try:
            db.commit()
            print "commiting %s doc at %s" %( i, str(datetime.datetime.now()))
        except Exception, ex:
            print "Error when commiting docs"
            print ex
        i += 1
    cont = False
