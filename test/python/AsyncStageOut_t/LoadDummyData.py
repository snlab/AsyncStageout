"""
Insert dummy data into the AsyncTransfer CouchDB instance
"""
from AsyncStageOut import getHashLfn
import random
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Configuration import loadConfigurationFile
import datetime
import time
from random import shuffle

config = loadConfigurationFile('config/asyncstageout/config.py')
server = CouchServer(config.AsyncTransfer.couch_instance)
db = server.connectDatabase(config.AsyncTransfer.files_database)

users = ['jbalcas', 'ramiro', 'iosif', 'harvey']

sites = ['T2_US_CaltechFDT',
         'T2_US_UmichFDT']
#         'T3_CH_CernFDT1', 'T3_CH_CernFDT2']

dest_sitesL = sites
users_dest = {'jbalcas': 'T3_CH_CernFDT1', 'ramiro': 'T2_US_CaltechFDT', 'iosif': 'T2_CH_CernFDT2', 'harvey':'T2_US_UmichFDT'}

size = 200 #TODO: readize = 2000 #TODO: read from script input
i = 100
 
# lfn_base has store/temp in it twice to make sure that
# the temp->permananet lfn change is correct.
lfn_base = '/store/temp/user/%s/my_cool_dataset%s_%s/file-%s-%s.root'
dest_lfn = '/store/user/%s/my_cool_dataset%s_%s/file-%s-%s.root'

now = str(datetime.datetime.now())
job_end_time =datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
last_update = int(time.time());

print "Script starts at %s" %now

q = []
for sourceSite in sites:
    for destSite in sites:
        for i in range(1,21):
            temp = []
            temp.append(sourceSite)
            temp.append(destSite)
            temp.append(i)
            temp.append(users[random.randint(0,int(len(users)-1))])
            q.append(temp)
        
joint_transfers = q

sizesOfSource = {"T2_US_UmichFDT" : 20*1024*1024*1024,
                 "T2_US_CaltechFDT": 20*1024*1024*1024,
                 "T3_CH_CernFDT2" : int(0.1*1024*1024*1024),
                 "T3_CH_CernFDT1" : int(0.1*1024*1024*1024)}

TODEVNULL = True # This means destination lfn will be /store/user/USERNAME/null.
# And it has to have a symlink to dev null
if TODEVNULL:
    dest_lfn = "/store/user/%s/null/"

shuffle(joint_transfers)

a = 0
for item in joint_transfers:
    print item
    a = item[2]
    dest = item[0]
    source_site = item[1]
    user = 'jbalcas' # item[3]
    source_size = sizesOfSource[source_site]
    if source_site == dest:
        continue
    i = 100
    print 'LOOP'
    while i <= size:
        id=0
        # We don`t want to make transfers from same site to same
        if source_site == dest:
            print 'Same. Ignore'
            continue
        #ha = lfn_base + random.random
        #_id=getHashLfn(lfn_base % (user,id , i))
        state='new'
        DDD = None
        if not TODEVNULL:
            DDD = dest_lfn % (user,a, source_site, id, i)
        else:
            DDD = dest_lfn % user
        _id = getHashLfn(lfn_base % (user, a, source_site, id, i))
        file_doc = {'_id': '%s' % _id,
                'lfn': lfn_base % (user,a, source_site, id, i),
                'destination_lfn' :DDD,
                'source_lfn' : lfn_base % (user,a, source_site, id, i),
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
                'destination': dest,
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
