"""
Simple program using pyosmium to parse an OSM historical file,
and import it's content into a PostgreSQL database.

Simply modify the DB variables, and run using:
    python osm-importer.py <osmfile>

"""
import osmium as o
import sys
import os
from datetime import date
import time
import psycopg2
# import pprint
import json

from queue import Queue
from threading import Thread, Lock
from multiprocessing import Process,Manager

DB_NAME='osmmonaco'
DB_USER='Julien'
DB_PWD=''
DB_HOST='localhost'
DB_PORT='5433'

NODE_TYPE="Nodes"
WAY_TYPE="Ways"
RELATION_TYPE="Relations"

BOTTOM_LEFT_BOUNDARY=[0,0]
TOP_RIGHT_BOUNDARY=[0,0]

nodes_discarded = 0
ways_discarded = 0
relations_discarded = 0

nodes_added = 0
ways_added = 0
relations_added = 0

actionsLogged = 0
lastActionLogged = 0

class DB(object):
    """encaspulate a database connection."""

    def __init__(self):
        try:
            self.connection = psycopg2.connect("dbname='"+DB_NAME+"' user='"+DB_USER+"' password='"+DB_PWD+"' host='"+DB_HOST+"' port='"+DB_PORT+"'")
        except:
            print('\033[91m'+"Unable to connect to the database."+'\033[0m')
            sys.exit(-1)

        self.createTables()


    def createTables(self):
        commands = [
        """
        CREATE TABLE IF NOT EXISTS nodes (
            id BIGINT NOT NULL,
            deleted BOOLEAN NOT NULL,
            visible BOOLEAN NOT NULL,
            version BIGINT NOT NULL,
            changeset BIGINT NOT NULL,
            uniqueid BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            user_name VARCHAR(255) NOT NULL,
            latitude INT NOT NULL,
            longitude INT NOT NULL,
            tags json NOT NULL,
            PRIMARY KEY (id, version)
        )
        """,
        """CREATE TABLE IF NOT EXISTS ways (
            id BIGINT NOT NULL,
            deleted BOOLEAN NOT NULL,
            visible BOOLEAN NOT NULL,
            version BIGINT NOT NULL,
            changeset BIGINT NOT NULL,
            uniqueid BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            user_name VARCHAR(255) NOT NULL,
            tags json NOT NULL,
            PRIMARY KEY (id, version)
        )
        """,
        """CREATE TABLE IF NOT EXISTS ways_nodes (
            id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            node_id BIGINT NOT NULL,
            node_version BIGINT NOT NULL,
            sequence_id BIGINT NOT NULL,
            latitude INT NOT NULL,
            longitude INT NOT NULL,
            foreign key (id,version) references ways(id,version),
            PRIMARY KEY (id,version,sequence_id)
        )
        """,
        """CREATE TABLE IF NOT EXISTS relations (
            id BIGINT NOT NULL,
            deleted BOOLEAN NOT NULL,
            visible BOOLEAN NOT NULL,
            version BIGINT NOT NULL,
            changeset BIGINT NOT NULL,
            uniqueid BIGINT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            user_name VARCHAR(255) NOT NULL,
            tags json NOT NULL,
            PRIMARY KEY (id, version)
        )
        """,
        """CREATE TABLE IF NOT EXISTS relations_members (
            id BIGINT NOT NULL,
            version BIGINT NOT NULL,
            member_id BIGINT NOT NULL,
            member_type CHAR(1) NOT NULL,
            member_role VARCHAR(255),
            sequence_id BIGINT NOT NULL,
            foreign key (id,version) references relations(id,version),
            PRIMARY KEY (id,version,sequence_id)
        )
        """,]

        self.execute(commands)

    def execute(self,commands=[]):
        try:
            cur = self.connection.cursor()
            # create table one by one
            for command in commands:
                cur.execute(command)

            # close communication with the PostgreSQL database server
            cur.close()

            # commit the changes
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('\033[91m'+"\nSQL ERROR:\n"+str(error)+'\033[0m')
            sys.exit(-1)

    def executeAndReturn(self,command):
        try:
            cur = self.connection.cursor()
            # create table one by one
            cur.execute(command)

            result = cur.fetchone()
            # close communication with the PostgreSQL database server
            cur.close()

            # commit the changes
            self.connection.commit()

            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print('\033[91m'+"\nSQL ERROR:\n"+str(error)+'\033[0m')
            sys.exit(-1)

class WayNodeChecker(Thread):
    # Appending item to list is a thread-safe operation
   def __init__(self, queue, queries, seq_id,db,way):
       Thread.__init__(self)
       self.queue = queue
       self.queries = queries
       self.seq_id = seq_id
       self.db = db
       self.way = way

   def run(self):
       node_way_query = """ INSERT INTO ways_nodes VALUES ({0}, {1}, {2}, {3},{4},{5},{6}) """

       while True:
           # Get the work from the queue
           item = self.queue.get()
           logAction("Testing node for a way, node_id: "+str(item.ref))

           node_query = """SELECT * from nodes where id = {0} and created_at<='{1}' order by created_at desc limit 1;"""
           current_node = self.executeSearchCommand(node_query.format(item.ref,self.way.timestamp))
           if current_node == None:
               node_query = """SELECT * from nodes where id = {0} order by created_at limit 1;"""
               current_node = self.executeSearchCommand(node_query.format(item.ref,self.way.timestamp))

           # filter out way if no o.nodes dans la zoe
           if current_node == None:
               logAction("Discarding a node from a way, node_id: "+str(item.ref))
               self.queue.task_done()
               continue

           logAction("Adding node to a way, node_id: "+str(item.ref))
           self.queries.append( node_way_query.format(self.way.id,self.way.version,item.ref,current_node[len(current_node)-8],self.seq_id.getValue(),current_node[len(current_node)-3],current_node[len(current_node)-2]) )
           self.seq_id.increment()
           self.queue.task_done()

   def executeSearchCommand(self,command):
         return self.db.executeAndReturn(command)

class RelationMemberChecker(Thread):
    # Appending item to list is a thread-safe operation
   def __init__(self, queue, queries, seq_id, db, relation):
       Thread.__init__(self)
       self.queue = queue
       self.queries = queries
       self.seq_id = seq_id
       self.db = db
       self.relation = relation

   def executeSearchCommand(self,command):
       return self.db.executeAndReturn(command)

   def run(self):
       node_way_query = """ INSERT INTO relations_members VALUES ({0}, {1}, {2}, '{3}', '{4}', {5}) """
       while True:
           # Get the work from the queue
           item = self.queue.get()

           # Now to make sure that the zone is in the database, we want to make
           # sure that either the node or the way is already in db (as both nodes insertion
           # and ways insertion make sure that entity is in zone)
           if item.type.replace("'","") == 'n':
               #  If item is a node
               node_query = """SELECT * from nodes where id = {0} limit 1;"""
               current_node = self.executeSearchCommand(node_query.format(item.ref))
               if current_node == None:
                   logAction("Discarding a node from a relation, id: "+str(item.ref))
                   self.queue.task_done()
                   continue

           elif (item.type.replace("'","")) == 'w':
               # if item is a way
               way_query = """SELECT * from ways where id = {0} limit 1;"""
               current_way = self.executeSearchCommand(way_query.format(item.ref))
               if current_way == None:
                   logAction("Discarding a way from a relation, id: "+str(item.ref))
                   self.queue.task_done()
                   continue

           else:
               # if item is a relation
               relation_query = """SELECT * from relations where id = {0} limit 1;"""
               current_relation = self.executeSearchCommand(relation_query.format(item.ref))
               if current_relation == None:
                   logAction("Discarding a relation from a relation, id: "+str(item.ref))
                   self.queue.task_done()
                   continue

           logAction("Adding an item of type"+ item.type.replace("'","") +" to a relation, id: "+str(item.ref))
           self.queries.append( node_way_query.format(self.relation.id,self.relation.version,item.ref,item.type.replace("'",""),item.role.replace("'",""),self.seq_id.getValue()) )
           self.seq_id.increment()
           self.queue.task_done()


# ======= Counter for seq id ==========
class Counter(object):
    def __init__(self, start=0):
        self.lock = Lock()
        self.value = start

    def increment(self):
        with self.lock:
            self.value = self.value + 1

    def getValue(self):
        with self.lock:
            return self.value

#  ============ Process starting threads =========

def processDealWithWay(way,db,queries):
    # Prepare for concurrency
    queue = Queue()
    sequence_id = Counter()

    tempQueries = []

    # Start 20 workers
    for x in range(25):
        worker = WayNodeChecker(queue,tempQueries,sequence_id,db, way)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    for mynode in way.nodes:
        logAction("Checking node for way: "+str(mynode.ref))
        queue.put(mynode)

    # Wait for workers to be done with analyzing all items in queue
    queue.join()
    queries += tempQueries


def processDealWithRelation(relation,db,queries):
    # Prepare for concurrency
    queue = Queue()
    sequence_id = Counter()

    tempQueries = []

    # Start 20 workers
    for x in range(25):
        worker = RelationMemberChecker(queue,tempQueries,sequence_id,db, relation)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    for member in relation.members:
        logAction("Checking member for relation: "+str(member.ref))
        queue.put(member)

    # Wait for workers to be done with analyzing all items in queue
    queue.join()
    queries += tempQueries

# ============= Importer class ==============

class Importer(object):

    def __init__(self,datatype, db):
        self.db = db
        self.datatype=datatype
        self.insertion_commands=[]

    # Deal with one entity (node, way or relation)
    def add(self, o):

        # We jsonify tags
        o.jsontags = self.jsonifyTags(o.tags)

        if self.datatype==NODE_TYPE:
            query = self.insertNodeSQL(o)
            if query!= None:
                self.insertion_commands.append(query)
        elif self.datatype==WAY_TYPE:
            query = self.insertWaySQL(o)
            if query!= None:
                self.insertion_commands += query
        elif self.datatype==RELATION_TYPE:
            query = self.insertRelationSQL(o)
            if query != None:
                self.insertion_commands += query
        else:
            print('\033[91m'+"\nERROR: type"+str( self.datatype)+" not found, or not handled."+'\033[0m')
            sys.exit(-1)

        # Execute commands every 100000
        if (len(self.insertion_commands)>100000):
            self.executeCommands()

    def executeCommands(self):
        self.db.execute( self.insertion_commands )
        self.insertion_commands = []

    def executeSearchCommand(self,command):
        return self.db.executeAndReturn(command)

    def jsonifyTags(self,tags):
        jsontags={}
        for tag in tags:
            jsontags[tag.k.replace("'","")] = tag.v.replace("'","")

        return json.dumps(jsontags)

    # Return a SQL command to insert a node
    def insertNodeSQL(self,o):

        global nodes_added, nodes_discarded

        if self.datatype!=NODE_TYPE:
            return None

        # Discard nodes not in zone
        if (not checkBoundary(o.location.x,o.location.y)):
            nodes_discarded+=1
            logAction("Discarding node: "+str(o.location.x)+" "+str(o.location.y))
            return None
        query =  """INSERT INTO nodes VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}',
        {8},{9},'{10}');"""

        logAction("Adding a node")
        nodes_added +=1
        return query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""),o.location.x, o.location.y, o.jsontags)

    # Return am array of SQL commands to insert a way
    def insertWaySQL(self,o):

        global ways_added, ways_discarded

        if self.datatype!=WAY_TYPE:
            return
        query = """INSERT INTO ways VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""

        queries = Manager().list()
        p = Process(target=processDealWithWay, args=(o,db,queries))
        p.start()
        p.join()

        # If all nodes were out of our zone we don't add the way
        if len(queries) == 0:
            logAction("Discarding a way, id: "+str(o.id))
            ways_discarded +=1
            return None

        logAction("Adding a way, id: "+str(o.id))
        queries.insert(0,query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags))

        ways_added+=1
        return queries

    # Return am array of SQL commands to insert a relation
    def insertRelationSQL(self,o):

        global relations_added, relations_discarded

        if self.datatype!=RELATION_TYPE:
            return


        query = """INSERT INTO relations VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""

        queries = Manager().list()
        p = Process(target=processDealWithRelation, args=(o,db,queries))
        p.start()
        p.join()

        if len (queries) == 0:
            logAction("Discarding a relation, id: "+str(o.id))
            relations_discarded +=1
            return None

        logAction("Adding a relation, id: "+str(o.id))
        queries.insert(0,query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags))

        relations_added+=1
        return queries

class FileHandler(o.SimpleHandler):
    def __init__(self, db):
        super(FileHandler, self).__init__()
        self.nodes = Importer(NODE_TYPE,db)
        self.ways = Importer(WAY_TYPE,db)
        self.rels = Importer(RELATION_TYPE,db)
        # We start with node
        self.current_type=NODE_TYPE

    def node(self, n):
        if(self.current_type == NODE_TYPE):
            self.nodes.add(n)

    def way(self, w):
        if(self.current_type == WAY_TYPE):
	        self.ways.add(w)

    def relation(self, r):
        if self.current_type == RELATION_TYPE:
	        self.rels.add(r)

    def finish_remaining_commands(self):
        self.nodes.executeCommands()
        self.ways.executeCommands()
        self.rels.executeCommands()

# Make sure given point is in defined zone
def checkBoundary(x,y):
    return (x>=BOTTOM_LEFT_BOUNDARY[1] and x<=TOP_RIGHT_BOUNDARY[1] and
        y>=BOTTOM_LEFT_BOUNDARY[0] and y <= TOP_RIGHT_BOUNDARY[0])

def logAction(action):
    global actionsLogged, nodes_added,nodes_discarded, ways_added, ways_discarded, relations_added, relations_discarded,lastActionLogged

    if lastActionLogged==0:
        lastActionLogged = time.time()

    actionsLogged += 1

    if actionsLogged % 1000 == 0 or time.time() - lastActionLogged > 60:
        file = open("logs/"+DB_NAME+"-log.txt","a")
        file.write('\n'+action)
        file.write("\nNodes added: "+str(nodes_added)+"\nNodes discarded: "+str(nodes_discarded)+
         "\nWays added: "+str(ways_added)+ "\nWays discarded: " + str(ways_discarded) +
         "\nRelations added: "+str(relations_added) + "\nRelations discarded: "+ str(relations_discarded)+"\n")
        file.close()
        lastActionLogged = time.time()


if __name__ == '__main__':

    white = '\033[0m'
    blue = '\033[94m'
    orange = '\033[93m'
    green = '\033[92m'

    print("\n=================================")
    print("======= "+blue+"OSM Data Importer "+white+"=======")
    print("=================================")

    starting_time = time.time()

    if len(sys.argv) < 2:
        print("Usage: python osm-importer.py <osmfile>")
        sys.exit(-1)

    print(orange+"\nWarning: All single quote ' are deleted in tags and users'name"+white)

    # Create connection with db
    if (not sys.argv[2] ):
        if sys.version_info[0] < 3:
            DB_NAME = raw_input("Please enter dbname:")
        else:
            DB_NAME = input("Please enter dbname:")
    else:
        DB_NAME = sys.argv[2]

    print("\nConnecting to db... ")
    db = DB()
    print("OK")

    #  Set up zone limit
    print("Setting up the zone limit...")
    if len(sys.argv) < 6:
        if sys.version_info[0] < 3:
            BOTTOM_LEFT_BOUNDARY[0] = int(raw_input("Please enter bottom left boundary x:"))
            BOTTOM_LEFT_BOUNDARY[1] = int(raw_input("Please enter bottom left boundary y:"))
            TOP_RIGHT_BOUNDARY[0] = int(raw_input("Please enter top right boundary x:"))
            TOP_RIGHT_BOUNDARY[1] = int(raw_input("Please enter top right boundary y:"))
        else:
            BOTTOM_LEFT_BOUNDARY[0] = int(input("Please enter bottom left boundary x:"))
            BOTTOM_LEFT_BOUNDARY[1] = int(input("Please enter bottom left boundary y:"))
            TOP_RIGHT_BOUNDARY[0] = int(input("Please enter top right boundary x:"))
            TOP_RIGHT_BOUNDARY[1] = int(input("Please enter top right boundary y:"))
    else:
        BOTTOM_LEFT_BOUNDARY[0] = int(sys.argv[3])
        BOTTOM_LEFT_BOUNDARY[1] = int(sys.argv[4])
        TOP_RIGHT_BOUNDARY[0] = int(sys.argv[5])
        TOP_RIGHT_BOUNDARY[1] = int(sys.argv[6])
    print("OK")

    print("Output will be in : logs/"+DB_NAME+"-log.txt")

    file = open("logs/"+DB_NAME+"-log.txt","w")

    # Parse file and importing
    file.write("\n\n------------------------------\nParsing and importing nodes...")
    file.write("\nTime elapsed: "+str(time.time()-starting_time))
    file.close()
    print("Parsing and importing nodes...")
    print("Time elapsed: "+str(time.time()-starting_time))
    n = FileHandler(db)
    n.apply_file(sys.argv[1])
    n.finish_remaining_commands()

    file = open("logs/"+DB_NAME+"-log.txt","a")
    file.write("\n\n------------------------------\nParsing and importing ways...")
    file.write("\nTime elapsed: "+str(time.time()-starting_time))
    file.close()
    print("Parsing and importing ways...")
    print("Time elapsed: "+str(time.time()-starting_time))
    n.current_type = WAY_TYPE
    n.apply_file(sys.argv[1])
    n.finish_remaining_commands()

    # file = open("logs/"+DB_NAME+"-log.txt","a")
    # file.write("\n\n------------------------------\nParsing and importing relations... ")
    # file.write("\nTime elapsed: "+str(time.time()-starting_time))
    # file.close()
    # print("Parsing and importing relations..")
    # print("Time elapsed: "+str(time.time()-starting_time))
    # n.current_type = RELATION_TYPE
    # n.apply_file(sys.argv[1])
    # n.finish_remaining_commands()
    print("Skipping relations imports")

    # Print report to output
    print(green+"Import successful!"+white)
    print("Time elapsed: "+str(time.time()-starting_time))

    print('nodes_discarded: '+str(nodes_discarded))
    print('ways_discarded: '+str(ways_discarded))
    print('relations_discarded: '+str(relations_discarded))

    # Print output tp file
    file = open("logs/"+DB_NAME+"-log.txt","a")
    file.write("\n\n------------------------------\nImport successful!")
    file.write("Time elapsed: "+str(time.time()-starting_time))

    file.write('nodes_discarded: '+str(nodes_discarded))
    file.write('ways_discarded: '+str(ways_discarded))
    file.write('relations_discarded: '+str(relations_discarded))

    file.close()
