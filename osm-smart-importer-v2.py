"""
Simple program using pyosmium to parse an OSM historical file,
and import it's content into a PostgreSQL database.

Simply modify the DB variables, and run using:
    python osm-importer.py <osmfile>

"""
import osmium as o
import sys
from datetime import date
import time
import psycopg2
# import pprint
import json

DB_NAME='osmmonaco'
DB_USER='Julien'
DB_PWD=''
DB_HOST='localhost'
DB_PORT='5433'

NODE_TYPE="Nodes"
WAY_TYPE="Ways"
RELATION_TYPE="Relations"

ONEE7 = 10000000

BOTTM_LEFT_BOUNDARY=[7.407896*ONEE7,43.724759*ONEE7]
TOP_RIGHT_BOUNDARY=[7.441014*ONEE7,43.752079*ONEE7]

nodes_discarded = 0
ways_discarded = 0
relations_discarded = 0

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
            tags jsonb NOT NULL,
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
            tags jsonb NOT NULL,
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
            tags jsonb NOT NULL,
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

        if self.datatype!=NODE_TYPE:
            return None

        # Discard nodes not in zone
        if (not checkBoundary(o.location.x,o.location.y)):
            increaseDiscardedNodes()
            return None
        query =  """INSERT INTO nodes VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}',
        {8},{9},'{10}');"""

        return query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""),o.location.x, o.location.y, o.jsontags)

    # Return am array of SQL commands to insert a way
    def insertWaySQL(self,o):

        if self.datatype!=WAY_TYPE:
            return

        query = """INSERT INTO ways VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""
        node_way_query = """ INSERT INTO ways_nodes VALUES ({0}, {1}, {2}, {3},{4},{5},{6}) """
        sequence_id=0

        queries =[]

        for mynode in o.nodes:

            node_query = """SELECT * from nodes where id = {0} and created_at<='{1}' order by created_at desc limit 1;"""
            current_node = self.executeSearchCommand(node_query.format(mynode.ref,o.timestamp))
            if current_node == None:
                node_query = """SELECT * from nodes where id = {0} order by created_at limit 1;"""
                current_node = self.executeSearchCommand(node_query.format(mynode.ref,o.timestamp))

            # filter out way if no o.nodes dans la zoe
            if current_node == None:
                continue

            queries.append( node_way_query.format(o.id,o.version,mynode.ref,current_node[len(current_node)-8],sequence_id,current_node[len(current_node)-3],current_node[len(current_node)-2]) )
            sequence_id += 1

        # If all nodes were out of our zone we don't add the way
        if len(queries) == 0:
            increaseDiscardedWays()
            return None

        queries.insert(0,query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags))

        return queries

    # Return am array of SQL commands to insert a relation
    def insertRelationSQL(self,o):

        if self.datatype!=RELATION_TYPE:
            return
        query = """INSERT INTO relations VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""
        node_way_query = """ INSERT INTO relations_members VALUES ({0}, {1}, {2}, '{3}', '{4}', {5}) """

        queries = []

        sequence_id=0
        for member in o.members:
            # Now to make sure that the zone is in the database, we want to make
            # sure that either the node or the way is already in db (as both nodes insertion
            # and ways insertion make sure that entity is in zone)
            if member.type.replace("'","") == 'n':
                #  If member is a node
                node_query = """SELECT * from nodes where id = {0} limit 1;"""
                current_node = self.executeSearchCommand(node_query.format(member.ref))
                if current_node == None:
                    continue

            elif (member.type.replace("'","")) == 'w':
                # if member is a way
                way_query = """SELECT * from ways where id = {0} limit 1;"""
                current_way = self.executeSearchCommand(way_query.format(member.ref))
                if current_way == None:
                    continue

            else:
                # if member is a relation
                relation_query = """SELECT * from relations where id = {0} limit 1;"""
                current_relation = self.executeSearchCommand(relation_query.format(member.ref))
                if current_relation == None:
                    continue

            queries.append( node_way_query.format(o.id,o.version,member.ref,member.type.replace("'",""),member.role.replace("'",""),sequence_id) )
            sequence_id += 1

        if len (queries) == 0:
            increaseDiscardedRelations()
            return None

        queries.insert(0,query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags))

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

def increaseDiscardedNodes():
    global nodes_discarded
    nodes_discarded+=1

def increaseDiscardedWays():
    global ways_discarded
    ways_discarded+=1


def increaseDiscardedRelations():
    global relations_discarded
    relations_discarded+=1

# Make sure given point is in defined zone
def checkBoundary(x,y):
    return (x>=BOTTM_LEFT_BOUNDARY[0] and x<=TOP_RIGHT_BOUNDARY[0] and
        y>=BOTTM_LEFT_BOUNDARY[1] and y <= TOP_RIGHT_BOUNDARY[1])


if __name__ == '__main__':
    white = '\033[0m'
    blue = '\033[94m'
    orange = '\033[93m'
    green = '\033[92m'

    print("\n=================================")
    print("======= "+blue+"OSM Data Importer "+white+"=======")
    print("=================================")

    starting_time = time.time()

    if len(sys.argv) != 2:
        print("Usage: python osm-importer.py <osmfile>")
        sys.exit(-1)

    print(orange+"\nWarning: All single quote ' are deleted in tags and users'name"+white)

    # Create connection with db and file importer
    print("\nConnecting to db... ")
    db = DB()
    print("OK")

    # Parse file and importing
    print("Parsing and importing nodes... ")
    n = FileHandler(db)
    n.apply_file(sys.argv[1])
    n.finish_remaining_commands()

    print("Parsing and importing ways... ")
    n.current_type = WAY_TYPE
    n.apply_file(sys.argv[1])
    n.finish_remaining_commands()

    print("Parsing and importing relations... ")
    n.current_type = RELATION_TYPE
    n.apply_file(sys.argv[1])
    n.finish_remaining_commands()

    print(green+"Import successful!"+white)
    print(time.time()-starting_time)

    print('nodes_discarded: '+str(nodes_discarded))
    print('ways_discarded: '+str(ways_discarded))
    print('relations_discarded: '+str(relations_discarded))
