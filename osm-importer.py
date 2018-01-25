"""
Simple program using pyosmium to parse an OSM historical file,
and import it's content into a PostgreSQL database.

Simply modify the DB variables, and run using:
    python osm-importer.py <osmfile>

"""
import osmium as o
import sys
from datetime import date
from progress.bar import Bar
import psycopg2
import pprint
import json

DB_NAME=''
DB_USER=''
DB_PWD=''
DB_HOST='localhost'
DB_PORT='5432'

NODE_TYPE="Nodes"
WAY_TYPE="Ways"
RELATION_TYPE="Relations"

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
            sequence_id BIGINT NOT NULL,
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

    def execute(self,commands=[],total=0):

        if total != 0:
            bar = Bar('Processing', max=total, suffix='%(percent)d%% - %(elapsed)ds')

        try:
            cur = self.connection.cursor()
            # create table one by one
            for command in commands:
                cur.execute(command)
                if total != 0 and "relations_members" not in command and "ways_nodes" not in command:
                    bar.next()
            # close communication with the PostgreSQL database server
            cur.close()
            if total != 0:
                bar.finish()
            # commit the changes
            self.connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print('\033[91m'+"\nSQL ERROR:\n"+str(error)+'\033[0m')
            sys.exit(-1)


class Importer(object):

    def __init__(self,datatype, db):
        self.added = 0
        self.modified = 0
        self.deleted = 0
        self.db = db

        self.datatype=datatype
        self.insertion_commands=[]

    # Deal with one entity (node, way or relation)
    def add(self, o):
        if o.deleted:
            self.deleted += 1
        elif o.version == 1:
            self.added += 1
        else:
            self.modified += 1

        # We jsonify tags
        o.jsontags = self.jsonifyTags(o.tags)

        if self.datatype==NODE_TYPE:
            self.insertion_commands.append(self.insertNodeSQL(o))
        elif self.datatype==WAY_TYPE:
            self.insertion_commands += self.insertWaySQL(o)
        elif self.datatype==RELATION_TYPE:
            self.insertion_commands += self.insertRelationSQL(o)
        else:
            print('\033[91m'+"\nERROR: type"+str( self.datatype)+" not found, or not handled."+'\033[0m')
            sys.exit(-1)

    def jsonifyTags(self,tags):

        jsontags={}
        for tag in tags:
            jsontags[tag.k.replace("'","")] = tag.v.replace("'","")

        return json.dumps(jsontags)

    def executeImport(self):
        self.db.execute(self.insertion_commands,self.added+self.modified+self.deleted)

    # Return a SQL command to insert a node
    def insertNodeSQL(self,o):
        if self.datatype!=NODE_TYPE:
            return

        query =  """INSERT INTO nodes VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}',
        {8},{9},'{10}');"""

        return query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""),o.location.x, o.location.y, o.jsontags)

    # Return am array of SQL commands to insert a way
    def insertWaySQL(self,o):
        if self.datatype!=WAY_TYPE:
            return
        query = """INSERT INTO ways VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""

        queries = [query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags)]

        node_way_query = """ INSERT INTO ways_nodes VALUES ({0}, {1}, {2}, {3}) """
        sequence_id=0
        for node in o.nodes:
            queries.append( node_way_query.format(o.id,o.version,node.ref,sequence_id) )
            sequence_id += 1

        return queries

    # Return am array of SQL commands to insert a relation
    def insertRelationSQL(self,o):
        if self.datatype!=RELATION_TYPE:
            return
        query = """INSERT INTO relations VALUES ({0}, {1}, {2} , {3}, {4}, {5}, '{6}','{7}','{8}');"""

        queries = [query.format(o.id,o.deleted,o.visible,o.version,o.changeset,o.uid,o.timestamp,o.user.replace("'",""), o.jsontags)]

        node_way_query = """ INSERT INTO relations_members VALUES ({0}, {1}, {2}, '{3}', '{4}', {5}) """
        sequence_id=0
        for member in o.members:
            queries.append( node_way_query.format(o.id,o.version,member.ref,member.type.replace("'",""),member.role.replace("'",""),sequence_id) )
            sequence_id += 1

        return queries

    # Print stats of inserted items
    def outstats(self):
        print("%s added: %d" % (self.datatype, self.added))
        print("%s modified: %d" % (self.datatype, self.modified))
        print("%s deleted: %d" % (self.datatype, self.deleted))

class FileStatsHandler(o.SimpleHandler):
    def __init__(self, db):
        super(FileStatsHandler, self).__init__()
        self.nodes = Importer(NODE_TYPE,db)
        self.ways = Importer(WAY_TYPE,db)
        self.rels = Importer(RELATION_TYPE,db)

    def node(self, n):
        self.nodes.add(n)

    def way(self, w):
	    self.ways.add(w)

    def relation(self, r):
	    self.rels.add(r)


if __name__ == '__main__':
    white = '\033[0m'
    blue = '\033[94m'
    orange = '\033[93m'
    green = '\033[92m'

    print("\n=================================")
    print("======= "+blue+"OSM Data Importer "+white+"=======")
    print("=================================")


    if len(sys.argv) != 2:
        print("Usage: python osm-importer.py <osmfile>")
        sys.exit(-1)

    print(orange+"\nWarning: All single quote ' are deleted in tags and users'name"+white)

    # Create connection with db and file importer
    print("\nConnecting to db... ",end='')
    db = DB()
    print("OK")

    # Parse file
    print("Parsing file... ",end='')
    h = FileStatsHandler(db)
    h.apply_file(sys.argv[1])
    print("OK")

    print("\nData found:")

    h.nodes.outstats()
    h.ways.outstats()
    h.rels.outstats()

    print("Starting nodes import...")
    h.nodes.executeImport()
    print("Starting ways import...")
    h.ways.executeImport()
    print("Starting relations import...")
    h.rels.executeImport()
    print("OK")

    print(green+"Import successful!"+white)
