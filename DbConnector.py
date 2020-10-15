from pymongo import MongoClient, version

from urllib.parse import quote

class DbConnector:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 DATABASE='Geolife',
                 HOST='tdt4225-20.idi.ntnu.no',
                 USER='PequeñaPerra',
                 PASSWORD=quote('@ZDL5Dd0vff29&VS^87Zhn1m3dOHFAUmn62!hwIEpBR0Eh2njCfW4')):
        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
