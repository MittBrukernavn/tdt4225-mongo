from DbConnector import DbConnector
from bson.code import Code # needed for aggregation
from pprint import pprint

class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.userCollection = self.db['User']
        self.activityCollection = self.db['Activity']
        self.trackpointCollection = self.db['Trackpoint']
    
    # based on function from example.py
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        return documents
    
    def task1(self):
        user_count = self.userCollection.count_documents({})
        activity_count = self.activityCollection.count_documents({})
        trackpoint_count = self.trackpointCollection.count_documents({})
        print("User count:", user_count, "| Activity count:", activity_count, "| Trackpoint count:", trackpoint_count)

    def task5(self):
        # map and reduce, see https://api.mongodb.com/python/current/examples/aggregation.html
        
        # emits key value-pairing for transportation modes that are not null
        mapper = Code(""" 
            function () {
                if(this.transportation_mode !== null) {
                    emit(this.transportation_mode, 1);
                }
            } """)
        # sums emitted values for each key
        reducer = Code(""" 
            function (key, val) {
                let total = 0;
                for(let i = 0; i < val.length; i++) {
                    total += val[i];
                }
                return total;
            } """)

        transportation_modes = self.activityCollection.map_reduce(mapper, reducer, "results")
        for document in transportation_modes.find().sort("value", -1):
            pprint(document)

def main():
    try:
        program = Part2()
        print("Part 2: Queries \n")

        print("\nQuery 1:\n")
        program.task1()

        print("\nQuery 5:\n")
        program.task5()

    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()