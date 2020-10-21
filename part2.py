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
    
    def task1(self):
        user_count = self.userCollection.count_documents({})
        activity_count = self.activityCollection.count_documents({})
        trackpoint_count = self.trackpointCollection.count_documents({})
        print("User count:", user_count, "| Activity count:", activity_count, "| Trackpoint count:", trackpoint_count)

    def task2(self):
        # Find the average number of activities per user
        user_count = self.userCollection.count_documents({})
        activity_count = self.activityCollection.count_documents({})
        avg = activity_count/user_count

        # TODO: do they want avg activity per unique user?

        # avg = self.activityCollection.aggregate([{"$group" : {"_id": "$user_id", "num_activities": {"$sum": 1}}}])
        # below does that in mongodb console 
        # db.Activity.aggregate([{$group: {_id: "$user_id", num_activities: {$sum: 1}}}])
        print("Average activities per users: " + str(avg))

    def task4(self):
        # finds unique user ids in activities where transporation_mode is 'taxi'
        for user_id in self.activityCollection.find({'transportation_mode': 'taxi'}, {'user_id': 1, '_id': 0}).distinct('user_id'):
            print(user_id)

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

        print("\nQuery 2:\n")
        program.task2()

        print("\nQuery 4:\n")
        # program.task4()

        print("\nQuery 5:\n")
        # program.task5()

    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()