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

    def task3(self):
        # Find the top 20 users with the highest number of activities.
        mapper = Code(""" 
            function() {
                emit(this.user_id, 1);
            }; """)
        reducer = Code(""" 
            function (key, val) {
                let total = 0;
                for(let i = 0; i < val.length; i++) {
                    total += val[i];
                }
                return total;
            } """)

        activity_count = self.activityCollection.map_reduce(mapper, reducer, "activity_count_results")
        for document in activity_count.find().sort("value", -1).limit(20):
            pprint(document)

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

    def task6a(self):
        # 6. a) Find the year with the most activities.
        query = list(self.trackpointCollection.aggregate([
            { '$project':
                { 
                    # '_id' : 0, # does not exclude _id from result?
                    'year' :
                    { '$year' : '$datetime' },
                }
            },
            { '$group' :
                { '_id' : 
                    { 'year' : '$year' },
                    'count' : 
                        { '$sum' : 1 }
                }
            },
            { '$sort' : {'count' : -1 }},
            { '$limit' : 1 }
        ]))

        print(query)

    def task6b(self):
        #b) Is this also the year with most recorded hours?
        print("")

    def task7(self):
        # Find the total distance (in km) walked in 2008, by user with id=112.
        print("")

    def task8(self):
        # Find the top 20 users who have gained the most altitude meters.
            # Output should be a field with (id, total meters gained per user).
            # Remember that some altitude-values are invalid
            # Tip: ∑(tp n.altitude − tp .altitude), tp .altitude p .altitude n−1 n > t n−1
        print("")

    def task9(self):
        # Find all users who have invalid activities, and the number of invalid activities per user
            # An invalid activity is defined as an activity with consecutive trackpoints
            # where the timestamps deviate with at least 5 minutes.
        print("")

    def task10(self):
        # Find the users who have tracked an activity in the Forbidden City of Beijing.
            # In this question you can consider the Forbidden City to have coordinates
            # that correspond to: lat 39.916, lon 116.397.
        print("")

    def task11(self):
        #Find all users who have registered transportation_mode and their most used transportation_mode.
            # The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
            # Some users may have the same number of activities tagged with e.g. walk and car. 
            # In this case it is up to you to decide which transportation mode to include in your answer (choose one).
            # Do not count the rows where the mode is null.
        print("")



def main():
    try:
        program = Part2()
        print("Part 2: Queries \n")

        print("\nQuery 1:\n")
        program.task1()

        print("\nQuery 2:\n")
        program.task2()

        print("\nQuery 3:\n")
        program.task3()

        print("\nQuery 4:\n")
        program.task4()

        print("\nQuery 5:\n")
        program.task5()

        print("\nQuery 6a:\n")
        program.task6a()

        #print("\nQuery 6b:\n")
        #program.task6b()

        # print("\nQuery 7:\n")
        # program.task7()

        # print("\nQuery 8:\n")
        # program.task8()

        # print("\nQuery 9:\n")
        # program.task9()

        # print("\nQuery 10:\n")
        # program.task10()
        
        # print("\nQuery 11:\n")
        # program.task11()





    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()