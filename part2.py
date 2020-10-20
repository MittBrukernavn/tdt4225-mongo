from DbConnector import DbConnector

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
        print("User count: ", user_count)

def main():
    # program = None
    try:
        program = Part2()
        program.task1()
    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()