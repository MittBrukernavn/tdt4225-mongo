from DbConnector import DbConnector
from os import walk, getcwd

class Part1:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection:', collection)
    
    def insert_documents(self, collection_name, docs):
        self.db[collection_name].insert_many(docs)
    
    def get_users(self):
        working_directory = getcwd()
        _, user_ids, _ = next(walk(f'{working_directory}/dataset/Data'))

        user_ids.sort()
    
        users = [{ 'id': user_id, 'has_labels': False } for user_id in user_ids]

        with open(f'{working_directory}/dataset/labeled_ids.txt') as f:
            for line in f:
                # make sure our logic works (would fail e.g. if some person IDs are missing)
                assert line.strip() == users[int(line)]['id']
                users[int(line)]['has_labels'] = True
        
        for user in users:
            user_id = user['id']
            has_labels = user['has_labels']
            user['activities'] = self.get_activities(user_id, has_labels)
        
        return users
    
    def get_activities(self, user_id, has_labels):
        labels = {}
        if has_labels:
            with open(f'{getcwd()}/dataset/Data/{user_id}/labels.txt', 'r') as f:
                f.readline() # skip header
                for line in f:
                    start_date, start_time, end_date, end_time, transport_mode = line.strip().split()
                    # dates are slightly differently formatted in the labels-files than the .plt-files:
                    start_date = start_date.replace('/', '-')
                    end_date = end_date.replace('/', '-')
                    labels[f'{start_date} {start_time}'] = (f'{end_date} {end_time}', transport_mode)
        _, _, activity_filenames = next(walk(f'{getcwd()}/dataset/Data/{user_id}/Trajectory'))
        activities = []
        for filename in activity_filenames:
            activity = {}
            trackpoints = None
            try:
                trackpoints = self.get_trackpoints(user_id, filename)
            except:
                continue
            activity['trackpoints'] = trackpoints
            transport_mode = None
            start_date, start_time = trackpoints[0]['date'], trackpoints[0]['time']
            end_date, end_time = trackpoints[-1]['date'], trackpoints[-1]['time']
            if f'{start_date} {start_time}' in labels:
                end_date_and_time, mode = labels[f'{start_date} {start_time}']
                if end_date_and_time == f'{end_date} {end_time}':
                    transport_mode = mode
            activity['transportation_mode'] = transport_mode
            activities.append(activity)
        return activities
    
    def get_trackpoints(self, user_id, activity_filename):
        with open(f'{getcwd()}/dataset/Data/{user_id}/Trajectory/{activity_filename}') as activity_file:
            # skip 6 first lines
            for _ in range(6):
                activity_file.readline()
            lines = activity_file.readlines()
            if len(lines) > 2500:
                raise Exception('File too long')
            points = []
            for line in lines:
                lat, lon, _, alt, date_days, date, time = line.strip().split(',')
                points.append({
                    'lat': lat,
                    'lon': lon,
                    'alt': alt,
                    'date_days': date_days,
                    'datetime': f'{date} {time}'
                })
            return points




def main():
    program = None
    try:
        program = Part1()
    except Exception as e:
        print("ERROR:", e)
    finally:
        if program:
            program.connection.close_connection()