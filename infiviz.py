import requests
import itertools
import json

base_infiviz_url = "http://public.infiviz.ai/api/v1"
infiviz_token = "4a241bf6ce0948b7bcfb46ba16a25c67"


class Infiviz:
    def __init__(self):
        pass

    def add_variables(self, client_id, category, channel, photo_type, end_date, start_date):
        
        self.client_id = client_id
        self.categories = category.split(",")
        self.channels = channel.split(",")
        self.photo_types = photo_type.split(",")
        self.end_date = end_date
        self.start_date = start_date

    def get_stats(self,response):
        res = {}

        res["num_sessions"] = len(response)
        res["num_images"] = sum([i["num_images"] for i in response])
        res["num_unique_stores"] = len(set([i["store_id"] for i in response]))
        res["average_imgs_per_sessions"] = res["num_images"]/res["num_sessions"]
        # res["display_count"] = 

        # res[]
        return res
    
    def get_combinations(self, processed = False):
        combinations = list(itertools.product(*[self.categories,self.photo_types, self.channels]))

        final_stats = []
        total_imgs = 0
        total_unique_store = 0
        total_sessions = 0
        total_num_unique_images = 0
        unique_store = []
        self.all_sessions = []

        for category,photo_type,channel in combinations:
            if processed:
                url = "{}/generic-output/?client={}&category={}&channel={}&photo_type={}&from_date={}&to_date={}".format(base_infiviz_url,
                        self.client_id, category, channel, photo_type,self.start_date, self.end_date)
                # print(url)
                tmp_session = self.get_processed_session(url)
            else:
                url = "{}/session_info/?category_name={}&channel_name={}&client_id={}&end_date={}&page={}&photo_type_name={}&start_date={}".format(base_infiviz_url,
                        category, channel, self.client_id, self.end_date, 1, photo_type, self.start_date)
                tmp_session = self.get_infiviz_sessions(url)
            if len(tmp_session)>0:
                stats = self.get_stats(tmp_session)
                # stats['channel'] = channel
                stats['category'] = category
                # stats['client_id'] = client_id
                stats['photo_type'] = photo_type
                # stats['path'] = path
                # stats['to_include'] = True
                # stats['get_all_store'] = True
                # stats['get_num_of_unique_stores'] = 2
                # stats['get_number_of_sessions'] = 0
                for i in tmp_session:
                    if i["store_id"] not in unique_store:
                        total_num_unique_images += i["num_images"]
                        unique_store.append(i["store_id"])

                total_imgs += stats["num_images"]
                total_sessions += stats['num_sessions']
                total_unique_store += stats["num_unique_stores"]
                final_stats.append(stats)
                self.all_sessions.extend(tmp_session)
        fin_ = {"client_id":self.client_id,"total_num_images":total_imgs,"total_num_sessions":total_sessions,"total_num_unique_stores":total_unique_store,"total_num_unique_images":total_num_unique_images,"stats":final_stats}
        return fin_

    def get_infiviz_sessions(self,next_url, timeout= 3000):

        sessions = []
        ind = 0

        while next_url:
            token_ = "Token {}".format(infiviz_token)
            # print("Next URL:",next_url)
            headers = {'Authorization': token_}
            try:

                r = requests.post(next_url, headers=headers, timeout= timeout)
                # r = requests.get(next_url, timeout= timeout)
                # print(r)
                response = r.json()
                ind+=1
                print("GOT Using Sessions Info API",ind)
            # print("Response:",response)
            # with open("tmp_"+str(ind)+".json","w") as fp:
            #     json.dump(response["results"]["sessions"], fp ,indent = 2)
            
                sessions.extend(response["results"]["sessions"])
                next_url = response["next"]
            except:
                return sessions

            # print(next_url)
        # print("Sessions: ",sessions)
        return sessions
    
    def modify_category_label(self,sessions):
        
        for response in sessions:
            for img_idx, image in enumerate(response["images"]):
                for ind,product in enumerate(image["products"]):
                    # print(product["item"]["label"])
                    if len(product["item"]["label"]) == 0:
                        product["category"]["label"] = "product-in-shelf"
                        product["category"]["score"] = 0.93
                    else:
                        product["category"]["label"] = product["item"]["label"]
                        product["category"]["score"] = product["item"]["score"]
        return sessions
            
        
        # pass
        

    def get_processed_session(self, next_url, timeout = 30000):

        sessions = []
        ind = 0

        while next_url:
            token_ = "Token {}".format(infiviz_token)
            headers = {'Authorization': token_,"CLIENTID":self.client_id}
            ind+=1

            r = requests.post(next_url, headers=headers, timeout=timeout)

            print("GOT Using Generic Output API",ind)
            # r = requests.get(next_url, timeout= timeout)
            # print(r)
            try:
                response = r.json()
            
            # print(response)
                sessions.extend(response["results"])
                next_url = response["next"]
            except Exception as e:
                print(e)
                return self.modify_category_label(sessions)

            # print(next_url)
            
        sessions = self.modify_category_label(sessions)
        return sessions
