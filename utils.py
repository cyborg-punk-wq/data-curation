import requests

def fetch_ml_pipeline(req_data,pipeline_url,timeout = 200):
    if pipeline_url == None:
        req_data['status'] = "failure"
        return req_data
    try:
        headers = {"API_KEY": "@hZ^Z~-PP<h8K}Kc+_V354^t7G6x)+<9*LTtBy7&h&wJ5WMG"}
        print("Requesting ML Pipeline",pipeline_url)
        r = requests.post(pipeline_url, json=req_data, headers= headers, timeout= timeout)
        # print(r.text)
        product_output = r.json()
    except Exception as e:
        print("ERROR : ",e)
        req_data['status'] = "failure"
        return req_data

    return product_output

def store_and_category_wise_filter(response,count):
    category_wise_response = {}
    for i in response:
        cate_ = cate_ = i["category_name"]
        if cate_ not in category_wise_response:
            category_wise_response[cate_] = []
        category_wise_response[cate_].append(i)

    filtered_sessions = []
    for i in category_wise_response:
        filtered_sessions.extend(store_wise_filter(category_wise_response[i],count[i][1]))

    return filtered_sessions

def store_wise_filter(response,count):
    
    unique_store_ids = {}
    indices = []
    num_images = 0
    total_imgs = 0

    for ind,session in enumerate(response):
        store_id = session['store_id']
        if store_id not in unique_store_ids.keys():
        # if len(unique_store_ids)
            unique_store_ids[store_id] = [[session["num_images"],ind]]
            total_imgs += session["num_images"]
            # num_images+=session["num_images"]
        else:
            unique_store_ids[store_id].append([session["num_images"],ind])
            total_imgs += session["num_images"]

    if total_imgs<=count:
        return response
    iter_ = 0

    while num_images<count:

        for store_id in unique_store_ids.keys():
            try:
                det_ = unique_store_ids[store_id][iter_]
                num_images+=det_[0]
                indices.append(det_[1])
                if num_images>count:
                    break
            except:
                pass
        iter_+=1

    return_response = [response[i] for i in indices]


    return return_response

def change_str2bool(str_):
    if str_.lower() == "true":
        return True
    elif str_.lower() == "false":
        return False
    else:
        return str_

def filter_sessions(sess,noi):
    return sess
    # pass

# def get_dataset_name(id, curation_token):
#     next_page = "https://curation.infilect.com/api/v1/datasets/"
#     response = []
#     name = ""
#     token_ = "Token {}".format(curation_token)
#     headers = {'Authorization': token_}
#     while next_page:
#         r = requests.get(next_page, headers=headers)
#         r = r.json()
#         # print(response)
#         response.extend(r["results"])
#         next_page = r["next"]
    
#     for r in response:
#         if id == r["id"]:
#             name = r["title"]
#             return name
    
#     return name

# def upload_softtags(req_data, dataset_id, version_name, softtags = ["category"],curation_token):

#     data = {}
#     data["session"] = req_data
#     data["softtags"] = softtags 
#     data["dataset"] = get_dataset_name(dataset_id)
#     data["version"] = version_name

#     base_url = "https://curation.infilect.com/api/v1/session/upload/"

#     # base_url = "https://curationdev.infilect.com/api/v1/session/upload/"
#     token_ = "Token {}".format(curation_token)
#     headers = {'Authorization': token_}

#     r = requests.post(base_url, json=data, headers=headers)
#     print(r.text)

# def get_infiviz_sessions(next_url, infiviz_token, timeout= 2000):

#     sessions = []

#     while next_url:
#         token_ = "Token {}".format(infiviz_token)
#         headers = {'Authorization': token_}

#         r = requests.post(next_url, headers=headers)
#         # r = requests.get(next_url, timeout= timeout)
#         # print(r)
#         response = r.json()
#         # print(response)
#         sessions.extend(response["results"])
#         next_url = response["next"]
#         # print(next_url)
#     return sessions

# def form_infiviz_url():
    





