import requests
from utils import change_str2bool
import pandas as pd

dummy_dataset_creation_json={
        "dataset": {
            "title": "New dataset 10",
            "curation_type": "bounding-box"
        },
        "version": [
            {"title": "Version 1"}
        ],
        "detection": {
            "category": False,
            "brand": False,
            "brand_form": False,
            "variant": False,
            "sku": False
        },
        "classification": {
            "category": False,
            "brand": False,
            "brand_form": False,
            "variant": False,
            "sku": False
        },
        "text": False
    }

dummy_dataset_updation_json={
        "dataset": {
            "id": -1,
            "title": "temp",
            "curation_type": "bounding-box"
        },
        "version": [
            {"title": "Version 1"}
        ],
        "detection": {
            "category": False,
            "brand": False,
            "brand_form": False,
            "variant": False,
            "sku": False
        },
        "classification": {
            "category": False,
            "brand": False,
            "brand_form": False,
            "variant": False,
            "sku": False
        },
        "text": False
    }

class Curation:
    def __init__(self):
        pass
    
    def add_variables(self, dataset_id, version_name, curation_token, softtags):
        self.dataset_id = int(dataset_id)
        self.version_name = version_name
        self.curation_token = curation_token
        self.softtags = softtags
        self.dataset_name = self.get_dataset_name()

    def get_dataset_name(self):
        next_page = "https://curation.infilect.com/api/v1/datasets/"
        response = []
        name = ""
        token_ = "Token {}".format(self.curation_token)
        headers = {'Authorization': token_}
        while next_page:
            try:
                r = requests.get(next_page, headers=headers)
                r = r.json()
                # print(r)
                response.extend(r["results"])
                next_page = r["next"]
            except Exception as e:
                print(e,next_page)
                break
        # print(type(self.dataset_id
        for r in response:
            # print("IDS: ",r["id"],type(r["id"]))
            if self.dataset_id == r["id"]:
                name = r["title"]
                return name
        
        return name


    
    def upload2curation(self, req_data):
        data = {}
        data["session"] = req_data
        data["softtags"] = self.softtags 
        data["dataset"] = self.dataset_name
        data["version"] = self.version_name

        base_url = "https://curation.infilect.com/api/v1/session/upload/"

        # base_url = "https://curationdev.infilect.com/api/v1/session/upload/"
        token_ = "Token {}".format(self.curation_token)
        headers = {'Authorization': token_}

        r = requests.post(base_url, json=data, headers=headers)
        return r.json()
        # print(r.text)


    def create_dataset(self, params):
        dataset_creation_json = dummy_dataset_creation_json.copy()


        dataset_creation_json["dataset"]["title"] = params["dataset_title"]
        dataset_creation_json["dataset"]["curation_type"] = params["curation_type"]
        dataset_creation_json["version"][0]["title"] = params["dataset_version"]
        dataset_creation_json["detection"]["category"] = change_str2bool(params["category"])
        dataset_creation_json["detection"]["brand"] = change_str2bool(params["brand"])
        dataset_creation_json["detection"]["brand_form"] = change_str2bool(params["brandform"])
        dataset_creation_json["detection"]["variant"] = change_str2bool(params["variant"])
        dataset_creation_json["detection"]["sku"] = change_str2bool(params["sku"])
        dataset_creation_json["classification"]["category"] = change_str2bool(params["class_category"])
        dataset_creation_json["classification"]["brand"] = change_str2bool(params["class_brand"])
        dataset_creation_json["classification"]["brand_form"] = change_str2bool(params["class_brandform"])
        dataset_creation_json["classification"]["variant"] = change_str2bool(params["class_variant"])
        dataset_creation_json["classification"]["sku"] = change_str2bool(params["class_sku"])
        dataset_creation_json["text"] = bool(params["is_text"])

        print(dataset_creation_json)

        base_url = "https://curation.infilect.com/api/v1/dataset/"

        curation_token = params["curation_token"]
        token_ = "Token {}".format(curation_token)
        headers = {'Authorization': token_}

        r = requests.post(base_url, json=[dataset_creation_json], headers=headers)
        return r.json()

    def update_dataset(self,params):

        dataset_creation_json = dummy_dataset_updation_json.copy()
        self.dataset_id = int(params["dataset_id"])
        self.curation_token = params["curation_token"]

        dataset_creation_json["dataset"]["id"] = int(params["dataset_id"])
        dataset_creation_json["dataset"]["title"] = self.get_dataset_name()
        dataset_creation_json["dataset"]["curation_type"] = params["curation_type"]
        dataset_creation_json["version"][0]["title"] = params["dataset_version"]
        dataset_creation_json["detection"]["category"] = change_str2bool(params["category"])
        dataset_creation_json["detection"]["brand"] = change_str2bool(params["brand"])
        dataset_creation_json["detection"]["brand_form"] = change_str2bool(params["brandform"])
        dataset_creation_json["detection"]["variant"] = change_str2bool(params["variant"])
        dataset_creation_json["detection"]["sku"] = change_str2bool(params["sku"])
        dataset_creation_json["classification"]["category"] = change_str2bool(params["class_category"])
        dataset_creation_json["classification"]["brand"] = change_str2bool(params["class_brand"])
        dataset_creation_json["classification"]["brand_form"] = change_str2bool(params["class_brandform"])
        dataset_creation_json["classification"]["variant"] = change_str2bool(params["class_variant"])
        dataset_creation_json["classification"]["sku"] = change_str2bool(params["class_sku"])
        dataset_creation_json["text"] = bool(params["is_text"])

        base_url = "https://curation.infilect.com/api/v1/dataset/"
        token_ = "Token {}".format(self.curation_token)
        headers = {'Authorization': token_}
        print(dataset_creation_json)

        r = requests.put(base_url, json=[dataset_creation_json], headers=headers)
        # print(r.json())
        return r.json()

    def add_labels(self,schema_path,params):


        xl = pd.ExcelFile(schema_path,engine='openpyxl')
        sheet_names = xl.sheet_names
        print(sheet_names)
        df = xl.parse(params["schema_sheet_names"])
        df.dropna(axis=0,how='all',inplace=True)
        df.dropna(axis=1,how='all',inplace=True)

        level = params["curation_level"]


        label_json = {"id": int(params["dataset_id"]), "category": [], "brand": [], "brandform": [], "variant": [], "sku": [], "text": []}

        ii = params["internal_columns"]
        mm = params["mapping_columns"]
        ind = 0
        for i,m in zip(df[ii],df[mm]):
            if ind ==490:
                base_url = "https://curation.infilect.com/api/v1/dataset/label/"

                token_ = "Token {}".format(params["curation_token"])
                headers = {'Authorization': token_}

                print(f"Adding labels to Dataset: {label_json['id']}, for {level} level...")
                r = requests.post(base_url, json=label_json, headers=headers)
                label_json[level] = []
                ind=0


            label_json[level].append({"title": m, "name": i})
            ind+=1

        print(f"Adding labels to Dataset: {label_json['id']}, for {level} level...")

        base_url = "https://curation.infilect.com/api/v1/dataset/label/"

        token_ = "Token {}".format(params["curation_token"])
        headers = {'Authorization': token_}

        r = requests.post(base_url, json=label_json, headers=headers)

        # print(r.json())

        return r.json()
        # print(r.text)





        # return dataset_creation_json
        
